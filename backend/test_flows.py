import base64
import json
import subprocess
from types import SimpleNamespace

import httpx
import pytest

from backend import as2, ems, flows, main, protocols
from backend.test_protocols import client, key_pair, profile


def make_flow(client, source, destination, **options):
    response=client.post('/api/flows',json={'name':'File route','source_id':source['id'],'destination_id':destination['id'],'min_age_seconds':0,**options})
    assert response.status_code==201,response.text
    return response.json()


def test_nas_as2_nas_real_file_roundtrip(client,tmp_path,monkeypatch):
    outbound=tmp_path/'outbound';outbound.mkdir()
    inbound=tmp_path/'inbound';inbound.mkdir()
    document=b'ISA|confidential purchase order|END'
    (outbound/'order.edi').write_bytes(document)
    alice_key,alice_cert=key_pair('Alice');bob_key,bob_cert=key_pair('Bob')
    for key,value in {'ALICE_KEY':alice_key,'ALICE_CERT':alice_cert,'BOB_KEY':bob_key,'BOB_CERT':bob_cert}.items():
        monkeypatch.setenv(key,value)
    nas_out=profile(client,'FILE',str(outbound))
    nas_in=profile(client,'FILE',str(inbound))
    alice=profile(client,'AS2','https://partner.test/as2',{'local_as2_id':'ALICE','partner_as2_id':'BOB','signing_key_env':'ALICE_KEY','decryption_key_env':'ALICE_KEY','partner_certificate_env':'BOB_CERT'})
    bob=profile(client,'AS2','https://relay.test/as2',{'local_as2_id':'BOB','partner_as2_id':'ALICE','signing_key_env':'BOB_KEY','decryption_key_env':'BOB_KEY','partner_certificate_env':'ALICE_CERT'})
    outbound_flow=make_flow(client,nas_out,alice,file_pattern='*.edi')
    inbound_flow=make_flow(client,bob,nas_in,destination_pattern='{job_id}-{filename}')
    def transport(request):
        response=client.post('/as2/'+bob['id'],headers=dict(request.headers),content=request.content)
        assert response.status_code==200,response.text
        # The MDN is returned only after the inbound routing job is durable.
        assert any(j['flow_id']==inbound_flow['id'] and j['status']=='queued' for j in flows.list_jobs())
        return httpx.Response(response.status_code,headers=response.headers,content=response.content)
    monkeypatch.setattr(as2,'http_client',lambda options:httpx.Client(transport=httpx.MockTransport(transport)))
    assert flows.poll_flow(outbound_flow['id'],force=True)['queued']==1
    assert flows.process_next_job()
    assert flows.process_next_job()
    received=list(inbound.iterdir())
    assert len(received)==1 and received[0].name.endswith('-order.edi')
    assert received[0].read_bytes()==document
    assert (outbound/'order.edi').read_bytes()==document
    assert all(j['status']=='success' for j in flows.list_jobs())
    assert flows.poll_flow(outbound_flow['id'],force=True)['queued']==0


def test_mounted_nas_boundary_and_no_overwrite(tmp_path):
    root=tmp_path/'nas';root.mkdir()
    connection={'protocol':'FILE','endpoint':str(root),'config':{}}
    protocols.operate(connection,'send','order.bin',b'abc')
    assert protocols.operate(connection,'receive','order.bin')==b'abc'
    with pytest.raises(FileExistsError):
        protocols.operate(connection,'send','order.bin',b'changed')
    with pytest.raises(protocols.ConnectorError):
        protocols.operate(connection,'receive','../outside')


def test_poll_filter_age_pause_and_destination_snapshot(client,tmp_path):
    source_root=tmp_path/'source';source_root.mkdir()
    destination_root=tmp_path/'destination';destination_root.mkdir()
    alternate_root=tmp_path/'alternate';alternate_root.mkdir()
    (source_root/'order.xml').write_bytes(b'<order/>')
    (source_root/'order.tmp').write_bytes(b'partial')
    source=profile(client,'FILE',str(source_root));destination=profile(client,'FILE',str(destination_root));alternate=profile(client,'FILE',str(alternate_root))
    flow=make_flow(client,source,destination,file_pattern='*.xml',min_age_seconds=100)
    assert flows.poll_flow(flow['id'],force=True)['queued']==0
    config={key:value for key,value in flow.items() if key!='id'}
    config['min_age_seconds']=0
    assert client.put('/api/flows/'+flow['id'],json=config).status_code==200
    assert flows.poll_flow(flow['id'],force=True)['queued']==1
    config['destination_id']=alternate['id'];config['enabled']=False
    assert client.put('/api/flows/'+flow['id'],json=config).status_code==200
    assert flows.process_next_job()
    job=flows.list_jobs()[0];assert job['status']=='held'
    config['enabled']=True
    client.put('/api/flows/'+flow['id'],json=config)
    assert client.post('/api/jobs/'+job['id']+'/retry').status_code==200
    assert flows.process_next_job()
    assert (destination_root/'order.xml').exists()
    assert not (alternate_root/'order.xml').exists()


def test_failure_preserves_spooled_payload(client,tmp_path,monkeypatch):
    source_root=tmp_path/'source';source_root.mkdir();(source_root/'order.txt').write_bytes(b'keep me')
    source=profile(client,'FILE',str(source_root));destination=profile(client,'SFTP','sftp://partner.test')
    flow=make_flow(client,source,destination)
    flows.poll_flow(flow['id'],force=True)
    def fail(*args,**kwargs):
        raise TimeoutError('network timeout')
    monkeypatch.setattr(flows,'operate',fail)
    assert flows.process_next_job()
    job=flows.list_jobs()[0];assert job['status']=='uncertain'
    with main.database() as db:
        assert db.execute('SELECT payload FROM jobs WHERE id=?',(job['id'],)).fetchone()[0]==b'keep me'
    assert not flows.process_next_job()  # no automatic resend of uncertain delivery


def test_nas_to_ems_bridge_contract(client,tmp_path,monkeypatch):
    source_root=tmp_path/'source';source_root.mkdir();(source_root/'invoice.pdf').write_bytes(b'%PDF-binary\x00')
    source=profile(client,'FILE',str(source_root))
    destination=profile(client,'EMS','tcp://ems.test:7222',{'destination':'B2B.INVOICES','username':'relay','password_env':'EMS_PASSWORD'})
    monkeypatch.setenv('EMS_PASSWORD','never-log-this')
    monkeypatch.setenv('RELAY_EMS_CLASSPATH','vendor/tibjms.jar;vendor/jms.jar')
    captured={}
    def bridge(command,**kwargs):
        values={key:base64.b64decode(value).decode() for key,value in (line.split('=',1) for line in kwargs['input'].splitlines())}
        captured.update(values)
        assert 'never-log-this' not in ' '.join(command)
        assert values['destination']=='B2B.INVOICES'
        assert base64.b64decode(values['data'])==b'%PDF-binary\x00'
        result=base64.b64encode(json.dumps({'committed':True,'message_id':'ID:1','delivery_mode':'persistent'}).encode()).decode()
        return SimpleNamespace(returncode=0,stdout='RELAY_RESULT '+result,stderr='')
    monkeypatch.setattr(ems.subprocess,'run',bridge)
    monkeypatch.setattr(ems.shutil,'which',lambda name:'java')
    flow=make_flow(client,source,destination)
    assert flows.poll_flow(flow['id'],force=True)['queued']==1
    assert flows.process_next_job()
    assert flows.list_jobs()[0]['detail']['committed'] is True
    assert captured['filename']=='invoice.pdf'


def test_ems_missing_jars_and_timeout(monkeypatch):
    monkeypatch.setattr(ems.shutil,'which',lambda name:'java')
    monkeypatch.delenv('RELAY_EMS_CLASSPATH',raising=False)
    options=protocols.EMSOptions(destination='ORDERS')
    with pytest.raises(protocols.ConnectorError,match='not configured'):
        ems.operate_ems('tcp://host:7222',options,'send','order',b'abc','text/plain')
    monkeypatch.setenv('RELAY_EMS_CLASSPATH','jars')
    def timeout(*args,**kwargs):
        raise subprocess.TimeoutExpired('java',30)
    monkeypatch.setattr(ems.subprocess,'run',timeout)
    with pytest.raises(protocols.ConnectorError,match='uncertain'):
        ems.operate_ems('tcp://host:7222',options,'send','order',b'abc','text/plain')
