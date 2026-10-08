import base64
import datetime
import io
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient

from backend import as2, main, protocols


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main, 'DB', tmp_path / 'test.db')
    # Protocol/flow tests provision fake partners; the save gate has its own tests.
    monkeypatch.setattr(main,'check_connection',lambda connection:{'success':True,'tested_at':'2026-10-07T00:00:00+00:00','detail':{}})
    return TestClient(main.app)


def profile(client, protocol='SFTP', endpoint='sftp://example.test/inbox', config=None):
    response = client.post('/api/connections', json={'name': 'Partner', 'protocol': protocol, 'endpoint': endpoint, 'config': config or {}})
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.parametrize('path', ['../secret', '/etc/passwd', 'a/../../secret', 'a\\secret', 'C:secret', 'a//b'])
def test_traversal_rejected(path):
    with pytest.raises(ValueError):
        protocols.relative_path(path)


def test_config_migration_and_validation(client):
    connection = profile(client)
    assert client.get('/api/connections').json()[0]['config']['timeout'] == 30
    assert client.put('/api/connections/'+connection['id'], json={'name':'Changed','protocol':'GCS','endpoint':'gs://bucket/prefix','config':{'project_id':'test'}}).status_code == 200
    assert client.post('/api/connections', json={'name':'Bad','protocol':'SFTP','endpoint':'sftp://user:password@example.test'}).status_code == 422
    assert client.post('/api/connections', json={'name':'Bad','protocol':'SMB','endpoint':'\\\\host\\share','config':{'password':'secret'}}).status_code == 422


def test_sftp_host_key_pin():
    key = protocols.paramiko.RSAKey.generate(2048)
    fingerprint = 'SHA256:'+base64.b64encode(protocols.hashlib.sha256(key.asbytes()).digest()).decode().rstrip('=')
    protocols.PinnedHostKey(fingerprint).missing_host_key(None, 'host', key)
    with pytest.raises(ValueError):
        protocols.PinnedHostKey('SHA256:wrong').missing_host_key(None, 'host', key)


def test_sftp_io_and_history(client, monkeypatch):
    channel = MagicMock()
    channel.normalize.side_effect = lambda path: path
    channel.open.return_value.__enter__.return_value = io.BytesIO(b'purchase order')
    channel.listdir_iter.return_value = iter([SimpleNamespace(filename='order.txt',st_size=14,st_mode=0o100644)])
    @contextmanager
    def session(*args):
        yield channel, '/inbox'
    monkeypatch.setattr(protocols, 'sftp_connection', session)
    connection = profile(client)
    url = f"/api/connections/{connection['id']}/operate"
    assert client.post(url,json={'operation':'list'}).json()['entries'][0]['name']=='order.txt'
    received = client.post(url,json={'operation':'receive','path':'order.txt'}).json()
    assert client.get(received['download_url']).content == b'purchase order'
    channel.open.return_value.__enter__.return_value = MagicMock()
    assert client.post(url,json={'operation':'send','path':'new.txt','content_base64':base64.b64encode(b'new').decode()}).status_code == 200
    assert channel.open.call_args.args == ('/inbox/new.txt','wx')
    assert client.post(url,json={'operation':'receive','path':'../escape'}).status_code == 400
    history=client.get('/api/transfers').json()
    assert len(history)==4 and history[0]['status']=='failed'


def test_gcs_no_overwrite_and_download(monkeypatch):
    blob = MagicMock(size=3,generation=7)
    blob.download_as_bytes.return_value=b'abc'
    bucket=MagicMock();bucket.blob.return_value=blob
    @contextmanager
    def session(*args):
        yield MagicMock(),bucket,'orders'
    monkeypatch.setattr(protocols,'gcs_connection',session)
    connection={'protocol':'GCS','endpoint':'gs://bucket/orders','config':{}}
    protocols.operate(connection,'send','new.txt',b'abc')
    assert bucket.blob.call_args.args==('orders/new.txt',)
    assert blob.upload_from_string.call_args.kwargs['if_generation_match']==0
    assert protocols.operate(connection,'receive','new.txt')==b'abc'
    assert blob.download_as_bytes.call_args.kwargs['if_generation_match']==7


def test_smb_exclusive_create(monkeypatch):
    @contextmanager
    def session(*args):
        yield '\\\\host\\share',{}
    monkeypatch.setattr(protocols,'smb_connection',session)
    monkeypatch.setattr(protocols.smbclient,'lstat',lambda *a,**k: SimpleNamespace(st_file_attributes=0))
    file=MagicMock()
    monkeypatch.setattr(protocols.smbclient,'open_file',file)
    protocols.operate({'protocol':'SMB','endpoint':'\\\\host\\share','config':{}},'send','order.txt',b'abc')
    assert file.call_args.kwargs['mode']=='xb'


def key_pair(name):
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])
    now=datetime.datetime.now(datetime.timezone.utc)
    cert=x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=1)).not_valid_after(now+datetime.timedelta(days=30)).add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True).sign(key,hashes.SHA256())
    public=cert.public_bytes(serialization.Encoding.PEM)
    private=key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
    return (private+public).decode(),public.decode()


@pytest.mark.parametrize('compress', [False, True])
def test_signed_encrypted_as2_roundtrip(client,monkeypatch,compress):
    alice_key,alice_cert=key_pair('Alice');bob_key,bob_cert=key_pair('Bob')
    for name,value in {'ALICE_KEY':alice_key,'ALICE_CERT':alice_cert,'BOB_KEY':bob_key,'BOB_CERT':bob_cert}.items():
        monkeypatch.setenv(name,value)
    alice=protocols.AS2Options(local_as2_id='ALICE',partner_as2_id='BOB',signing_key_env='ALICE_KEY',decryption_key_env='ALICE_KEY',partner_certificate_env='BOB_CERT',compress=compress)
    bob=protocols.AS2Options(local_as2_id='BOB',partner_as2_id='ALICE',signing_key_env='BOB_KEY',decryption_key_env='BOB_KEY',partner_certificate_env='ALICE_CERT')
    connection=profile(client,'AS2','https://partner.test/as2',bob.model_dump())
    inbound_url=f"/as2/{connection['id']}"
    delivered=[]
    def transport(request):
        response=client.post(inbound_url,headers=dict(request.headers),content=request.content)
        assert response.status_code==200,response.text
        delivered.append((dict(request.headers),request.content))
        return httpx.Response(response.status_code,headers=response.headers,content=response.content)
    monkeypatch.setattr(as2,'http_client',lambda options:httpx.Client(transport=httpx.MockTransport(transport)))
    result=as2.send('https://partner.test/as2',alice,'order.txt',b'confidential purchase order','text/plain')
    assert result['mdn_status']=='processed'
    transfer=client.get('/api/transfers').json()[0]
    assert client.get(f"/api/transfers/{transfer['id']}/download").content==b'confidential purchase order'
    headers,body=delivered[0]
    assert client.post(inbound_url,headers=headers,content=body).status_code==200
    assert b'confidential purchase order' not in body
    tampered = bytearray(body)
    tampered[len(tampered)//2] ^= 0x01
    tamper_headers={**headers,'message-id':'<new-tampered-message@example.test>'}
    assert client.post(inbound_url,headers=tamper_headers,content=bytes(tampered)).status_code==400


def test_compression_limit(monkeypatch):
    from email.message import Message
    from pyas2lib.cms import compress_message
    payload=Message()
    payload.set_type('application/pkcs7-mime')
    payload.set_param('smime-type','compressed-data')
    payload.set_payload(compress_message(b'x'*1000))
    monkeypatch.setattr(as2,'MAX_BYTES',100)
    with pytest.raises(ValueError,match='oversized'):
        as2.BoundedMessage()._decompress_data(payload)


def test_as2_invalid_mdn_rejected(client,monkeypatch):
    options=protocols.AS2Options(local_as2_id='A',partner_as2_id='B',sign=False,encrypt=False)
    monkeypatch.setattr(as2,'http_client',lambda options:httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'OK'))))
    with pytest.raises(ValueError,match='MDN'):
        as2.send('https://partner.test',options,'order.txt',b'abc','text/plain')

def test_async_signed_receipt_and_replay(client, monkeypatch):
    alice_key,alice_cert=key_pair('Alice');bob_key,bob_cert=key_pair('Bob')
    for name,value in {'ALICE_KEY':alice_key,'ALICE_CERT':alice_cert,'BOB_KEY':bob_key,'BOB_CERT':bob_cert}.items():
        monkeypatch.setenv(name,value)
    sender=profile(client,'AS2','https://bob.test/as2',dict(local_as2_id='ALICE',partner_as2_id='BOB',signing_key_env='ALICE_KEY',decryption_key_env='ALICE_KEY',partner_certificate_env='BOB_CERT',mdn_mode='ASYNC',mdn_url='https://alice.test/callback'))
    receiver=profile(client,'AS2','https://alice.test/as2',dict(local_as2_id='BOB',partner_as2_id='ALICE',signing_key_env='BOB_KEY',decryption_key_env='BOB_KEY',partner_certificate_env='ALICE_CERT',partner_mdn_url='https://alice.test/callback'))
    delivered=[]
    def transport(request):
        if str(request.url)=='https://alice.test/callback':
            response=client.post('/as2/'+sender['id']+'/mdn',headers=dict(request.headers),content=request.content)
            assert response.status_code==200,response.text
            return httpx.Response(200)
        delivered.append((dict(request.headers),request.content))
        response=client.post('/as2/'+receiver['id'],headers=dict(request.headers),content=request.content)
        assert response.status_code==202,response.text
        return httpx.Response(202)
    monkeypatch.setattr(as2,'http_client',lambda options:httpx.Client(transport=httpx.MockTransport(transport)))
    result=as2.send(sender['endpoint'],protocols.AS2Options.model_validate(sender['config']),'order.xml',b'<order/>','application/xml',sender['id'])
    assert result['delivery_status']=='awaiting_mdn'
    headers,body=delivered[0]
    assert client.post('/as2/'+receiver['id'],headers=headers,content=body).status_code==202
    assert len(client.get('/api/transfers').json())==1
    assert client.post('/as2/'+receiver['id'],headers=headers,content=body+b'changed').status_code==409
    with main.database() as db:
        receipt=db.execute('SELECT headers,body FROM as2_receipts').fetchone()
    import json
    receipt_headers=json.loads(receipt[0])
    assert client.post('/as2/'+receiver['id']+'/mdn',headers=receipt_headers,content=receipt[1]).status_code==400
    assert client.post('/as2/'+sender['id']+'/mdn',headers=receipt_headers,content=b'invalid').status_code==400
    assert as2.process_next_receipt()
    assert as2.outbound_status(result['message_id'])=='confirmed'
    assert client.post('/as2/'+sender['id']+'/mdn',headers=receipt_headers,content=receipt[1]).status_code==200
    evidence=client.get('/api/as2/messages/'+result['message_id']+'/receipt')
    assert evidence.status_code==200 and b'PRIVATE KEY' not in evidence.content
    info=client.get('/api/connections/'+sender['id']+'/as2-info').json()
    assert 'BEGIN CERTIFICATE' in info['certificate_pem']
    assert 'PRIVATE KEY' not in str(info)


def test_inbound_transport_guards(client,monkeypatch):
    monkeypatch.setenv('INBOUND_PASSWORD','password')
    connection=profile(client,'AS2','https://partner.test/as2',dict(local_as2_id='A',partner_as2_id='B',sign=False,encrypt=False,inbound_username='partner',inbound_password_env='INBOUND_PASSWORD'))
    assert client.head('/as2/'+connection['id']).status_code==401
    assert client.head('/as2/'+connection['id'],auth=('partner','password')).status_code==200
    secure=profile(client,'AS2','https://partner.test/as2',dict(local_as2_id='A',partner_as2_id='B',sign=False,encrypt=False,require_https=True))
    assert client.head('/as2/'+secure['id']).status_code==403


def test_async_callback_retry_limit(client,monkeypatch):
    connection=profile(client,'AS2','https://partner.test/as2',dict(local_as2_id='A',partner_as2_id='B',sign=False,encrypt=False))
    with main.database() as db:
        db.execute("INSERT INTO as2_receipts(id,connection_id,message_id,request_hash,headers,body,callback_url,status) VALUES (?,?,?,?,?,?,?,?)",('receipt',connection['id'],'message','hash','{}',b'mdn','https://partner.test/mdn','queued'))
    monkeypatch.setattr(as2,'http_client',lambda options:httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(503))))
    for _ in range(3):
        with main.database() as db:
            db.execute('UPDATE as2_receipts SET next_attempt=0')
        assert as2.process_next_receipt()
    assert not as2.process_next_receipt()
    with main.database() as db:
        assert db.execute('SELECT status,attempts FROM as2_receipts').fetchone()==('failed',3)
    assert client.post('/api/as2/receipts/receipt/retry').status_code==200
