"""Connector integration and failure-semantics checks."""
import base64
import datetime
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from backend import flows, inbox, main, protocols
from backend.test_protocols import client, profile
from backend.test_flows import make_flow


@pytest.mark.parametrize("protocol,endpoint",[
    ("FTP","ftp://localhost/%2e%2e/secret"),("HTTP","http://localhost/%2e%2e"),
    ("SHAREPOINT","https://tenant.sharepoint.com/sites/%2e%2e")])
def test_encoded_endpoint_traversal_rejected(protocol,endpoint):
    with pytest.raises(ValueError):
        protocols.validate_endpoint(protocol,endpoint)


def test_http_inbound_auth_storage_and_nas_routing(client,tmp_path,monkeypatch):
    monkeypatch.setenv("HTTP_INBOUND","server-secret")
    source=profile(client,"HTTP","http://partner.test/files",{
        "enable_inbound":True,"inbound_only":True,"inbound_token_env":"HTTP_INBOUND"})
    nas=tmp_path/"nas";nas.mkdir()
    destination=profile(client,"FILE",str(nas))
    make_flow(client,source,destination)
    url="/http/"+source["id"]+"/order.xml"
    assert client.put(url,content=b"private").status_code==401
    auth={"Authorization":"Bearer server-secret"}
    assert client.put(url,headers=auth,content=b"<order/>").status_code==201
    assert client.put(url,headers=auth,content=b"changed").status_code==412
    assert client.get(url,headers=auth).content==b"<order/>"
    listing=client.get("/http/"+source["id"],params={"list":"1"},headers=auth).json()
    assert listing["entries"][0]["name"]=="order.xml"
    assert flows.process_next_job()
    assert (nas/"order.xml").read_bytes()==b"<order/>"
    assert len(flows.list_jobs())==1
    assert "server-secret" not in json.dumps(client.get("/api/connections").json())
    assert client.get("/api/connectors/inbox").json()["documents"][0]["bytes"]==8


def test_http_client_roundtrip_and_no_overwrite(client,monkeypatch):
    from backend import extended
    monkeypatch.setenv("HTTP_TOKEN","token")
    peer=profile(client,"HTTP","http://partner.test",{"enable_inbound":True,"inbound_token_env":"HTTP_TOKEN"})
    transport=httpx.MockTransport(lambda request:httpx.Response(
        (response:=client.request(request.method,request.url.path,params=dict(request.url.params),headers=dict(request.headers),content=request.content)).status_code,
        headers=response.headers,content=response.content))
    monkeypatch.setattr(extended,"http_session",lambda options:httpx.Client(transport=transport,headers={"Authorization":"Bearer token"}))
    remote={"protocol":"HTTP","endpoint":"http://partner.test/http/"+peer["id"],"config":{}}
    result=protocols.operate(remote,"send","purchase.xml",b"exact-bytes","application/xml")
    assert result["http_status"]==201
    assert protocols.operate(remote,"receive","purchase.xml")==b"exact-bytes"
    assert protocols.operate(remote,"list")["entries"][0]["name"]=="purchase.xml"
    with pytest.raises(FileExistsError):
        protocols.operate(remote,"send","purchase.xml",b"other")


@pytest.fixture(params=["FTP","FTPS"])
def ftp_peer(request,tmp_path,monkeypatch):
    folder=tmp_path/"ftp";folder.mkdir()
    with socket.socket() as allocation:
        allocation.bind(("127.0.0.1",0));port=allocation.getsockname()[1]
    env={**os.environ,"FTP_USERNAME":"relay","FTP_PASSWORD":"sample-password"}
    command=[sys.executable,"-m","backend.ftp_listener","--root",str(folder),"--port",str(port)]
    config={"username":"relay","password_env":"FTP_PASSWORD"}
    if request.param=="FTPS":
        private=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,"localhost")])
        now=datetime.datetime.now(datetime.timezone.utc)
        cert=x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(private.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(days=1)).not_valid_after(now+datetime.timedelta(days=1)).add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]),False).sign(private,hashes.SHA256())
        public=cert.public_bytes(serialization.Encoding.PEM)
        combined=private.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())+public
        certfile=tmp_path/"server.pem";certfile.write_bytes(combined)
        command+=["--certificate",str(certfile)]
        config.update({"tls_mode":"explicit","ca_certificate_env":"FTP_TEST_CA"})
        monkeypatch.setenv("FTP_TEST_CA",public.decode())
    log=(tmp_path/"listener.log").open("wb")
    process=subprocess.Popen(command,cwd=Path(__file__).resolve().parents[1],env=env,stdout=log,stderr=log,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0)
    monkeypatch.setenv("FTP_PASSWORD",env["FTP_PASSWORD"])
    try:
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if process.poll() is not None:
                log.flush()
                pytest.fail("FTP listener failed: "+(tmp_path/"listener.log").read_text())
            try:
                with socket.create_connection(("127.0.0.1",port),.1):
                    break
            except OSError:
                time.sleep(.05)
        else:
            pytest.fail("FTP listener readiness timeout")
        yield {"protocol":request.param,"endpoint":request.param.lower()+"://localhost:"+str(port),"config":config},folder
    finally:
        process.terminate();process.wait(timeout=10);log.close()


def test_real_ftp_ftps_transfer(ftp_peer):
    connection,folder=ftp_peer
    assert protocols.operate(connection,"test")["reachable"]
    payload=b"purchase-order\x00\xffbinary"
    protocols.operate(connection,"send","order.bin",payload)
    assert (folder/"order.bin").read_bytes()==payload
    assert protocols.operate(connection,"receive","order.bin")==payload
    assert protocols.operate(connection,"list")["entries"][0]["name"]=="order.bin"
    with pytest.raises(FileExistsError):
        protocols.operate(connection,"send","order.bin",b"overwrite")
    assert (folder/"order.bin").read_bytes()==payload


def test_kafka_publish_requires_broker_ack(monkeypatch):
    from backend import kafka
    import confluent_kafka
    message=SimpleNamespace(topic=lambda:"orders",partition=lambda:1,offset=lambda:42)
    producer=MagicMock()
    producer.produce.side_effect=lambda *a,**kw:kw["on_delivery"](None,message)
    producer.flush.return_value=0
    factory=MagicMock(return_value=producer)
    monkeypatch.setattr(confluent_kafka,"Producer",factory)
    connection={"id":"k","protocol":"KAFKA","endpoint":"kafka://localhost:9092","config":{"topic":"orders","security_protocol":"PLAINTEXT"}}
    result=protocols.operate(connection,"send","order.xml",b"abc")
    assert result["offset"]==42
    assert factory.call_args.args[0]["enable.idempotence"] is True
    assert factory.call_args.args[0]["acks"]=="all"
    assert producer.produce.call_args.kwargs["value"]==b"abc"
    producer.flush.return_value=1
    with pytest.raises(ValueError,match="uncertain"):
        protocols.operate(connection,"send","order.xml",b"abc")


def test_kafka_consumer_durable_before_commit_and_idempotent_replay(client,tmp_path,monkeypatch):
    from backend import kafka
    import confluent_kafka
    source=profile(client,"KAFKA","kafka://localhost:9092",{
        "topic":"orders","security_protocol":"PLAINTEXT","enable_receiver":True,"consumer_group_id":"relay"})
    root=tmp_path/"nas";root.mkdir()
    destination=profile(client,"FILE",str(root));make_flow(client,source,destination)
    message=SimpleNamespace(error=lambda:None,value=lambda:b"document",headers=lambda:[("filename",b"order.bin")],
        topic=lambda:"orders",partition=lambda:0,offset=lambda:7)
    consumer=MagicMock()
    consumer.poll.side_effect=[message,None,message,None]
    def commit(*args,**kwargs):
        assert client.get("/api/connectors/inbox").json()["documents"][0]["bytes"]==8
        assert len(flows.list_jobs())==1
        return []
    consumer.commit.side_effect=commit
    monkeypatch.setattr(confluent_kafka,"Consumer",lambda *args,**kwargs:consumer)
    try:
        assert kafka.poll_receiver(source)==1
        assert kafka.poll_receiver(source)==1
        assert len(client.get("/api/connectors/inbox").json()["documents"])==1
        assert flows.process_next_job()
        assert (root/"order.bin").read_bytes()==b"document"
    finally:
        kafka.close_receivers()


def test_kafka_never_commits_failed_storage(client,monkeypatch):
    from backend import kafka
    import confluent_kafka
    source=profile(client,"KAFKA","kafka://localhost:9092",{
        "topic":"orders","security_protocol":"PLAINTEXT","enable_receiver":True,"consumer_group_id":"relay"})
    message=SimpleNamespace(error=lambda:None,value=lambda:b"doc",headers=lambda:[],topic=lambda:"orders",partition=lambda:0,offset=lambda:1)
    consumer=MagicMock();consumer.poll.return_value=message
    monkeypatch.setattr(confluent_kafka,"Consumer",lambda *args,**kwargs:consumer)
    monkeypatch.setattr(inbox,"accept",lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError("disk failure")))
    with pytest.raises(RuntimeError):
        kafka.poll_receiver(source)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_ldap_lookup_filter_escape_and_authentication(client,monkeypatch):
    from backend import directory
    connection=profile(client,"LDAP","ldaps://directory.test",{"base_dn":"dc=test","password_env":"LDAP_PASSWORD","bind_dn":"cn=sync"})
    searched=MagicMock()
    searched.result={"result":0}
    searched.response=[{"type":"searchResEntry","dn":"cn=alice,dc=test","attributes":{"sAMAccountName":"alice","mail":"alice@example.test"}}]
    binds=[]
    @contextmanager
    def session(connection,options,user=None,password=None):
        if user is not None:
            binds.append((user,password))
            if password!="correct":
                raise ValueError("Denied")
        yield searched
    monkeypatch.setattr(directory,"session",session)
    assert client.get("/api/connections/"+connection["id"]+"/users").json()["users"][0]["username"]=="alice"
    assert client.post("/api/connections/"+connection["id"]+"/authenticate",json={"username":"alice*)(uid=*)","password":"correct"}).status_code==200
    filter_=searched.search.call_args.args[1]
    assert r"alice\2a\29\28uid=\2a\29" in filter_
    assert binds[-1]==("cn=alice,dc=test","correct")
    assert client.post("/api/connections/"+connection["id"]+"/authenticate",json={"username":"alice","password":"wrong"}).status_code==401
    assert client.post("/api/connections/"+connection["id"]+"/operate",json={"operation":"send","path":"a","content_base64":"YQ=="}).status_code==400
    file_connection=profile(client,"FILE",str(Path(__file__).parent.resolve()))
    assert client.post("/api/flows",json={"name":"invalid","source_id":connection["id"],"destination_id":file_connection["id"]}).status_code==400


def test_sharepoint_oauth_upload_download_and_conflict(monkeypatch):
    from backend import sharepoint
    monkeypatch.setenv("SP_SECRET","client-secret")
    uploaded=[];conflict=[False]
    def handle(request):
        path=request.url.path
        if request.url.host=="login.microsoftonline.com":
            assert b"client_secret=client-secret" in request.content
            return httpx.Response(200,json={"access_token":"token"})
        if path=="/v1.0/sites/tenant.sharepoint.com:/sites/B2B":
            return httpx.Response(200,json={"id":"site"})
        if path=="/v1.0/sites/site/drives":
            return httpx.Response(200,json={"value":[{"name":"Documents","id":"drive"}]})
        if path.endswith("/children"):
            return httpx.Response(200,json={"value":[{"name":"order.xml","size":3,"file":{}}]})
        if path.endswith("/createUploadSession"):
            assert request.headers["Authorization"]=="Bearer token"
            assert request.read() and json.loads(request.content)["item"]["@microsoft.graph.conflictBehavior"]=="fail"
            return httpx.Response(409 if conflict[0] else 200,json={"uploadUrl":"https://tenant.sharepoint.com/upload"})
        if request.url.host=="tenant.sharepoint.com" and path=="/upload":
            assert "authorization" not in request.headers
            assert request.headers["content-range"]=="bytes 0-2/3"
            uploaded.append(request.content)
            return httpx.Response(201,json={"id":"file"})
        if path.endswith("/content"):
            return httpx.Response(302,headers={"Location":"https://tenant.sharepoint.com/download"})
        if path=="/download":
            assert "authorization" not in request.headers
            return httpx.Response(200,content=b"abc")
        return httpx.Response(200,json={"id":"file","size":3,"eTag":'"etag"'})
    real_client=httpx.Client
    monkeypatch.setattr(sharepoint.httpx,"Client",lambda **kwargs:real_client(transport=httpx.MockTransport(handle)))
    connection={"protocol":"SHAREPOINT","endpoint":"https://tenant.sharepoint.com/sites/B2B","config":{
        "tenant_id":"tenant-id","application_id":"app-id","application_secret_env":"SP_SECRET","drive_name":"Documents","root_path":"orders"}}
    assert protocols.operate(connection,"send","order.xml",b"abc")["bytes"]==3
    assert uploaded==[b"abc"]
    assert protocols.operate(connection,"receive","order.xml")==b"abc"
    assert protocols.operate(connection,"list")["entries"][0]["name"]=="order.xml"
    conflict[0]=True
    with pytest.raises(FileExistsError):
        protocols.operate(connection,"send","order.xml",b"abc")
    with pytest.raises(ValueError):
        sharepoint.trusted_download("https://evil.test/download")


def test_inbox_rejects_reused_identity(client):
    first=inbox.accept("endpoint","message:1","first.bin",b"abc")
    replay=inbox.accept("endpoint","message:1","first.bin",b"abc")
    assert replay["duplicate"] and replay["id"]==first["id"]
    with pytest.raises(ValueError):
        inbox.accept("endpoint","message:1","first.bin",b"changed")


def test_ftp_staging_hides_partial_upload(ftp_peer):
    from backend.extended import ftp_session
    connection,folder=ftp_peer
    options=protocols.FTPOptions.model_validate(connection["config"])
    with ftp_session(connection["protocol"],connection["endpoint"],options) as channel:
        channel.voidcmd("TYPE I")
        data_channel=channel.transfercmd("STOR staged.bin")
        data_channel.sendall(b"first")
        time.sleep(.1)
        assert not (folder/"staged.bin").exists()
        data_channel.sendall(b"-last")
        import ssl
        if isinstance(data_channel,ssl.SSLSocket):
            data_channel=data_channel.unwrap()
        data_channel.close()
        assert channel.voidresp().startswith("226")
        assert (folder/"staged.bin").read_bytes()==b"first-last"
        assert list((folder/".relay-incoming").iterdir())==[]


def test_file_parent_creation_post_process_and_failure(tmp_path,monkeypatch):
    root=tmp_path/"root";root.mkdir()
    command=[sys.executable,"-c","from pathlib import Path; import sys; Path(sys.argv[1]).with_suffix('.done').write_bytes(Path(sys.argv[1]).read_bytes())",chr(36)+"{file}"]
    monkeypatch.setenv("FILE_PROCESS",json.dumps(command))
    connection={"protocol":"FILE","endpoint":str(root),"config":{"force_make_directories":True,"post_process_command_env":"FILE_PROCESS"}}
    protocols.operate(connection,"send","orders/order.xml",b"<order/>")
    assert (root/"orders"/"order.done").read_bytes()==b"<order/>"
    monkeypatch.setenv("FILE_PROCESS",json.dumps([sys.executable,"-c","raise SystemExit(2)"]))
    with pytest.raises(ValueError,match="uncertain"):
        protocols.operate(connection,"send","orders/failed.xml",b"retained")
    assert (root/"orders"/"failed.xml").read_bytes()==b"retained"


def test_ldap_tls_precedes_bind_and_referrals_are_disabled(monkeypatch):
    from backend import directory
    events=[]
    peer=MagicMock()
    peer.open.side_effect=lambda:events.append("open")
    peer.start_tls.side_effect=lambda:events.append("starttls") or True
    peer.bind.side_effect=lambda:events.append("bind") or True
    factory=MagicMock(return_value=peer)
    monkeypatch.setenv("LDAP_SYNC","secret")
    monkeypatch.setattr(directory.ldap3,"Connection",factory)
    connection={"endpoint":"ldap://directory.test"}
    options=protocols.LDAPOptions(bind_dn="cn=sync",password_env="LDAP_SYNC")
    assert directory.test_directory(connection,options)["reachable"]
    assert events==["open","starttls","bind"]
    assert factory.call_args.kwargs["auto_referrals"] is False
    assert factory.call_args.args[0].tls.validate==2


def test_scoped_catalog_matches_available_protocols(client):
    report=client.get("/api/connector-catalog").json()
    assert {item["id"] for item in report["connectors"]}=={"HTTP","FTP","FILE","KAFKA","LDAP","SFTP","SHAREPOINT","SMB","AS2","GCS","EMS"}
    for item in report["connectors"]:
        assert item["id"] in protocols.OPTION_TYPES
        assert item["gaps"] and item["verification"]
    for config in Path("samples").glob("*/connection.json"):
        template=json.loads(config.read_text())
        if template["protocol"]=="FILE":
            template["endpoint"]=str(Path.cwd())
        main.Connection.model_validate(template)
