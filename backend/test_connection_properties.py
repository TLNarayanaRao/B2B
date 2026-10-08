"""Shared properties resolution, API boundaries, and inbound/flow integration."""
import base64
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import main, protocols, flows
from backend.connection_properties import resolve_connection


@pytest.fixture
def server(tmp_path, monkeypatch):
    folder = tmp_path / "configuration"
    folder.mkdir()
    monkeypatch.setenv("RELAY_CONFIG_DIR", str(folder))
    monkeypatch.setattr(main, "DB", tmp_path / "profiles.db")
    return folder, TestClient(main.app)


@pytest.mark.parametrize("protocol", list(protocols.OPTION_TYPES))
def test_every_type_uses_its_own_file_and_keeps_profile_unresolved(server, monkeypatch, protocol):
    folder, client = server
    sample = json.loads((Path("samples") / protocol.lower() / "connection.json").read_text())
    if protocol == "FILE":
        sample["endpoint"] = str(folder.parent)
    config = sample["config"]
    lines = ["endpoint=" + sample["endpoint"], "timeout=17"]
    for key, value in config.items():
        value = str(value).lower() if isinstance(value, bool) else str(value)
        lines.append(key + "=" + value)
    (folder / f"{protocol}.acme.properties").write_text("\n".join(lines), encoding="utf-8")
    captured = []
    def adapter(connection, operation):
        effective = resolve_connection(connection)
        captured.append(effective)
        return {"reachable": True}
    monkeypatch.setattr(main, "operate", adapter)
    response = client.post("/api/connections", json={"name": "Partner", "protocol": protocol,
                                                   "config": {"config_key": "acme"}})
    assert response.status_code == 201, response.text
    assert captured[0]["endpoint"] == sample["endpoint"]
    assert captured[0]["config"]["timeout"] == 17
    assert response.json()["endpoint"] == ""
    assert response.json()["config"]["timeout"] == 30
    assert response.json()["config"]["config_key"] == "acme"


def test_real_file_transfer_and_flow_use_file_endpoints(server, tmp_path):
    folder, client = server
    source = tmp_path / "source"; source.mkdir()
    destination = tmp_path / "destination"; destination.mkdir()
    for key, root in (("source", source), ("destination", destination)):
        (folder / f"FILE.{key}.properties").write_text(f"endpoint={root}\nforce_make_directories=true\n")
    def save(key):
        response = client.post("/api/connections", json={"name": key, "protocol": "FILE", "config": {"config_key": key}})
        assert response.status_code == 201, response.text
        return response.json()
    src, dst = save("source"), save("destination")
    operation = client.post(f'/api/connections/{src["id"]}/operate', json={
        "operation": "send", "path": "order.xml", "content_base64": base64.b64encode(b"document").decode()})
    assert operation.status_code == 200, operation.text
    flow = flows.create_flow(flows.Flow(name="Route", source_id=src["id"], destination_id=dst["id"], min_age_seconds=0))
    assert flows.poll_flow(flow["id"], force=True)["queued"] == 1
    assert flows.process_next_job()
    assert (destination / "order.xml").read_bytes() == b"document"


def test_direct_password_and_key_file_are_server_only_and_reload(server):
    folder, client = server
    keyfile = folder / "private.pem"
    keyfile.write_text("private-key-content\n")
    file = folder / "SFTP.acme.properties"
    file.write_text("endpoint=sftp://partner.test:22\nusername=user\npassword=private/secret==\nprivate_key_file=private.pem\n")
    raw = {"protocol": "SFTP", "endpoint": "", "config": {"config_key": "acme"}}
    effective = resolve_connection(raw)
    assert protocols.secret(effective["config"]["password_env"], True) == "private/secret=="
    assert protocols.secret(effective["config"]["private_key_env"], True) == "private-key-content\n"
    assert "private/secret" not in json.dumps(effective)
    file.write_text(file.read_text().replace("private/secret==", "rotated/secret=="))
    assert protocols.secret(effective["config"]["password_env"], True) == "rotated/secret=="
    assert raw["config"] == {"config_key": "acme"}


def test_http_inbound_auth_reads_named_file_and_never_exposes_token(server, monkeypatch):
    import httpx
    from backend import extended
    monkeypatch.setattr(extended, "http_session", lambda options: httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))))
    folder, client = server
    file = folder / "HTTP.acme.properties"
    file.write_text("endpoint=http://partner.test\nenable_inbound=true\ninbound_only=true\ninbound_token=private-token\n")
    response = client.post("/api/connections", json={"name": "Inbox", "protocol": "HTTP", "config": {"config_key": "acme"}})
    assert response.status_code == 201, response.text
    url = "/http/" + response.json()["id"] + "/order.xml"
    assert client.put(url, content=b"payload").status_code == 401
    assert client.put(url, headers={"Authorization": "Bearer private-token"}, content=b"payload").status_code == 201
    assert "private-token" not in client.get("/api/connections").text
    file.write_text(file.read_text().replace("private-token", "rotated-token"))
    assert client.get(url, headers={"Authorization": "Bearer private-token"}).status_code == 401
    assert client.get(url, headers={"Authorization": "Bearer rotated-token"}).content == b"payload"


def test_as2_inbound_transport_and_ldap_actions_resolve_file(server, monkeypatch):
    folder, client = server
    monkeypatch.setattr(main, "operate", lambda *args: {"reachable": True})
    (folder / "AS2.acme.properties").write_text("endpoint=http://partner.test\nsign=false\nencrypt=false\ninbound_username=partner\ninbound_password=private-password\n")
    saved = client.post("/api/connections", json={"name": "AS2", "protocol": "AS2", "config": {"config_key": "acme"}}).json()
    assert client.head("/as2/" + saved["id"]).status_code == 401
    assert client.head("/as2/" + saved["id"], auth=("partner", "private-password")).status_code == 200
    (folder / "LDAP.acme.properties").write_text("endpoint=ldaps://directory.test\nbind_dn=CN=sync\npassword=directory-secret\nbase_dn=DC=example\n")
    saved = client.post("/api/connections", json={"name": "Directory", "protocol": "LDAP", "config": {"config_key": "acme"}}).json()
    from backend import directory
    def lookup(connection, options):
        assert connection["endpoint"] == "ldaps://directory.test"
        assert protocols.secret(options.password_env) == "directory-secret"
        return {"users": []}
    monkeypatch.setattr(directory, "lookup", lookup)
    assert client.get(f'/api/connections/{saved["id"]}/users').json() == {"users": []}


@pytest.mark.parametrize("contents", ["endpoint=sftp://partner\nunknown=private-secret\n", "endpoint=sftp://user:private-secret@partner\n", "endpoint=sftp://partner\ntimeout=private-secret\n", "endpoint=sftp://partner\npassword=one\npassword_env=ENV\n"])
def test_invalid_files_fail_safely(server, contents):
    folder, client = server
    (folder / "SFTP.acme.properties").write_text(contents)
    response = client.post("/api/connections", json={"name": "Bad", "protocol": "SFTP", "config": {"config_key": "acme"}})
    assert response.status_code == 422
    assert "private-secret" not in response.text
    assert client.get("/api/connections").json() == []


def test_connection_type_isolation_and_missing_file_preserve_profile(server, tmp_path):
    folder, client = server
    (folder / "FILE.acme.properties").write_text(f"endpoint={tmp_path}\n")
    saved = client.post("/api/connections", json={"name": "Files", "protocol": "FILE", "config": {"config_key": "acme"}}).json()
    response = client.post("/api/connections", json={"name": "FTP", "protocol": "FTP", "config": {"config_key": "acme"}})
    assert response.status_code == 422
    assert "FTP.acme.properties" in response.text
    (folder / "FILE.acme.properties").unlink()
    response = client.put('/api/connections/' + saved['id'], json={"name": "Update", "protocol": "FILE", "config": {"config_key": "acme"}})
    assert response.status_code == 422
    assert client.get("/api/connections").json() == [saved]


def test_kafka_generic_endpoint_password_and_receiver_rotation(server, monkeypatch):
    from unittest.mock import MagicMock
    import confluent_kafka
    from backend import kafka
    folder, client = server
    file = folder / "KAFKA.acme.properties"
    file.write_text("endpoint=kafka://broker.test:9092\nsecurity_protocol=SASL_SSL\nsasl_mechanism=PLAIN\nusername=key\npassword=first-secret\ntopic=orders\nenable_receiver=true\nconsumer_group_id=relay\n")
    raw = {"id": "k", "protocol": "KAFKA", "endpoint": "", "config": {"config_key": "acme"}}
    options, native = kafka.load_settings(protocols.KafkaOptions(config_key="acme"))
    assert options.bootstrap_servers == "broker.test:9092"
    assert kafka.configuration(raw, options, native)["sasl.password"] == "first-secret"
    consumer = MagicMock(); consumer.poll.return_value = None
    factory = MagicMock(return_value=consumer)
    monkeypatch.setattr(confluent_kafka, "Consumer", factory)
    try:
        assert kafka.poll_receiver(raw) == 0
        file.write_text(file.read_text().replace("first-secret", "second-secret"))
        assert kafka.poll_receiver(raw) == 0
        assert factory.call_count == 2
        assert factory.call_args.args[0]["sasl.password"] == "second-secret"
        consumer.close.assert_called_once()
    finally:
        kafka.close_receivers()
