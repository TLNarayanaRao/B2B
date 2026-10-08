"""Save-time checks exercise the production gate, independently of protocol fixtures."""
import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from backend import main
from backend.protocols import ConnectorError


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DB", tmp_path / "save.db")
    return TestClient(main.app)


def candidate(root):
    return {"name": "NAS", "protocol": "FILE", "endpoint": str(root), "config": {}}


@pytest.mark.parametrize("protocol", list(main.OPTION_TYPES))
def test_every_protocol_tests_exact_settings_before_create_and_update(client, tmp_path, monkeypatch, protocol):
    data = json.loads((Path("samples") / protocol.lower() / "connection.json").read_text())
    if protocol == "FILE":
        data["endpoint"] = str(tmp_path)
    calls = []
    def operate(connection, operation):
        assert operation == "test"
        # Nothing has been saved when the first check runs.
        if not calls:
            assert client.get("/api/connections").json() == []
        calls.append(connection)
        return {"reachable": True, "entries": [{"name": "private-file"}]}
    monkeypatch.setattr(main, "operate", operate)
    response = client.post("/api/connections", json=data)
    assert response.status_code == 201, response.text
    record = response.json()
    assert calls[0] == main.Connection.model_validate(data).model_dump()
    assert record["connection_test"]["success"] is True
    assert record["connection_test"]["tested_at"]
    assert record["connection_test"]["detail"] == {}
    data["name"] = "Updated"
    data["config"]["timeout"] = 17
    response = client.put("/api/connections/" + record["id"], json=data)
    assert response.status_code == 200, response.text
    assert calls[-1] == main.Connection.model_validate(data).model_dump()
    assert len(calls) == 2
    assert client.get("/api/connections").json()[0] == response.json()


@pytest.mark.parametrize("result", [
    {"reachable": False}, {"connected": False}, None,
    {"http_status": 401}, {"http_status": 403}, {"http_status": 404},
    {"http_status": 500},
])
def test_negative_results_never_save(client, tmp_path, monkeypatch, result):
    monkeypatch.setattr(main, "operate", lambda *args: result)
    response = client.post("/api/connections", json=candidate(tmp_path))
    assert response.status_code == 422
    assert "Nothing was saved" in response.json()["detail"]
    assert client.get("/api/connections").json() == []


def test_failed_update_preserves_profile_and_evidence(client, tmp_path, monkeypatch):
    data = candidate(tmp_path)
    # Uses the real mounted-file connector for initial save.
    saved = client.post("/api/connections", json=data).json()
    def fail(*args):
        raise ConnectorError("Partner authentication failed")
    monkeypatch.setattr(main, "operate", fail)
    data["name"] = "Failed update"
    data["config"]["timeout"] = 18
    response = client.put("/api/connections/" + saved["id"], json=data)
    assert response.status_code == 422
    assert "Partner authentication failed" in response.json()["detail"]
    assert client.get("/api/connections").json() == [saved]


def test_manual_test_cannot_authorize_later_save(client, tmp_path, monkeypatch):
    data = candidate(tmp_path)
    assert client.post("/api/connections/test", json=data).json()["success"]
    assert client.get("/api/connections").json() == []
    def fail(*args):
        raise RuntimeError("secret-provider-token")
    monkeypatch.setattr(main, "operate", fail)
    data["config"]["timeout"] = 12
    response = client.post("/api/connections", json=data)
    assert response.status_code == 422
    assert "secret-provider-token" not in response.text
    assert "RuntimeError" in response.text
    assert client.get("/api/connections").json() == []


def test_real_file_check_accepts_existing_root_and_rejects_missing(client, tmp_path):
    response = client.post("/api/connections", json=candidate(tmp_path))
    assert response.status_code == 201, response.text
    assert response.json()["connection_test"]["success"]
    response = client.post("/api/connections", json=candidate(tmp_path / "missing"))
    assert response.status_code == 422, response.text
    assert len(client.get("/api/connections").json()) == 1
    assert not (tmp_path / "missing").exists()


def test_invalid_config_and_missing_id_do_not_test(client, tmp_path, monkeypatch):
    def unexpected(*args):
        pytest.fail("An invalid candidate must not start a network test")
    monkeypatch.setattr(main, "operate", unexpected)
    data = candidate(tmp_path)
    data["config"]["timeout"] = 0
    assert client.post("/api/connections", json=data).status_code == 422
    data["config"] = {}
    assert client.put("/api/connections/unknown", json=data).status_code == 404
    assert client.get("/api/connections").json() == []


def test_existing_database_migrates_without_claiming_a_test(client, tmp_path):
    with sqlite3.connect(main.DB) as db:
        db.execute("CREATE TABLE connections (id TEXT PRIMARY KEY,name TEXT,protocol TEXT,endpoint TEXT,config TEXT)")
        db.execute("INSERT INTO connections VALUES (?,?,?,?,?)", ("legacy","NAS","FILE",str(tmp_path),"{}"))
    saved = client.get("/api/connections").json()[0]
    assert saved["connection_test"] is None
    response = client.put("/api/connections/legacy", json=candidate(tmp_path))
    assert response.status_code == 200
    assert response.json()["connection_test"]["success"]
