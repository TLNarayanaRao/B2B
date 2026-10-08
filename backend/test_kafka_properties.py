import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import confluent_kafka
import pytest
from fastapi.testclient import TestClient

from backend import kafka, main, protocols
from backend.kafka_properties import load_settings


@pytest.fixture
def settings(tmp_path, monkeypatch):
    folder = tmp_path / "configuration"
    folder.mkdir()
    monkeypatch.setenv("RELAY_CONFIG_DIR", str(folder))
    monkeypatch.setattr(main, "DB", tmp_path / "profiles.db")
    file = folder / "Kafka.xyz.properties"
    file.write_text("# Vendor settings\nbootstrap.servers=vendor.test:9092\n"
                    "security.protocol=SASL_SSL\nsasl.mechanisms=PLAIN\n"
                    "sasl.username=api-key\nsasl.password=private/secret+value==\n"
                    "topic=orders\ngroup.id=relay-xyz\n", encoding="utf-8-sig")
    producer = MagicMock()
    producer.list_topics.return_value = SimpleNamespace(topics={
        "orders": SimpleNamespace(error=None, partitions={0: object()})})
    factory = MagicMock(return_value=producer)
    monkeypatch.setattr(confluent_kafka, "Producer", factory)
    yield file, producer, factory, TestClient(main.app)
    kafka.close_receivers()


def candidate(**config):
    return {"name": "Vendor", "protocol": "KAFKA", "config": {"config_key": "xyz", **config}}


def test_key_only_save_uses_file_without_persisting_credentials(settings):
    file, producer, factory, client = settings
    response = client.post("/api/connections", json=candidate())
    assert response.status_code == 201, response.text
    saved = response.json()
    assert saved["endpoint"] == ""
    assert saved["config"]["config_key"] == "xyz"
    assert saved["config"]["username"] == ""
    native = factory.call_args.args[0]
    assert native["bootstrap.servers"] == "vendor.test:9092"
    assert native["security.protocol"] == "SASL_SSL"
    assert native["sasl.mechanism"] == "PLAIN"
    assert native["sasl.password"] == "private/secret+value=="
    assert "private/secret+value" not in json.dumps(client.get("/api/connections").json())
    assert "api-key" not in main.DB.read_bytes().decode(errors="ignore")
    producer.produce.assert_not_called()


@pytest.mark.parametrize("key", ["../xyz", "xyz/other", "xyz\\other", ".", "C:xyz", "xyz.properties"])
def test_configuration_key_rejects_paths(settings, key):
    client = settings[3]
    assert client.post("/api/connections", json=candidate(config_key=key)).status_code == 422


@pytest.mark.parametrize("contents,reason", [
    ("sasl.password=private-secret\n", "requires bootstrap.servers"),
    ("bootstrap.servers=x\nsasl.password=one\nsasl.password=private-secret\n", "duplicate"),
    ("bootstrap.servers=x\nsasl.jaas.config=private-secret\n", "unsupported"),
    ("bootstrap.servers=x\nsecurity.protocol=private-secret\n", "Invalid Kafka properties values"),
    ("bootstrap.servers=x\nssl.certificate.location=client.pem\n", "both client certificate"),
])
def test_bad_file_fails_safely_without_save(settings, contents, reason):
    file, producer, factory, client = settings
    file.write_text(contents)
    response = client.post("/api/connections", json=candidate())
    assert response.status_code == 422
    assert reason in response.json()["detail"]
    assert "private-secret" not in response.text
    assert client.get("/api/connections").json() == []
    factory.assert_not_called()


def test_missing_file_fails_with_filename(settings):
    file, producer, factory, client = settings
    file.unlink()
    response = client.post("/api/connections", json=candidate())
    assert response.status_code == 422
    assert "Cannot read Kafka.xyz.properties" in response.json()["detail"]
    factory.assert_not_called()


def test_publish_reloads_file_and_preserves_acknowledgements(settings):
    file, producer, factory, client = settings
    saved = client.post("/api/connections", json=candidate()).json()
    message = SimpleNamespace(topic=lambda: "orders", partition=lambda: 0, offset=lambda: 9)
    producer.produce.side_effect = lambda *args, **kw: kw["on_delivery"](None, message)
    producer.flush.return_value = 0
    file.write_text(file.read_text(encoding="utf-8-sig").replace("api-key", "rotated-key"))
    assert protocols.operate(saved, "send", "order.xml", b"payload")["offset"] == 9
    assert factory.call_args.args[0]["sasl.username"] == "rotated-key"
    assert factory.call_args.args[0]["acks"] == "all"
    assert factory.call_args.args[0]["enable.idempotence"] is True
    assert producer.produce.call_args.args[0] == "orders"


def test_consumer_reconnects_on_file_changes_and_never_auto_commits(settings, monkeypatch):
    file, producer, factory, client = settings
    saved = client.post("/api/connections", json=candidate(enable_receiver=True)).json()
    consumer = MagicMock()
    consumer.poll.return_value = None
    consumers = MagicMock(return_value=consumer)
    monkeypatch.setattr(confluent_kafka, "Consumer", consumers)
    assert kafka.poll_receiver(saved) == 0
    assert kafka.poll_receiver(saved) == 0
    assert consumers.call_count == 1
    native = consumers.call_args.args[0]
    assert native["group.id"] == "relay-xyz"
    assert native["enable.auto.commit"] is False
    assert native["enable.auto.offset.store"] is False
    file.write_text(file.read_text(encoding="utf-8-sig").replace("private/secret+value==", "rotated-secret"))
    assert kafka.poll_receiver(saved) == 0
    assert consumers.call_count == 2
    assert consumers.call_args.args[0]["sasl.password"] == "rotated-secret"
    consumer.close.assert_called_once()
    file.unlink()
    with pytest.raises(protocols.ConnectorError, match="Cannot read"):
        kafka.poll_receiver(saved)
    assert consumer.poll.call_count == 3


def test_receiver_requires_effective_group_and_relative_tls_paths_resolve(settings):
    file = settings[0]
    file.write_text("bootstrap.servers=x\nsecurity.protocol=SSL\nssl.ca.location=ca.pem\ntopic=orders\n")
    options, native = load_settings(protocols.KafkaOptions(config_key="xyz"))
    assert native["ssl.ca.location"] == str((file.parent / "ca.pem").resolve())
    with pytest.raises(protocols.ConnectorError, match="requires group.id"):
        load_settings(protocols.KafkaOptions(config_key="xyz", enable_receiver=True))


def test_properties_cannot_override_durable_commit_policy(settings):
    file = settings[0]
    file.write_text("bootstrap.servers=x\nenable.auto.commit=true\n")
    with pytest.raises(protocols.ConnectorError, match="unsupported"):
        load_settings(protocols.KafkaOptions(config_key="xyz"))
