"""Server-only, named Kafka connection settings. Never return file contents to clients."""
from .protocols import KafkaOptions

OPTION_KEYS = {
    "bootstrap.servers": "bootstrap_servers", "security.protocol": "security_protocol",
    "sasl.mechanism": "sasl_mechanism", "sasl.mechanisms": "sasl_mechanism",
    "sasl.username": "username", "client.id": "client_id", "topic": "topic",
    "group.id": "consumer_group_id", "auto.offset.reset": "auto_offset_reset",
}
SECRET_KEYS = {"sasl.password", "ssl.ca.location", "ssl.certificate.location",
               "ssl.key.location", "ssl.key.password"}


def load_settings(options):
    from .connection_properties import resolve_connection
    connection = resolve_connection({"protocol": "KAFKA", "endpoint": "", "config": options.model_dump()})
    return KafkaOptions.model_validate(connection["config"]), connection.get("_kafka_native", {})
