"""Resolve ConnectionType.PartnerKey.properties only on the API server."""
import hashlib
import json
import os
from pathlib import Path

from .protocols import ConnectorError, OPTION_TYPES, validate_endpoint

# References contain no credentials. They identify server-side properties.
SECRET_REFERENCES = {}


def configuration_root():
    return Path(os.environ.get("RELAY_CONFIG_DIR", Path(__file__).resolve().parents[1] / "configuration")).resolve()


def read_properties(protocol, key):
    import re
    if protocol not in OPTION_TYPES or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", key):
        raise ConnectorError("Invalid connection type or configuration key")
    root = configuration_root()
    # Keep existing Kafka files compatible on case-sensitive hosts as well.
    filename = f"{'Kafka' if protocol == 'KAFKA' else protocol}.{key}.properties"
    canonical = root / f"{protocol}.{key}.properties"
    legacy = root / filename
    if canonical.exists() and legacy.exists() and canonical.resolve() != legacy.resolve():
        raise ConnectorError("Multiple properties files match this connection key")
    path = (canonical if canonical.exists() else legacy).resolve()
    if path.parent != root:
        raise ConnectorError("Properties file must stay inside the configuration folder")
    try:
        with path.open("r", encoding="utf-8-sig") as source:
            contents = source.read(65537)
    except (OSError, UnicodeError) as exc:
        raise ConnectorError(f"Cannot read {filename} in the server configuration folder") from exc
    if len(contents) > 65536:
        raise ConnectorError("Properties file exceeds 64 KiB")
    properties = {}
    for number, line in enumerate(contents.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        name, separator, value = line.partition("=")
        name, value = name.strip(), value.strip()
        if not separator or not name or name in properties:
            raise ConnectorError(f"Invalid or duplicate {protocol} property on line {number}")
        properties[name] = value
    return properties


def register_secret(protocol, key, name):
    descriptor = (protocol, key, name)
    reference = "RELAY_PROPERTY_" + hashlib.sha256(json.dumps(descriptor).encode()).hexdigest()
    SECRET_REFERENCES[reference] = descriptor
    return reference


def property_secret(reference):
    descriptor = SECRET_REFERENCES.get(reference)
    if descriptor is None:
        return None
    protocol, key, name = descriptor
    value = read_properties(protocol, key).get(name)
    if name.endswith("_file") and value:
        path = Path(value)
        path = path if path.is_absolute() else configuration_root() / path
        try:
            value = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            raise ConnectorError("Cannot read a credential file referenced by the connection properties") from exc
    if not value:
        raise ConnectorError("Required credential property is missing or empty")
    return value


def resolve_connection(connection):
    protocol = connection["protocol"]
    key = connection.get("config", {}).get("config_key", "")
    if not key:
        return connection
    properties = read_properties(protocol, key)
    model = OPTION_TYPES[protocol]
    fields = model.model_fields
    aliases = {name[:-4]: name for name in fields if name.endswith("_env")}
    aliases.update({name[:-4] + "_file": name for name in fields if name.endswith("_env")})
    kafka_options, kafka_native = {}, set()
    if protocol == "KAFKA":
        from .kafka_properties import OPTION_KEYS, SECRET_KEYS
        kafka_options, kafka_native = OPTION_KEYS, SECRET_KEYS
    allowed = set(fields) | set(aliases) | set(kafka_options) | kafka_native | {"endpoint"}
    if any(name not in allowed or name == "config_key" for name in properties):
        raise ConnectorError(f"Invalid, unsupported or duplicate {protocol} property")
    if "sasl.mechanism" in properties and "sasl.mechanisms" in properties:
        raise ConnectorError("Use only one SASL mechanism property")
    values = dict(connection.get("config", {}))
    native = {}
    destinations = set()
    for name, value in properties.items():
        if name == "endpoint":
            continue
        if name in kafka_native:
            native[name] = value
            continue
        target = aliases.get(name, kafka_options.get(name, name))
        if target in destinations:
            raise ConnectorError("Multiple properties configure the same connection field")
        destinations.add(target)
        if name in aliases:
            value = register_secret(protocol, key, name)
        elif fields[target].annotation == list[str]:
            try:
                value = json.loads(value)
            except ValueError as exc:
                raise ConnectorError("List properties must contain a JSON array") from exc
        values[target] = value
    # Validate the complete effective configuration, including inbound requirements.
    values["config_key"] = ""
    if protocol == "KAFKA" and str(values.get("enable_receiver", False)).lower() in ("true", "1") and not values.get("consumer_group_id"):
        raise ConnectorError("Event stream receiver requires group.id in the properties file or Consumer Group Id in the connection")
    try:
        options = model.model_validate(values)
    except ValueError as exc:
        label = "Kafka" if protocol == "KAFKA" else protocol
        raise ConnectorError(f"Invalid {label} properties values; verify required fields and value types") from exc
    endpoint = properties.get("endpoint", connection.get("endpoint", ""))
    if protocol == "KAFKA":
        if not options.bootstrap_servers and not endpoint:
            raise ConnectorError("Kafka properties file requires bootstrap.servers or endpoint")
        if bool(native.get("ssl.certificate.location")) != bool(native.get("ssl.key.location")):
            raise ConnectorError("Kafka properties must provide both client certificate and key locations")
        for name in ("ssl.ca.location", "ssl.certificate.location", "ssl.key.location"):
            if name in native:
                location = Path(native[name])
                native[name] = str((location if location.is_absolute() else configuration_root() / location).resolve())
    if endpoint or protocol != "KAFKA":
        try:
            validate_endpoint(protocol, endpoint)
        except (ValueError, OSError) as exc:
            raise ConnectorError(f"Invalid {protocol} endpoint in the connection properties") from exc
    if protocol == "KAFKA" and not options.bootstrap_servers:
        from urllib.parse import urlsplit
        options = options.model_copy(update={"bootstrap_servers": urlsplit(endpoint).netloc})
    config = options.model_dump()
    config["config_key"] = key
    return {**connection, "endpoint": endpoint, "config": config, "_kafka_native": native}
