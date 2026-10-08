"""Publish documents using installed broker JMS client JARs and a Java 17 bridge."""
import base64
import json
import os
import shutil
import subprocess
from pathlib import Path

from .protocols import ConnectorError, secret

BRIDGE = Path(__file__).with_name("ems_bridge") / "RelayEmsBridge.java"


def operate_ems(endpoint, options, operation, filename, data, content_type):
    if operation not in ("test", "send"):
        raise ConnectorError("EMS supports publishing files to a queue or topic; receive files from NAS, SFTP, GCS or inbound AS2")
    if not options.destination:
        raise ConnectorError("EMS queue or topic destination is required")
    java = os.environ.get("RELAY_JAVA") or shutil.which("java")
    if not java:
        raise ConnectorError("EMS requires Java 17; set RELAY_JAVA to the Java executable")
    classpath = secret(options.classpath_env, True)
    trusted = secret(options.trusted_certificate_env)
    if endpoint.startswith("ssl://") and not trusted:
        raise ConnectorError("EMS TLS requires a trusted certificate environment reference")
    values = {
        "url": endpoint, "username": options.username, "password": secret(options.password_env) or "",
        "operation": operation, "destination": options.destination, "destinationType": options.destination_type,
        "messageType": options.message_type, "namespace": options.jms_namespace,
        "filename": filename, "contentType": content_type, "trustedCertificate": trusted or "",
        "data": base64.b64encode(data or b"").decode(),
    }
    stdin = "\n".join(key + "=" + base64.b64encode(value.encode()).decode() for key, value in values.items()) + "\n"
    try:
        result = subprocess.run([java, "--class-path", classpath, str(BRIDGE)], input=stdin, text=True, encoding="utf-8", capture_output=True, timeout=options.timeout + 15, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    except subprocess.TimeoutExpired as exc:
        raise ConnectorError("EMS delivery timed out; acceptance may be uncertain. Reconcile with the broker before retrying") from exc
    except OSError as exc:
        raise ConnectorError("Cannot start the EMS Java bridge; verify Java and the client classpath") from exc
    lines = [line for line in result.stdout.splitlines() if line.startswith("RELAY_RESULT ")]
    if result.returncode != 0 or not lines:
        raise ConnectorError("EMS connection or JMS operation failed; verify client JARs, broker, destination permissions and credentials. A send outcome may be uncertain")
    try:
        detail = json.loads(base64.b64decode(lines[-1].split(" ", 1)[1]))
    except (ValueError, KeyError) as exc:
        raise ConnectorError("EMS bridge returned an invalid result; delivery outcome may be uncertain") from exc
    return {**detail, "bytes": len(data or b""), "path": filename, "destination": options.destination}
