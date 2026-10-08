"""Protocol clients. Credentials resolve from server environment, never SQLite."""
import base64
import hashlib
import hmac
import io
import os
import posixpath
import stat
from contextlib import contextmanager
from itertools import islice
from urllib.parse import urlsplit
from pathlib import Path

import httpx
import paramiko
import smbclient
from google.cloud import storage
from google.oauth2 import service_account
from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_BYTES = 10 * 1024 * 1024


class ConnectorError(ValueError):
    """An actionable, credential-free error safe to display to the operator."""


def secret(reference, required=False):
    if not reference:
        if required:
            raise ConnectorError("A credential environment variable reference is required")
        return None
    from .connection_properties import property_secret
    value = property_secret(reference)
    if value is None:
        value = os.environ.get(reference)
    if not value:
        raise ConnectorError(f"Credential environment variable {reference} is not configured")
    return value


class Options(BaseModel):
    model_config = ConfigDict(extra="forbid")
    config_key: str = Field(default="", max_length=100, pattern=r"^(?:[A-Za-z0-9][A-Za-z0-9_-]*)?$")
    timeout: int = Field(default=30, ge=1, le=120)
    max_entries: int = Field(default=500, ge=1, le=1500)

    @model_validator(mode="after")
    def environment_references(self):
        import re
        for name, value in self.model_dump().items():
            if name.endswith("_env") and value and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value):
                raise ConnectorError(f"{name} must be an environment variable name, not a secret value")
        return self


class SFTPOptions(Options):
    username: str = ""
    password_env: str = ""
    private_key_env: str = ""
    key_passphrase_env: str = ""
    host_key_sha256: str = ""
    compress: bool = False
    force_make_directories: bool = False


class SMBOptions(Options):
    username: str = ""
    domain: str = ""
    force_make_directories: bool = False
    password_env: str = ""
    encrypt: bool = True


class GCSOptions(Options):
    project_id: str = ""
    service_account_env: str = ""


class FileOptions(Options):
    force_make_directories: bool = False
    post_process_command_env: str = ""
    post_process_working_directory: str = ""
    post_process_timeout: int = Field(default=30,ge=1,le=120)

    @model_validator(mode="after")
    def post_process_directory(self):
        relative_path(self.post_process_working_directory,allow_empty=True)
        return self


class EMSOptions(Options):
    username: str = ""
    password_env: str = ""
    classpath_env: str = "RELAY_EMS_CLASSPATH"
    destination: str = ""
    destination_type: str = Field(default="queue", pattern=r"^(queue|topic)$")
    message_type: str = Field(default="bytes", pattern=r"^(bytes|text)$")
    jms_namespace: str = Field(default="javax.jms", pattern=r"^(javax\.jms|jakarta\.jms)$")
    trusted_certificate_env: str = ""

    @model_validator(mode="after")
    def safe_destination(self):
        if any(ord(c) < 32 for c in self.destination):
            raise ConnectorError("EMS destination cannot contain control characters")
        return self


class AS2Options(Options):
    local_as2_id: str = ""
    partner_as2_id: str = ""
    sign: bool = True
    encrypt: bool = True
    compress: bool = False
    signing_key_env: str = ""
    signing_passphrase_env: str = ""
    decryption_key_env: str = ""
    decryption_passphrase_env: str = ""
    partner_certificate_env: str = ""
    partner_signing_certificate_env: str = ""
    partner_encryption_certificate_env: str = ""
    mdn_mode: str = Field(default="SYNC", pattern=r"^(SYNC|ASYNC)$")
    mdn_url: str = ""
    partner_mdn_url: str = ""
    mdn_timeout_seconds: int = Field(default=300, ge=5, le=86400)
    require_signed_mdn: bool | None = None
    require_inbound_signing: bool | None = None
    require_inbound_encryption: bool | None = None
    require_https: bool = False
    inbound_username: str = ""
    inbound_password_env: str = ""
    username: str = ""
    password_env: str = ""
    subject: str = "B2B document"

    @model_validator(mode="after")
    def headers_safe(self):
        for value in (self.local_as2_id, self.partner_as2_id, self.subject):
            if any(ord(c) < 32 for c in value):
                raise ConnectorError("AS2 headers cannot contain control characters")
        for url in (self.mdn_url, self.partner_mdn_url):
            if url:
                validate_endpoint("AS2", url)
        return self



class FTPOptions(Options):
    username: str = ""
    password_env: str = ""
    passive: bool = True
    tls_mode: str = Field(default="explicit", pattern=r"^(explicit|implicit)$")
    ca_certificate_env: str = ""


class HTTPOptions(Options):
    username: str = ""
    password_env: str = ""
    token_env: str = ""
    ca_certificate_env: str = ""
    send_method: str = Field(default="PUT", pattern=r"^(PUT|POST)$")
    enable_inbound: bool = False
    inbound_token_env: str = ""
    inbound_only: bool = False
    require_https: bool = False

    @model_validator(mode="after")
    def inbound_auth(self):
        if self.config_key:
            return self
        if self.enable_inbound and not self.inbound_token_env:
            raise ConnectorError("HTTP inbound requires a bearer token environment reference")
        if self.inbound_only and not self.enable_inbound:
            raise ConnectorError("Inbound-only HTTP requires Enable inbound")
        return self


class KafkaOptions(Options):
    bootstrap_servers: str = ""
    topic: str = ""
    username: str = ""
    password_env: str = ""
    security_protocol: str = Field(default="SASL_SSL", pattern=r"^(PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL)$")
    sasl_mechanism: str = Field(default="SCRAM-SHA-256", pattern=r"^(PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)$")
    ca_certificate_env: str = ""
    client_certificate_env: str = ""
    client_key_env: str = ""
    client_key_password_env: str = ""
    enable_receiver: bool = False
    consumer_group_id: str = ""
    auto_offset_reset: str = Field(default="earliest", pattern=r"^(earliest|latest|error)$")
    poll_timeout_ms: int = Field(default=1000, ge=100, le=10000)
    poll_interval_ms: int = Field(default=1000, ge=100, le=60000)
    max_poll_records: int = Field(default=20, ge=1, le=100)
    message_max_bytes: int = Field(default=1_000_000, ge=1, le=MAX_BYTES)
    partition: int = Field(default=-1, ge=-1)
    record_key: str = ""
    compression_type: str = Field(default="none", pattern=r"^(none|gzip|snappy|lz4|zstd)$")
    client_id: str = "relay-b2b"
    isolation_level: str = Field(default="read_committed", pattern=r"^(read_committed|read_uncommitted)$")

    @model_validator(mode="after")
    def kafka_identity(self):
        import re
        if self.topic and not re.fullmatch(r"[A-Za-z0-9._-]{1,249}",self.topic):
            raise ConnectorError("Event stream topic contains invalid characters")
        if self.enable_receiver and not self.consumer_group_id and not self.config_key:
            raise ConnectorError("Event stream receiver requires Consumer Group Id")
        if bool(self.client_certificate_env)!=bool(self.client_key_env):
            raise ConnectorError("Event stream client certificate and key must be provided together")
        if any(ord(c)<32 for c in self.bootstrap_servers+self.topic+self.client_id):
            raise ConnectorError("Event stream configuration contains control characters")
        return self


class LDAPOptions(Options):
    directory_type: str = "LDAP directory"
    bind_dn: str = ""
    password_env: str = ""
    base_dn: str = ""
    search_filter: str = "(objectClass=person)"
    username_attribute: str = "sAMAccountName"
    email_attribute: str = "mail"
    uid_attribute: str = "objectGUID"
    first_name_attribute: str = "givenName"
    last_name_attribute: str = "sn"
    full_name_attribute: str = "displayName"
    phone_attribute: str = "telephoneNumber"
    home_directory_attribute: str = "homeDirectory"
    start_tls: bool = True
    ca_certificate_env: str = ""
    automatic_dns_lookup: bool = False
    dns_domain: str = ""
    srv_records: list[str] = Field(default_factory=list,max_length=20)

    @model_validator(mode="after")
    def ldap_attributes(self):
        import re
        for key,value in self.model_dump().items():
            if key.endswith("_attribute") and value and not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]*",value):
                raise ConnectorError("LDAP attribute names must be identifiers")
        if not self.search_filter.startswith("(") or not self.search_filter.endswith(")"):
            raise ConnectorError("LDAP search filter must use RFC 4515 parenthesized syntax")
        if self.automatic_dns_lookup and not self.dns_domain:
            raise ConnectorError("Automatic LDAP DNS lookup requires a DNS domain")
        return self


class SharePointOptions(Options):
    tenant_id: str = ""
    application_id: str = ""
    application_secret_env: str = ""
    drive_name: str = "Documents"
    root_path: str = ""

    @model_validator(mode="after")
    def sharepoint_paths(self):
        import re
        for value in (self.tenant_id,self.application_id):
            if value and not re.fullmatch(r"[A-Za-z0-9.-]+",value):
                raise ConnectorError("Document library tenant/application IDs must be identifiers")
        relative_path(self.root_path,allow_empty=True)
        return self


OPTION_TYPES = {"SFTP": SFTPOptions, "SMB": SMBOptions, "GCS": GCSOptions, "AS2": AS2Options, "FILE": FileOptions, "EMS": EMSOptions, "FTP": FTPOptions, "FTPS": FTPOptions, "HTTP": HTTPOptions, "HTTPS": HTTPOptions, "KAFKA": KafkaOptions, "LDAP": LDAPOptions, "SHAREPOINT": SharePointOptions}


def validate_endpoint(protocol, endpoint):
    if protocol == "FILE":
        if not Path(endpoint).is_absolute():
            raise ConnectorError("Mounted NAS root must be an absolute folder path on the Python server")
        return
    if protocol == "SMB":
        endpoint = endpoint.replace("\\", "/")
        if not endpoint.startswith("//") or len(endpoint[2:].split("/")) < 2:
            raise ConnectorError("SMB endpoint must be a UNC path: \\\\server\\share")
        parts = endpoint[2:].split("/")
        if not parts[0] or not parts[1] or any(p in ("..", ".") for p in parts):
            raise ConnectorError("Invalid SMB share path")
        return
    if any(ord(c)<32 for c in endpoint):
        raise ConnectorError("Endpoint cannot contain control characters")
    parsed = urlsplit(endpoint)
    schemes = {"FTP":("ftp",), "FTPS":("ftps",), "HTTP":("http","https"), "HTTPS":("https",), "KAFKA":("kafka",), "LDAP":("ldap","ldaps"), "SHAREPOINT":("https",), "AS2": ("https", "http"), "SFTP": ("sftp",), "GCS": ("gs",), "EMS": ("tcp", "ssl")}
    if parsed.scheme not in schemes[protocol] or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConnectorError(f"Invalid {protocol} endpoint URL; credentials belong in environment references")
    if parsed.port and not 1 <= parsed.port <= 65535:
        raise ConnectorError("Invalid port")
    from urllib.parse import unquote
    decoded=unquote(parsed.path)
    if "\\" in decoded or any(ord(c)<32 for c in decoded) or any(part in ("..", ".") for part in decoded.split("/")):
        raise ConnectorError("Endpoint path cannot contain traversal segments")


def relative_path(path, allow_empty=False):
    if not path and allow_empty:
        return ""
    if not path or path.startswith(("/", "\\")) or "\\" in path or ":" in path or any(p in ("", ".", "..") for p in path.split("/")) or any(ord(c) < 32 for c in path):
        raise ConnectorError("Use a relative path within the endpoint root, without traversal")
    return path


class PinnedHostKey(paramiko.MissingHostKeyPolicy):
    def __init__(self, fingerprint):
        self.fingerprint = fingerprint

    def missing_host_key(self, client, hostname, key):
        actual = "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
        if not hmac.compare_digest(actual, self.fingerprint):
            raise ConnectorError("SFTP host key fingerprint does not match the configured trusted key")


@contextmanager
def sftp_connection(endpoint, options):
    if not options.username or not options.host_key_sha256:
        raise ConnectorError("SFTP requires username and independently verified SHA256 host key fingerprint")
    if not options.password_env and not options.private_key_env:
        raise ConnectorError("SFTP requires a password or private key environment reference")
    key = None
    if options.private_key_env:
        text = secret(options.private_key_env, True)
        for key_type in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
            try:
                key = key_type.from_private_key(io.StringIO(text), password=secret(options.key_passphrase_env))
                break
            except paramiko.SSHException:
                continue
        if key is None:
            raise ConnectorError("Unsupported or invalid SSH private key")
    url = urlsplit(endpoint)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(PinnedHostKey(options.host_key_sha256))
    try:
        client.connect(url.hostname, port=url.port or 22, username=options.username, password=secret(options.password_env), pkey=key, allow_agent=False, look_for_keys=False, timeout=options.timeout, auth_timeout=options.timeout, banner_timeout=options.timeout, compress=options.compress)
        with client.open_sftp() as channel:
            channel.get_channel().settimeout(options.timeout)
            root = channel.normalize(url.path or ".")
            yield channel, root
    finally:
        client.close()


def sftp_path(channel, root, path):
    path = posixpath.join(root, path)
    # Resolve existing parents to reject symlink escapes on reads and writes.
    parent = channel.normalize(posixpath.dirname(path))
    resolved = posixpath.join(parent, posixpath.basename(path))
    try:
        resolved = channel.normalize(resolved)
    except OSError:
        pass
    if resolved != root and not resolved.startswith(root.rstrip("/") + "/"):
        raise ConnectorError("Remote path resolves outside the endpoint root")
    return resolved


@contextmanager
def smb_connection(endpoint, options):
    root = endpoint.replace("/", "\\").rstrip("\\")
    host = root[2:].split("\\")[0]
    if not options.username:
        raise ConnectorError("SMB requires a username")
    cache = {}  # Isolate sessions between endpoints with different credentials.
    try:
        smbclient.register_session(host, username=(options.domain+"\\"+options.username if options.domain and "\\" not in options.username else options.username), password=secret(options.password_env, True), encrypt=options.encrypt, connection_timeout=options.timeout, connection_cache=cache)
        yield root, {"connection_cache": cache}
    finally:
        smbclient.reset_connection_cache(connection_cache=cache)


@contextmanager
def gcs_connection(endpoint, options):
    import json
    credentials = None
    if options.service_account_env:
        credentials = service_account.Credentials.from_service_account_info(json.loads(secret(options.service_account_env, True)))
    client = storage.Client(project=options.project_id or None, credentials=credentials)
    url = urlsplit(endpoint)
    try:
        yield client, client.bucket(url.hostname), url.path.lstrip("/").rstrip("/")
    finally:
        client.close()


def object_name(prefix, path):
    return prefix + "/" + path if prefix else path


def operate(connection, operation, path="", data=None, content_type="application/octet-stream"):
    if connection["protocol"] != "KAFKA":
        from .connection_properties import resolve_connection
        connection = resolve_connection(connection)
    protocol, endpoint = connection["protocol"], connection["endpoint"]
    options = OPTION_TYPES[protocol].model_validate(connection.get("config", {}))
    path = relative_path(path, allow_empty=operation in ("list", "test") or protocol=="KAFKA")
    if protocol in ("FTP","FTPS","HTTP","HTTPS"):
        from .extended import operate_extended
        return operate_extended(connection,options,operation,path,data,content_type)
    if protocol=="KAFKA":
        from .kafka import operate_kafka
        return operate_kafka(connection,options,operation,path,data,content_type)
    if protocol=="LDAP":
        from .directory import test_directory
        if operation!="test":
            raise ConnectorError("LDAP is an authentication connector; use List users or Test authentication")
        return test_directory(connection,options)
    if protocol=="SHAREPOINT":
        from .sharepoint import operate_sharepoint
        return operate_sharepoint(connection,options,operation,path,data,content_type)
    if protocol == "EMS":
        from .ems import operate_ems
        return operate_ems(endpoint, options, operation, path, data, content_type)
    if protocol == "FILE":
        root = Path(endpoint).resolve(strict=True)
        if not root.is_dir():
            raise ConnectorError("Mounted NAS root is not a directory")
        location = (root / path).resolve()
        if not location.is_relative_to(root):
            raise ConnectorError("File path resolves outside the mounted NAS root")
        if operation in ("test", "list"):
            entries = list(islice(location.iterdir(), options.max_entries + 1))
            return {"entries": [{"name": p.name, "size": p.stat().st_size, "directory": p.is_dir(), "modified": p.stat().st_mtime} for p in entries[:options.max_entries]], "truncated": len(entries) > options.max_entries}
        if operation == "receive":
            with location.open("rb") as file:
                return bounded_read(file)
        if options.force_make_directories:
            location.parent.mkdir(parents=True,exist_ok=True)
        with location.open("xb") as file:
            file.write(data)
        if options.post_process_command_env:
            from .file_processing import post_process
            post_process(root,location,options)
        return {"bytes": len(data), "path": path}
    if protocol == "AS2":
        from .as2 import send, test
        if operation == "test":
            return test(endpoint, options)
        if operation != "send":
            raise ConnectorError("AS2 is push-only. Receive messages at /as2/{connection_id}; directory browsing is unavailable")
        return send(endpoint, options, path, data, content_type, connection_id=connection.get("id"))
    if protocol == "SFTP":
        with sftp_connection(endpoint, options) as (channel, root):
            if operation=="send" and options.force_make_directories:
                current=root
                for part in path.split("/")[:-1]:
                    candidate=posixpath.join(current,part)
                    try:
                        channel.stat(candidate)
                    except FileNotFoundError:
                        channel.mkdir(candidate)
                    current=sftp_path(channel,root,candidate[len(root.rstrip("/"))+1:])
            location = sftp_path(channel, root, path) if path else root
            if operation in ("test", "list"):
                entries = list(islice(channel.listdir_iter(location), options.max_entries + 1))
                return {"entries": [{"name": e.filename, "size": e.st_size, "directory": stat.S_ISDIR(e.st_mode), "modified": getattr(e, "st_mtime", None)} for e in entries[:options.max_entries]], "truncated": len(entries) > options.max_entries}
            if operation == "receive":
                with channel.open(location, "rb") as file:
                    return bounded_read(file)
            with channel.open(location, "wx") as file:
                file.write(data)
            return {"bytes": len(data), "path": path}
    if protocol == "SMB":
        with smb_connection(endpoint, options) as (root, kwargs):
            location = root + ("\\" + path.replace("/", "\\") if path else "")
            # Reject symlinks/reparse points below the configured share root.
            current = root
            for segment in path.split("/") if path else []:
                current += "\\" + segment
                try:
                    if smbclient.lstat(current, **kwargs).st_file_attributes & 0x400:
                        raise ConnectorError("SMB reparse points are not allowed")
                except FileNotFoundError:
                    break
            if operation in ("test", "list"):
                with smbclient.scandir(location, **kwargs) as directory:
                    entries = list(islice(directory, options.max_entries + 1))
                    result = [{"name": e.name, "size": e.stat().st_size, "directory": e.is_dir(), "modified": e.stat().st_mtime} for e in entries[:options.max_entries]]
                return {"entries": result, "truncated": len(entries) > options.max_entries}
            if operation == "receive":
                with smbclient.open_file(location, mode="rb", **kwargs) as file:
                    return bounded_read(file)
            if options.force_make_directories:
                import ntpath
                smbclient.makedirs(ntpath.dirname(location),exist_ok=True,**kwargs)
            with smbclient.open_file(location, mode="xb", **kwargs) as file:
                file.write(data)
            return {"bytes": len(data), "path": path}
    with gcs_connection(endpoint, options) as (client, bucket, prefix):
        name = object_name(prefix, path)
        if operation in ("test", "list"):
            listing_prefix = name.rstrip("/") + "/" if name else ""
            blobs = list(client.list_blobs(bucket, prefix=listing_prefix, max_results=options.max_entries + 1, timeout=options.timeout))
            return {"entries": [{"name": b.name[len(prefix)+1:] if prefix else b.name, "size": b.size, "directory": False, "modified": b.updated.timestamp() if b.updated else None} for b in blobs[:options.max_entries]], "truncated": len(blobs) > options.max_entries}
        blob = bucket.blob(name)
        if operation == "receive":
            blob.reload(timeout=options.timeout)
            if blob.size > MAX_BYTES:
                raise ConnectorError("File exceeds the 10 MiB transfer limit")
            return blob.download_as_bytes(timeout=options.timeout, if_generation_match=blob.generation)
        blob.upload_from_string(data, content_type=content_type, if_generation_match=0, timeout=options.timeout)
        return {"bytes": len(data), "path": path}


def bounded_read(file):
    content = file.read(MAX_BYTES + 1)
    if len(content) > MAX_BYTES:
        raise ConnectorError("File exceeds the 10 MiB transfer limit")
    return content
