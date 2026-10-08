"""AS2 signed/encrypted document exchange and durable synchronous/asynchronous MDNs."""
import base64
import hashlib
import hmac
import json
import time
import zlib
from email import message_from_bytes
from types import SimpleNamespace
from urllib.parse import urlsplit

from asn1crypto import cms
import httpx
from pyas2lib.as2 import Mdn, Message, Organization, Partner
from pyas2lib.utils import unquote_as2name

from .protocols import MAX_BYTES, ConnectorError, secret


def schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS as2_outbox (
        message_id TEXT PRIMARY KEY, connection_id TEXT, filename TEXT, bytes INTEGER,
        created REAL, deadline REAL, status TEXT, context TEXT, mic TEXT,
        receipt_headers TEXT, receipt_body BLOB, detail TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS as2_receipts (
        id TEXT PRIMARY KEY, connection_id TEXT, message_id TEXT, request_hash TEXT,
        headers TEXT, body BLOB, callback_url TEXT, status TEXT,
        attempts INTEGER DEFAULT 0, next_attempt REAL DEFAULT 0,
        UNIQUE(connection_id,message_id))""")


class BoundedMessage(Message):
    def _decompress_data(self, payload):
        if payload.get_content_type() != "application/pkcs7-mime" or payload.get_param("smime-type") != "compressed-data":
            return False, payload
        envelope = cms.ContentInfo.load(payload.get_payload(decode=True))
        if envelope["content_type"].native != "compressed_data":
            raise ConnectorError("Invalid AS2 compressed envelope")
        content = envelope["content"]
        if content["compression_algorithm"]["algorithm"].native != "zlib":
            raise ConnectorError("Unsupported AS2 compression algorithm")
        decoder = zlib.decompressobj()
        decoded = decoder.decompress(content["encap_content_info"]["content"].native, MAX_BYTES + 1)
        if len(decoded) > MAX_BYTES or not decoder.eof or decoder.unused_data:
            raise ConnectorError("AS2 decompressed content is oversized or invalid")
        return True, message_from_bytes(decoded)


def credential_bytes(reference):
    value = secret(reference)
    if not value:
        return None
    return value.encode() if "-----BEGIN" in value else base64.b64decode(value, validate=True)


def signed_receipt_required(options):
    return options.sign if options.require_signed_mdn is None else options.require_signed_mdn


def identities(options):
    if not options.local_as2_id or not options.partner_as2_id:
        raise ConnectorError("Local and partner AS2 IDs are required")
    if options.sign and not options.signing_key_env:
        raise ConnectorError("AS2 signing requires a key-pair environment reference")
    verification = credential_bytes(options.partner_signing_certificate_env or options.partner_certificate_env)
    encryption = credential_bytes(options.partner_encryption_certificate_env or options.partner_certificate_env)
    if (options.sign or signed_receipt_required(options)) and not verification:
        raise ConnectorError("AS2 signature verification requires the partner signing certificate")
    if options.encrypt and not encryption:
        raise ConnectorError("AS2 encryption requires the partner encryption certificate")
    if options.mdn_mode == "ASYNC" and not options.mdn_url:
        raise ConnectorError("Asynchronous AS2 requires your MDN callback URL")
    org = Organization(
        as2_name=options.local_as2_id, mdn_url=options.mdn_url or None,
        sign_key=credential_bytes(options.signing_key_env),
        sign_key_pass=secret(options.signing_passphrase_env),
        decrypt_key=credential_bytes(options.decryption_key_env),
        decrypt_key_pass=secret(options.decryption_passphrase_env),
    )
    partner = Partner(
        as2_name=options.partner_as2_id, sign=options.sign, encrypt=options.encrypt,
        compress=options.compress, verify_cert=verification, encrypt_cert=encryption,
        digest_alg="sha256", enc_alg="aes_256_cbc", mdn_mode=options.mdn_mode,
        mdn_digest_alg="sha256" if signed_receipt_required(options) else None,
    )
    return org, partner


def http_client(options):
    auth = (options.username, secret(options.password_env, True)) if options.username else None
    return httpx.Client(timeout=options.timeout, verify=True, follow_redirects=False, trust_env=False, auth=auth)


def test(endpoint, options):
    identities(options)
    with http_client(options) as client:
        response = client.head(endpoint)
        if response.status_code >= 500 or response.status_code in (401, 403):
            response.raise_for_status()
    return {"reachable": True, "http_status": response.status_code,
            "detail": "HTTP endpoint reached; send a document and verify its MDN to test AS2 interoperability."}


def raw_message(headers, body):
    return b"".join(f"{key}: {value}\r\n".encode() for key, value in headers.items()) + b"\r\n" + body


def response_bytes(response):
    parts, length = [], 0
    for chunk in response.iter_bytes():
        length += len(chunk)
        if length > MAX_BYTES:
            raise ConnectorError("AS2 receipt exceeds the response limit")
        parts.append(chunk)
    return b"".join(parts)


def request_hash(headers, body):
    # Include AS2/MIME semantics, but tolerate transport headers changing on replay.
    keys = ("message-id", "as2-from", "as2-to", "content-type",
            "disposition-notification-to", "disposition-notification-options",
            "receipt-delivery-option", "content-transfer-encoding", "content-disposition")
    selected = {key: headers.get(key, "") for key in keys}
    return hashlib.sha256(raw_message(selected, body)).hexdigest()


def validate_mdn(message, headers, body):
    normalized = {key.lower(): value for key, value in headers.items()}
    if unquote_as2name(normalized.get("as2-from", "")) != message.receiver.as2_name or unquote_as2name(normalized.get("as2-to", "")) != message.sender.as2_name:
        raise ConnectorError("AS2 MDN sender or recipient does not match the original exchange")
    receipt = Mdn()
    status, detail = receipt.parse(raw_message(headers, body), lambda mid, recipient:
        message if mid == message.message_id and recipient == message.receiver.as2_name else None)
    if status != "processed" or detail:
        raise ConnectorError("AS2 delivery was not confirmed by a valid processed MDN")
    if message.receiver.mdn_digest_alg:
        reports = [part for part in receipt.payload.walk() if part.get_content_type() == "message/disposition-notification"]
        mic = reports[0].get_payload()[-1].get("Received-Content-MIC", "").split(",") if reports else []
        if len(mic) != 2 or not message.mic or not hmac.compare_digest(mic[0].strip(), message.mic.decode()):
            raise ConnectorError("Signed AS2 receipt has a missing or mismatched message integrity check")
        if mic[1].strip().lower().replace("-", "") != message.digest_alg.replace("-", ""):
            raise ConnectorError("AS2 receipt MIC algorithm does not match the original signature")
    return receipt


def save_outbound(message, options, connection_id, filename, size):
    from .main import database
    cert = credential_bytes(options.partner_signing_certificate_env or options.partner_certificate_env)
    context = {"local_as2_id":options.local_as2_id, "partner_as2_id":options.partner_as2_id,
               "digest_alg":message.digest_alg or "sha256", "signed_mdn":signed_receipt_required(options),
               "verify_cert":base64.b64encode(cert).decode() if cert else None}
    with database() as db:
        db.execute("INSERT INTO as2_outbox VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (message.message_id, connection_id, filename, size, time.time(),
             time.time()+options.mdn_timeout_seconds, "sending", json.dumps(context),
             message.mic.decode() if message.mic else None, None, None, None))


def update_outbound(message_id, status, detail=None, headers=None, body=None):
    from .main import database
    with database() as db:
        # A callback can arrive while the original HTTP POST is still returning.
        db.execute("""UPDATE as2_outbox SET status=?,detail=?,
                   receipt_headers=COALESCE(?,receipt_headers),receipt_body=COALESCE(?,receipt_body)
                   WHERE message_id=? AND status!='confirmed'""",
                   (status, detail, json.dumps(dict(headers)) if headers is not None else None, body, message_id))


def outbound_status(message_id):
    from .main import database
    with database() as db:
        row = db.execute("SELECT status FROM as2_outbox WHERE message_id=?", (message_id,)).fetchone()
    return row[0] if row else None


def send(endpoint, options, filename, data, content_type, connection_id=None):
    org, partner = identities(options)
    message = Message(sender=org, receiver=partner)
    message.build(data, filename=filename, subject=options.subject, content_type=content_type)
    save_outbound(message, options, connection_id, filename, len(data))
    try:
        with http_client(options) as client:
            with client.stream("POST", endpoint, headers=message.headers, content=message.content) as response:
                response.raise_for_status()
                body = response_bytes(response)
                headers = dict(response.headers)
        if options.mdn_mode == "ASYNC":
            update_outbound(message.message_id, "awaiting_mdn")
        else:
            validate_mdn(message, headers, body)
            update_outbound(message.message_id, "confirmed", headers=headers, body=body)
    except Exception as exc:
        update_outbound(message.message_id, "uncertain", "No verified receipt; reconcile with the partner before resending")
        if isinstance(exc, ConnectorError):
            raise
        raise ConnectorError("AS2 send failed or timed out; delivery is uncertain until the partner's receipt is verified") from exc
    status = outbound_status(message.message_id)
    return {"message_id":message.message_id, "mdn_status":"processed" if status=="confirmed" else "pending",
            "delivery_status":status, "bytes":len(data), "path":filename}


def receive(options, headers, body, duplicate):
    callback = headers.get("receipt-delivery-option", "")
    if callback and (not options.partner_mdn_url or callback != options.partner_mdn_url):
        raise ConnectorError("Partner MDN callback does not match the configured allowed URL")
    org, partner = identities(options)
    partner.sign = options.sign if options.require_inbound_signing is None else options.require_inbound_signing
    partner.encrypt = options.encrypt if options.require_inbound_encryption is None else options.require_inbound_encryption
    message = BoundedMessage()
    status, exception, receipt = message.parse(raw_message(headers, body),
        find_org_cb=lambda name:org if name==org.as2_name else None,
        find_partner_cb=lambda name:partner if name==partner.as2_name else None,
        find_message_cb=duplicate)
    if status != "processed" or exception[0]:
        raise ConnectorError("AS2 signature, encryption, identity, or duplicate validation failed")
    data = message.payload.get_payload(decode=True)
    if data is None or len(data)>MAX_BYTES:
        raise ConnectorError("Invalid or oversized AS2 payload")
    from .flows import filename_only
    return message.message_id, data, receipt, filename_only(message.payload.get_filename()), message.payload.get_content_type()


def accept_async_mdn(connection_id, headers, body):
    from .main import database
    candidate = Mdn()
    try:
        candidate.payload = message_from_bytes(raw_message(headers, body))
        message_id, recipient = candidate.detect_mdn()
    except Exception as exc:
        raise ConnectorError("Invalid asynchronous MDN document") from exc
    with database() as db:
        row = db.execute("SELECT context,mic,status FROM as2_outbox WHERE message_id=? AND connection_id=?",
                         (message_id,connection_id)).fetchone()
    if not row:
        raise ConnectorError("MDN does not match an outbound message for this endpoint")
    context = json.loads(row[0])
    message = Message(sender=SimpleNamespace(as2_name=context["local_as2_id"]),
        receiver=Partner(as2_name=context["partner_as2_id"],
                         verify_cert=base64.b64decode(context["verify_cert"]) if context["verify_cert"] else None,
                         mdn_digest_alg=context["digest_alg"] if context["signed_mdn"] else None))
    message.message_id, message.digest_alg = message_id, context["digest_alg"]
    message.mic = row[1].encode() if row[1] else None
    validate_mdn(message, headers, body)
    update_outbound(message_id, "confirmed", headers=headers, body=body)
    reconcile_jobs()
    return {"message_id":message_id, "status":"confirmed"}


def reconcile_jobs():
    from .main import database
    with database() as db:
        db.execute("UPDATE as2_outbox SET status='uncertain',detail='MDN deadline elapsed; reconcile with partner' WHERE status='awaiting_mdn' AND deadline<?", (time.time(),))
        for message_id, status in db.execute("SELECT message_id,status FROM as2_outbox WHERE status IN ('confirmed','uncertain')").fetchall():
            job_status = "success" if status=="confirmed" else "uncertain"
            db.execute("UPDATE jobs SET status=? WHERE status='awaiting_mdn' AND json_extract(detail,'$.message_id')=?", (job_status,message_id))
            db.execute("UPDATE transfers SET status=? WHERE status='awaiting_mdn' AND json_extract(detail,'$.message_id')=?", (job_status,message_id))


def process_next_receipt():
    from .main import database, get_connection
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT id,connection_id,headers,body,callback_url,attempts FROM as2_receipts WHERE status='queued' AND next_attempt<=? LIMIT 1", (time.time(),)).fetchone()
        if not row:
            return False
        db.execute("UPDATE as2_receipts SET status='sending',attempts=attempts+1 WHERE id=?", (row[0],))
    try:
        from .connection_properties import resolve_connection
        options = resolve_connection(get_connection(row[1]))["config"]
        from .protocols import AS2Options
        with http_client(AS2Options.model_validate(options)) as client:
            response = client.post(row[4], headers=json.loads(row[2]), content=row[3])
            response.raise_for_status()
        status = "delivered"
    except Exception:
        status = "queued" if row[5]+1 < 3 else "failed"
    with database() as db:
        db.execute("UPDATE as2_receipts SET status=?,next_attempt=? WHERE id=?",
                   (status, time.time()+5*(2**row[5]), row[0]))
    return True


def public_info(connection, base_url):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from .protocols import AS2Options
    from .connection_properties import resolve_connection
    connection = resolve_connection(connection)
    org, _ = identities(AS2Options.model_validate(connection["config"]))
    cert = x509.load_der_x509_certificate(org.sign_key[1].asn1.dump()) if org.sign_key else None
    return {"local_as2_id":org.as2_name, "partner_as2_id":connection["config"].get("partner_as2_id"),
            "inbound_url":base_url.rstrip("/")+"/as2/"+connection["id"],
            "mdn_callback_url":base_url.rstrip("/")+"/as2/"+connection["id"]+"/mdn",
            "certificate_pem":cert.public_bytes(serialization.Encoding.PEM).decode() if cert else None,
            "certificate_sha256":cert.fingerprint(hashes.SHA256()).hex() if cert else None,
            "certificate_expires":cert.not_valid_after_utc.isoformat() if cert else None}
