import json
import base64
import binascii
import sqlite3
import threading
import os
import hmac
from contextlib import contextmanager, asynccontextmanager, nullcontext
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, Field, SecretStr, model_validator
from .protocols import OPTION_TYPES, MAX_BYTES, ConnectorError, operate, validate_endpoint
from . import flows, as2

@asynccontextmanager
async def lifespan(app):
    flows.start_worker()
    yield
    flows.stop_worker()


app = FastAPI(title="Relay B2B API", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    # Pydantic includes the rejected input, which can contain pasted credentials.
    return JSONResponse(status_code=422, content={"detail": [
        {key: error[key] for key in ("type", "loc", "msg") if key in error}
        for error in exc.errors()
    ]})


app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_methods=["*"], allow_headers=["*"])
DB = Path(os.environ.get("RELAY_DB_PATH", Path(__file__).with_name("relay.db")))
SCHEMA_LOCK = threading.Lock()


@contextmanager
def database():
    db = sqlite3.connect(DB)
    with SCHEMA_LOCK:
        db.execute("CREATE TABLE IF NOT EXISTS connections (id TEXT PRIMARY KEY, name TEXT, protocol TEXT, endpoint TEXT)")
        db.execute("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, created TEXT, output TEXT)")
        if "connection_test" not in [row[1] for row in db.execute("PRAGMA table_info(connections)")]:
            db.execute("ALTER TABLE connections ADD COLUMN connection_test TEXT")
        if "config" not in [row[1] for row in db.execute("PRAGMA table_info(connections)")]:
            db.execute("ALTER TABLE connections ADD COLUMN config TEXT NOT NULL DEFAULT '{}'")
        db.execute("CREATE TABLE IF NOT EXISTS transfers (id TEXT PRIMARY KEY, connection_id TEXT, operation TEXT, path TEXT, status TEXT, created TEXT, detail TEXT, payload BLOB, message_id TEXT)")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS inbound_message ON transfers(connection_id, message_id) WHERE message_id IS NOT NULL")
        flows.schema(db)
        as2.schema(db)
        from . import inbox
        inbox.schema(db)
        db.execute("CREATE TABLE IF NOT EXISTS receivers (connection_id TEXT PRIMARY KEY, last_poll REAL, received INTEGER, error TEXT)")
    try:
        with db:
            yield db
    finally:
        db.close()


class Connection(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    protocol: Literal["AS2", "SFTP", "SMB", "GCS", "FILE", "EMS", "FTP", "FTPS", "HTTP", "HTTPS", "KAFKA", "LDAP", "SHAREPOINT"]
    endpoint: str = Field(default="", max_length=500)
    config: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_protocol(self):
        self.config = OPTION_TYPES[self.protocol].model_validate(self.config).model_dump()
        if not (self.config.get("config_key") and not self.endpoint):
            validate_endpoint(self.protocol, self.endpoint)
        return self


class Transformation(BaseModel):
    payload: str = Field(max_length=1_000_000)
    source_format: Literal["JSON", "XML"] = "JSON"
    target_format: Literal["JSON", "XML"] = "JSON"
    mapping: dict[str, str] = Field(default_factory=dict)


@app.head("/api/health")
@app.get("/api/health")
def health():
    return {"status": "ready", "live_transfers": True, "as2_mdn": ["synchronous", "asynchronous"]}


@app.get("/api/connections")
def connections():
    with database() as db:
        records = [dict(zip(["id", "name", "protocol", "endpoint", "config", "connection_test"], row)) for row in db.execute("SELECT id,name,protocol,endpoint,config,connection_test FROM connections")]
        for record in records:
            record["config"] = json.loads(record["config"])
            record["connection_test"] = json.loads(record["connection_test"]) if record["connection_test"] else None
        return records


def check_connection(connection: Connection):
    """Test the exact candidate settings. Never send documents or consume messages."""
    try:
        result=operate(connection.model_dump(),"test")
        if not isinstance(result,dict) or result.get("reachable") is False or result.get("connected") is False:
            raise ConnectorError("The endpoint did not report a successful connection")
        if "http_status" in result and not 200<=result["http_status"]<300:
            raise ConnectorError("The endpoint did not return a successful HTTP response")
        allowed=("http_status","detail","role","topic","partitions","destination")
        return {"success":True,"tested_at":datetime.now(timezone.utc).isoformat(),
            "detail":{key:result[key] for key in allowed if key in result}}
    except Exception as exc:
        reason=str(exc) if isinstance(exc,ConnectorError) else f"{type(exc).__name__}: verify endpoint, credentials, permissions and network access"
        raise HTTPException(422,"Connection test failed. Nothing was saved. "+reason) from exc


@app.post("/api/connections/test")
def test_candidate_connection(connection: Connection):
    return check_connection(connection)


@app.post("/api/connections", status_code=201)
def create_connection(connection: Connection):
    result=check_connection(connection)
    record = {"id": str(uuid4()), **connection.model_dump(),"connection_test":result}
    with database() as db:
        db.execute("INSERT INTO connections (id,name,protocol,endpoint,config,connection_test) VALUES (?, ?, ?, ?, ?, ?)", (record["id"], record["name"], record["protocol"], record["endpoint"], json.dumps(record["config"]),json.dumps(result)))
    return record


def get_connection(connection_id):
    record = next((c for c in connections() if c["id"] == connection_id), None)
    if not record:
        raise HTTPException(404, "Connection not found")
    return record


def get_runtime_connection(connection_id):
    from .connection_properties import resolve_connection
    try:
        return resolve_connection(get_connection(connection_id))
    except ConnectorError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.put("/api/connections/{connection_id}")
def update_connection(connection_id: str, connection: Connection):
    get_connection(connection_id)
    result=check_connection(connection)
    with database() as db:
        db.execute("UPDATE connections SET name=?,protocol=?,endpoint=?,config=?,connection_test=? WHERE id=?", (connection.name, connection.protocol, connection.endpoint, json.dumps(connection.config),json.dumps(result), connection_id))
    return get_connection(connection_id)


class TransferRequest(BaseModel):
    operation: Literal["test", "list", "send", "receive"]
    path: str = Field(default="", max_length=500)
    content_base64: str = Field(default="", max_length=14_000_000)
    content_type: str = Field(default="application/octet-stream", pattern=r"^[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+$")


def record_transfer(connection_id, operation, path, status, detail, payload=None, message_id=None, transaction=None):
    transfer_id = str(uuid4())
    with nullcontext(transaction) if transaction is not None else database() as db:
        db.execute("INSERT INTO transfers VALUES (?,?,?,?,?,?,?,?,?)", (transfer_id, connection_id, operation, path, status, datetime.now(timezone.utc).isoformat(), json.dumps(detail), payload, message_id))
    return transfer_id


@app.post("/api/connections/{connection_id}/operate")
def connection_operation(connection_id: str, request: TransferRequest):
    connection = get_connection(connection_id)
    try:
        data = base64.b64decode(request.content_base64, validate=True) if request.operation == "send" else None
        if data is not None and len(data) > MAX_BYTES:
            raise ConnectorError("File exceeds the 10 MiB transfer limit")
        result = operate(connection, request.operation, request.path, data, request.content_type)
        payload = result if isinstance(result, bytes) else None
        detail = {"bytes": len(result), "path": request.path} if payload is not None else result
        status = "awaiting_mdn" if detail.get("delivery_status") == "awaiting_mdn" else "success"
        transfer_id = record_transfer(connection_id, request.operation, request.path, status, detail, payload)
        return {"id": transfer_id, **detail, **({"download_url": f"/api/transfers/{transfer_id}/download"} if payload is not None else {})}
    except Exception as exc:
        # Raw exception strings can contain credentials or payloads. Store a safe category.
        detail = str(exc) if isinstance(exc, ConnectorError) else f"{type(exc).__name__}: operation failed; verify endpoint, credentials and network access"
        record_transfer(connection_id, request.operation, request.path, "failed", {"error": detail})
        raise HTTPException(400 if isinstance(exc, ValueError) else 502, detail) from exc


@app.get("/api/transfers")
def transfers():
    with database() as db:
        records = [dict(zip(["id", "connection_id", "operation", "path", "status", "created", "detail"], row)) for row in db.execute("SELECT id,connection_id,operation,path,status,created,detail FROM transfers ORDER BY created DESC LIMIT 100")]
    for record in records:
        record["detail"] = json.loads(record["detail"])
    return records


@app.get("/api/transfers/{transfer_id}/download")
def download_transfer(transfer_id: str):
    with database() as db:
        row = db.execute("SELECT payload FROM transfers WHERE id=?", (transfer_id,)).fetchone()
    if row is None or row[0] is None:
        raise HTTPException(404, "Received file not found")
    return Response(row[0], media_type="application/octet-stream", headers={"Content-Disposition": 'attachment; filename="received-document.bin"'})


def inbound_transport(request, options):
    from .protocols import secret
    if options.require_https and request.url.scheme != "https":
        raise HTTPException(403, "This AS2 endpoint requires HTTPS")
    if options.inbound_username:
        try:
            scheme, encoded = request.headers.get("authorization","").split(" ",1)
            supplied = base64.b64decode(encoded,validate=True)
        except Exception:
            supplied, scheme = b"", ""
        expected = (options.inbound_username+":"+secret(options.inbound_password_env,True)).encode()
        if scheme.lower()!="basic" or not hmac.compare_digest(supplied,expected):
            raise HTTPException(401,"AS2 HTTP authentication required",headers={"WWW-Authenticate":"Basic"})


async def limited_body(request):
    body=bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body)>MAX_BYTES:
            raise HTTPException(413,"AS2 message exceeds 10 MiB")
    return bytes(body)


@app.head("/as2/{connection_id}")
def as2_reachable(connection_id: str, request: Request):
    connection=get_runtime_connection(connection_id)
    if connection["protocol"]!="AS2":
        raise HTTPException(400,"This endpoint is not AS2")
    inbound_transport(request,OPTION_TYPES["AS2"].model_validate(connection["config"]))
    return Response(status_code=200)


@app.post("/as2/{connection_id}")
async def receive_as2(connection_id: str, request: Request):
    connection=get_runtime_connection(connection_id)
    if connection["protocol"]!="AS2":
        raise HTTPException(400,"This endpoint is not AS2")
    options=OPTION_TYPES["AS2"].model_validate(connection["config"])
    inbound_transport(request,options)
    body=await limited_body(request)
    headers=dict(request.headers)
    message_id=headers.get("message-id","").strip("<>")
    if not message_id or not headers.get("as2-from") or not headers.get("as2-to"):
        raise HTTPException(400,"Message-ID, AS2-From and AS2-To headers are required")
    fingerprint=as2.request_hash(headers,body)
    def cached_receipt():
        with database() as db:
            return db.execute("SELECT request_hash,headers,body,callback_url FROM as2_receipts WHERE connection_id=? AND message_id=?",(connection_id,message_id)).fetchone()
    def replay_response(cached):
        return Response(status_code=202 if cached[3] else 200,content=b"" if cached[3] else cached[2],headers={} if cached[3] else json.loads(cached[1]))
    cached=await run_in_threadpool(cached_receipt)
    if cached:
        if not hmac.compare_digest(cached[0],fingerprint):
            raise HTTPException(409,"Message-ID was already used for a different AS2 document")
        return replay_response(cached)
    def duplicate(mid,partner_id):
        with database() as db:
            return db.execute("SELECT 1 FROM transfers WHERE connection_id=? AND message_id=?",(connection_id,mid)).fetchone() is not None
    try:
        message_id,data,mdn,filename,content_type=await run_in_threadpool(as2.receive,options,headers,body,duplicate)
        callback=headers.get("receipt-delivery-option") or None
        with database() as db:
            transfer_id=record_transfer(connection_id,"inbound",filename,"success",{"bytes":len(data),"message_id":message_id,"accepted_to_inbox":True},data,message_id,transaction=db)
            jobs=flows.stage_inbound(db,connection_id,transfer_id,filename,data,content_type)
            db.execute("UPDATE transfers SET detail=? WHERE id=?",(json.dumps({"bytes":len(data),"message_id":message_id,"accepted_to_inbox":True,"queued_jobs":jobs}),transfer_id))
            db.execute("INSERT INTO as2_receipts(id,connection_id,message_id,request_hash,headers,body,callback_url,status) VALUES (?,?,?,?,?,?,?,?)",
                (str(uuid4()),connection_id,message_id,fingerprint,json.dumps(mdn.headers if mdn else {}),mdn.content if mdn else b"",callback,"queued" if callback else "returned"))
        return Response(status_code=202 if callback else 200,content=b"" if callback else mdn.content if mdn else b"",headers={} if callback else mdn.headers if mdn else {})
    except sqlite3.IntegrityError as exc:
        cached=await run_in_threadpool(cached_receipt)
        if cached and hmac.compare_digest(cached[0],fingerprint):
            return replay_response(cached)
        raise HTTPException(409,"AS2 Message-ID conflicts with an accepted document") from exc
    except Exception as exc:
        record_transfer(connection_id,"inbound","AS2 document","failed",{"error":"AS2 message validation failed"})
        raise HTTPException(400,"AS2 message rejected; check identities, certificates and security requirements") from exc


@app.post("/as2/{connection_id}/mdn")
async def receive_async_mdn(connection_id: str, request: Request):
    connection=get_runtime_connection(connection_id)
    if connection["protocol"]!="AS2":
        raise HTTPException(400,"This endpoint is not AS2")
    inbound_transport(request,OPTION_TYPES["AS2"].model_validate(connection["config"]))
    body=await limited_body(request)
    try:
        return await run_in_threadpool(as2.accept_async_mdn,connection_id,dict(request.headers),body)
    except ConnectorError as exc:
        raise HTTPException(400,str(exc)) from exc


@app.get("/api/as2/messages")
def as2_messages():
    with database() as db:
        outbound=[dict(zip(["message_id","connection_id","filename","bytes","created","status","detail"],row)) for row in db.execute("SELECT message_id,connection_id,filename,bytes,created,status,detail FROM as2_outbox ORDER BY created DESC LIMIT 100")]
        receipts=[dict(zip(["id","connection_id","message_id","callback_url","status","attempts"],row)) for row in db.execute("SELECT id,connection_id,message_id,callback_url,status,attempts FROM as2_receipts ORDER BY rowid DESC LIMIT 100")]
    return {"outbound":outbound,"receipts":receipts}


@app.get("/api/as2/messages/{message_id}/receipt")
def download_receipt(message_id: str):
    with database() as db:
        row=db.execute("SELECT receipt_headers,receipt_body FROM as2_outbox WHERE message_id=?",(message_id,)).fetchone()
    if not row or row[1] is None:
        raise HTTPException(404,"No verified receipt is available")
    return Response(as2.raw_message(json.loads(row[0]),row[1]),media_type="message/rfc822",headers={"Content-Disposition":'attachment; filename="verified-mdn.eml"'})


@app.get("/api/connections/{connection_id}/as2-info")
def as2_connection_info(connection_id: str, request: Request):
    connection=get_runtime_connection(connection_id)
    if connection["protocol"]!="AS2":
        raise HTTPException(400,"This endpoint is not AS2")
    try:
        return as2.public_info(connection,str(request.base_url))
    except ConnectorError as exc:
        raise HTTPException(400,str(exc)) from exc


@app.post("/api/as2/receipts/{receipt_id}/retry")
def retry_as2_receipt(receipt_id: str):
    with database() as db:
        cursor=db.execute("UPDATE as2_receipts SET status='queued',attempts=0,next_attempt=0 WHERE id=? AND status='failed'",(receipt_id,))
        if not cursor.rowcount:
            raise HTTPException(400,"Only failed MDN callbacks can be retried")
    return {"status":"queued"}


@app.get("/api/flows")
def get_flows():
    return flows.list_flows()


@app.post("/api/flows", status_code=201)
def add_flow(flow: flows.Flow):
    try:
        return flows.create_flow(flow)
    except ConnectorError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.put("/api/flows/{flow_id}")
def edit_flow(flow_id: str, flow: flows.Flow):
    try:
        return flows.update_flow(flow_id, flow)
    except ConnectorError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/flows/{flow_id}/run")
def run_flow(flow_id: str):
    try:
        return flows.poll_flow(flow_id, force=True)
    except Exception as exc:
        raise HTTPException(400, flows.safe_error(exc)) from exc


@app.get("/api/jobs")
def get_jobs():
    return flows.list_jobs()


@app.post("/api/jobs/{job_id}/retry")
def retry_job(job_id: str):
    try:
        return flows.retry_job(job_id)
    except ConnectorError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/transform")
def transform(request: Transformation):
    try:
        if request.source_format == "JSON":
            data = json.loads(request.payload)
        else:
            root = ET.fromstring(request.payload)
            data = {child.tag: child.text or "" for child in root}
        if not isinstance(data, dict):
            raise ValueError("Payload must contain an object")
        result = {target: data[source] for target, source in request.mapping.items()} if request.mapping else data
        if request.target_format == "JSON":
            output = json.dumps(result, indent=2)
        else:
            root = ET.Element("document")
            for key, value in result.items():
                if not key or not key.replace("_", "a").replace("-", "a").isalnum() or not (key[0].isalpha() or key[0] == "_"):
                    raise ValueError(f"Invalid XML field name: {key}")
                ET.SubElement(root, key).text = str(value)
            ET.indent(root)
            output = ET.tostring(root, encoding="unicode")
    except (ValueError, KeyError, ET.ParseError) as exc:
        raise HTTPException(400, f"Transformation failed: {exc}") from exc
    record = {"id": str(uuid4()), "created": datetime.now(timezone.utc).isoformat(), "output": output}
    with database() as db:
        db.execute("INSERT INTO runs VALUES (?, ?, ?)", tuple(record.values()))
    return record


@app.get("/api/runs")
def runs():
    with database() as db:
        return [dict(zip(["id", "created", "output"], row)) for row in db.execute("SELECT * FROM runs ORDER BY created DESC LIMIT 50")]


def connector_protocol(connection_id, protocol):
    connection=get_runtime_connection(connection_id)
    if connection["protocol"] not in protocol:
        raise HTTPException(400,"This connection does not support the requested action")
    return connection


def http_inbound_options(connection_id, request):
    connection=connector_protocol(connection_id,("HTTP","HTTPS"))
    options=OPTION_TYPES[connection["protocol"]].model_validate(connection["config"])
    if not options.enable_inbound:
        raise HTTPException(404,"HTTP inbox is disabled")
    if (options.require_https or connection["protocol"]=="HTTPS") and request.url.scheme!="https":
        raise HTTPException(403,"This connector requires HTTPS")
    from .protocols import secret
    expected="Bearer "+secret(options.inbound_token_env,True)
    if not hmac.compare_digest(request.headers.get("authorization","").encode(),expected.encode()):
        raise HTTPException(401,"Invalid connector bearer token")
    return options


@app.api_route("/http/{connection_id}",methods=["GET","HEAD"])
@app.api_route("/http/{connection_id}/{path:path}",methods=["PUT","POST","GET","HEAD"])
async def http_inbox(connection_id: str, request: Request, path: str=""):
    from . import inbox
    from .protocols import relative_path
    options=http_inbound_options(connection_id,request)
    try:
        relative_path(path,allow_empty=request.method in ("HEAD","GET"))
    except ConnectorError as exc:
        raise HTTPException(400,str(exc)) from exc
    if request.method=="HEAD":
        return Response(status_code=200)
    if request.method=="GET":
        if request.query_params.get("list")=="1" or not path:
            return await run_in_threadpool(inbox.listing,connection_id,path,options.max_entries)
        row=await run_in_threadpool(inbox.read,connection_id,path)
        if not row:
            raise HTTPException(404,"HTTP document not found")
        return Response(row[0],media_type=row[1])
    payload=await limited_body(request)
    if not path:
        raise HTTPException(400,"A relative document path is required")
    try:
        result=await run_in_threadpool(inbox.accept,connection_id,str(uuid4()),path,payload,
            "application/octet-stream",True)
        return Response(json.dumps(result),status_code=201,media_type="application/json")
    except FileExistsError as exc:
        raise HTTPException(412,"HTTP document path already exists") from exc
    except ConnectorError as exc:
        raise HTTPException(400,str(exc)) from exc


class DirectoryCredentials(BaseModel):
    username: str = Field(min_length=1,max_length=256)
    password: SecretStr = Field(min_length=1,max_length=2048)


@app.get("/api/connections/{connection_id}/users")
def directory_users(connection_id: str):
    from .directory import lookup
    connection=connector_protocol(connection_id,("LDAP",))
    try:
        return lookup(connection,OPTION_TYPES["LDAP"].model_validate(connection["config"]))
    except Exception as exc:
        raise HTTPException(502,"LDAP search failed; verify directory configuration") from exc


@app.post("/api/connections/{connection_id}/authenticate")
def directory_authenticate(connection_id: str, credentials: DirectoryCredentials):
    from .directory import authenticate
    connection=connector_protocol(connection_id,("LDAP",))
    try:
        return authenticate(connection,OPTION_TYPES["LDAP"].model_validate(connection["config"]),
            credentials.username,credentials.password.get_secret_value())
    except Exception as exc:
        raise HTTPException(401,"Directory authentication failed") from exc


@app.post("/api/connections/{connection_id}/consume")
def consume_kafka(connection_id: str):
    from .kafka import poll_receiver
    connection=connector_protocol(connection_id,("KAFKA",))
    if not connection["config"]["enable_receiver"]:
        raise HTTPException(400,"Enable the Kafka receiver first")
    try:
        return {"received":poll_receiver(connection)}
    except Exception as exc:
        raise HTTPException(502,"Event stream receiver failed; offsets remain uncommitted for unaccepted documents") from exc


@app.get("/api/connectors/inbox")
def connector_documents():
    with database() as db:
        rows=db.execute("SELECT id,connection_id,path,length(payload),created FROM documents ORDER BY created DESC LIMIT 100").fetchall()
        states=db.execute("SELECT connection_id,last_poll,received,error FROM receivers").fetchall()
    return {"documents":[dict(zip(["id","connection_id","path","bytes","created"],row)) for row in rows],
            "receivers":[dict(zip(["connection_id","last_poll","received","error"],row)) for row in states]}


@app.get("/api/connectors/inbox/{document_id}")
def connector_download(document_id: str):
    with database() as db:
        row=db.execute("SELECT payload FROM documents WHERE id=?",(document_id,)).fetchone()
    if not row:
        raise HTTPException(404,"Inbox document not found")
    return Response(row[0],media_type="application/octet-stream",
        headers={"Content-Disposition":'attachment; filename="connector-document.bin"'})


@app.get("/api/connector-catalog")
def connector_catalog():
    return json.loads(Path(__file__).with_name("connector_catalog.json").read_text(encoding="utf-8"))


DOCUMENTATION = {
    "user-guide": ("User guide", "docs/USER_GUIDE.md"),
    "connectors": ("Connector reference", "CONNECTORS.md"),
    "protocols": ("Protocol setup", "PROTOCOLS.md"),
    "data-flows": ("File routing", "DATA_FLOWS.md"),
    "api": ("API reference", "docs/API_REFERENCE.md"),
    "operations": ("Operations", "docs/OPERATIONS.md"),
    "samples": ("Runnable samples", "samples/README.md"),
    "connector-examples": ("Connector examples", "samples/CONNECTORS.md"),
}


@app.get("/api/documentation")
def documentation_index():
    return [{"slug": slug, "title": title} for slug, (title, _) in DOCUMENTATION.items()]


@app.get("/api/documentation/{slug}")
def documentation_page(slug: str):
    if slug not in DOCUMENTATION:
        raise HTTPException(404, "Guide not found")
    title, relative = DOCUMENTATION[slug]
    path = Path(__file__).resolve().parent.parent / relative
    return {"slug": slug, "title": title, "content": path.read_text(encoding="utf-8")}
