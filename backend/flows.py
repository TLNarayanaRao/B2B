"""Durable store-and-forward flows with content-based polling deduplication."""
import fnmatch
import hashlib
import json
import mimetypes
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from .protocols import ConnectorError, operate, relative_path

STOP = threading.Event()
WORKER = None
POLL_LOCK = threading.Lock()


class Flow(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    source_id: str
    destination_id: str
    source_path: str = Field(default="", max_length=400)
    file_pattern: str = Field(default="*", min_length=1, max_length=100)
    destination_pattern: str = Field(default="{filename}", min_length=1, max_length=400)
    interval_seconds: int = Field(default=30, ge=5, le=86400)
    max_files: int = Field(default=20, ge=1, le=100)
    min_age_seconds: int = Field(default=5, ge=0, le=86400)
    enabled: bool = True

    @model_validator(mode="after")
    def paths(self):
        if self.source_id == self.destination_id:
            raise ValueError("Source and destination must be different endpoints")
        relative_path(self.source_path, allow_empty=True)
        if "/" in self.file_pattern or "\\" in self.file_pattern:
            raise ValueError("File pattern matches filenames in the selected directory")
        sample = self.destination_pattern.replace("{filename}", "document.txt").replace("{job_id}", "job")
        if "{" in sample or "}" in sample:
            raise ValueError("Destination supports only {filename} and {job_id} placeholders")
        relative_path(sample)
        return self


def schema(db):
    db.execute("CREATE TABLE IF NOT EXISTS flows (id TEXT PRIMARY KEY, config TEXT, last_poll REAL NOT NULL DEFAULT 0, last_error TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, flow_id TEXT, dedup_key TEXT UNIQUE, source_path TEXT, destination_path TEXT, status TEXT, created TEXT, updated TEXT, detail TEXT, payload BLOB, content_type TEXT)")
    db.execute("CREATE INDEX IF NOT EXISTS job_queue ON jobs(status,created)")
    if "destination_id" not in [row[1] for row in db.execute("PRAGMA table_info(jobs)")]:
        db.execute("ALTER TABLE jobs ADD COLUMN destination_id TEXT")


def now():
    return datetime.now(timezone.utc).isoformat()


def safe_error(exc):
    return str(exc) if isinstance(exc, ConnectorError) else type(exc).__name__ + ": transfer failed; verify configuration and network access"


def list_flows():
    from .main import database
    with database() as db:
        return [{"id": row[0], **json.loads(row[1]), "last_poll": row[2], "last_error": row[3]} for row in db.execute("SELECT id,config,last_poll,last_error FROM flows")]


def validate_flow(flow):
    from .main import get_connection
    source = get_connection(flow.source_id)
    destination=get_connection(flow.destination_id)
    if "LDAP" in (source["protocol"],destination["protocol"]):
        raise ConnectorError("LDAP authenticates users; it cannot be a file-flow endpoint")
    if source["protocol"]=="KAFKA" and not source["config"].get("enable_receiver"):
        raise ConnectorError("Enable the Kafka receiver before using it as a source")
    if source["protocol"] == "EMS":
        raise ConnectorError("EMS is currently an outbound JMS publisher; choose a file endpoint or AS2 as the source")


def create_flow(flow):
    from .main import database
    validate_flow(flow)
    flow_id = str(uuid4())
    with database() as db:
        db.execute("INSERT INTO flows(id,config) VALUES (?,?)", (flow_id, json.dumps(flow.model_dump())))
    return {"id": flow_id, **flow.model_dump()}


def update_flow(flow_id, flow):
    from .main import database
    validate_flow(flow)
    with database() as db:
        cursor = db.execute("UPDATE flows SET config=?,last_poll=0,last_error=NULL WHERE id=?", (json.dumps(flow.model_dump()), flow_id))
        if cursor.rowcount == 0:
            raise ConnectorError("Data flow not found")
    return {"id": flow_id, **flow.model_dump()}


def filename_only(filename):
    # AS2 filenames can contain paths. Keep only the final segment.
    name = (filename or "document.bin").replace("\\", "/").split("/")[-1]
    try:
        relative_path(name)
    except ConnectorError:
        name = "document.bin"
    return name or "document.bin"


def stage(db, flow, source_path, filename, payload, content_type, source_key):
    job_id, timestamp = str(uuid4()), now()
    name = filename_only(filename)
    if not fnmatch.fnmatchcase(name, flow["file_pattern"]):
        return None
    destination = flow["destination_pattern"].replace("{filename}", name).replace("{job_id}", job_id)
    relative_path(destination)
    key = hashlib.sha256((flow["id"] + ":" + source_key).encode()).hexdigest()
    cursor = db.execute("INSERT OR IGNORE INTO jobs (id,flow_id,dedup_key,source_path,destination_path,status,created,updated,detail,payload,content_type,destination_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (job_id, flow["id"], key, source_path, destination, "queued", timestamp, timestamp, "{}", payload, content_type, flow["destination_id"]))
    return job_id if cursor.rowcount else None


def stage_inbound(db, connection_id, transfer_id, filename, payload, content_type):
    jobs = []
    for row in db.execute("SELECT id,config FROM flows"):
        flow = {"id": row[0], **json.loads(row[1])}
        if flow["enabled"] and flow["source_id"] == connection_id:
            job_id = stage(db, flow, filename, filename, payload, content_type, "inbound:" + transfer_id)
            if job_id:
                jobs.append(job_id)
    return jobs


def poll_flow(flow_id, force=False):
    from .main import database, get_connection
    with POLL_LOCK:
        flow = next((f for f in list_flows() if f["id"] == flow_id), None)
        if not flow:
            raise ConnectorError("Data flow not found")
        if not flow["enabled"]:
            raise ConnectorError("Enable the data flow before running it")
        source = get_connection(flow["source_id"])
        if source["protocol"] == "KAFKA":
            from .kafka import poll_receiver
            return {"queued":poll_receiver(source),"detail":"Kafka messages are durably received once and routed to every matching flow"}
        if source["protocol"] in ("HTTP","HTTPS") and source["config"].get("inbound_only"):
            return {"queued":0,"detail":"Waiting for inbound HTTP document uploads"}
        if source["protocol"] == "AS2":
            return {"queued": 0, "detail": "Waiting for partner POSTs to /as2/" + source["id"]}
        if not force and time.time() - flow["last_poll"] < flow["interval_seconds"]:
            return {"queued": 0}
        with database() as db:
            db.execute("UPDATE flows SET last_poll=?,last_error=NULL WHERE id=?", (time.time(), flow_id))
        try:
            entries = operate(source, "list", flow["source_path"])
            selected = [e for e in entries["entries"] if not e["directory"] and fnmatch.fnmatchcase(e["name"].split("/")[-1], flow["file_pattern"])]
            count = 0
            for entry in selected:
                if count >= flow["max_files"]:
                    break
                name = entry["name"]
                if entry.get("modified") is not None and time.time() - entry["modified"] < flow.get("min_age_seconds", 5):
                    continue
                path = name if source["protocol"] == "GCS" else "/".join(p for p in [flow["source_path"], name] if p)
                payload = operate(source, "receive", path)
                digest = hashlib.sha256(payload).hexdigest()
                with database() as db:
                    job_id = stage(db, flow, path, name, payload, mimetypes.guess_type(name)[0] or "application/octet-stream", path + ":" + digest)
                if job_id:
                    count += 1
            if entries.get("truncated"):
                with database() as db:
                    db.execute("UPDATE flows SET last_error=? WHERE id=?", ("Directory listing was truncated. Split sources into smaller directories or increase the entry limit.", flow_id))
            return {"queued": count, "truncated": entries.get("truncated", False)}
        except Exception as exc:
            with database() as db:
                db.execute("UPDATE flows SET last_error=? WHERE id=?", (safe_error(exc), flow_id))
            raise


def list_jobs():
    from .main import database
    with database() as db:
        rows = db.execute("SELECT id,flow_id,source_path,destination_path,status,created,updated,detail FROM jobs ORDER BY created DESC LIMIT 100").fetchall()
    items = [dict(zip(["id","flow_id","source_path","destination_path","status","created","updated","detail"], row)) for row in rows]
    for item in items:
        item["detail"] = json.loads(item["detail"])
    return items


def process_next_job():
    from .main import database, get_connection, record_transfer
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT id,flow_id,destination_path,payload,content_type,destination_id FROM jobs WHERE status='queued' ORDER BY created LIMIT 1").fetchone()
        if not row:
            return False
        flow_row = db.execute("SELECT config FROM flows WHERE id=?", (row[1],)).fetchone()
        if not flow_row or not json.loads(flow_row[0])["enabled"]:
            db.execute("UPDATE jobs SET status='held',updated=? WHERE id=?", (now(), row[0]))
            return True
        flow = json.loads(flow_row[0])
        db.execute("UPDATE jobs SET status='processing',updated=? WHERE id=?", (now(), row[0]))
    job_id, _, destination_path, payload, content_type, destination_id = row
    destination = None
    try:
        destination = get_connection(destination_id or flow["destination_id"])
        result = operate(destination, "send", destination_path, payload, content_type)
        detail = {"job_id": job_id, **result}
        status = "awaiting_mdn" if result.get("delivery_status")=="awaiting_mdn" else "success"
    except Exception as exc:
        # A timeout may happen after a remote peer accepted a file. Never auto-retry.
        status = "failed" if isinstance(exc, (FileExistsError, ConnectorError)) and "uncertain" not in str(exc).lower() and "timed out" not in str(exc).lower() else "uncertain"
        detail = {"job_id": job_id, "error": safe_error(exc)}
    with database() as db:
        db.execute("UPDATE jobs SET status=?,updated=?,detail=? WHERE id=?", (status, now(), json.dumps(detail), job_id))
    if destination:
        record_transfer(destination["id"], "flow_send", destination_path, status, detail)
    return True


def retry_job(job_id):
    from .main import database
    with database() as db:
        cursor = db.execute("UPDATE jobs SET status='queued',updated=? WHERE id=? AND status IN ('failed','uncertain','held')", (now(), job_id))
        if not cursor.rowcount:
            raise ConnectorError("Only failed, uncertain or held jobs can be retried")
    return {"id": job_id, "status": "queued"}


def worker():
    while not STOP.is_set():
        try:
            from . import as2
            as2.process_next_receipt()
            as2.reconcile_jobs()
            poll_connectors()
            for flow in list_flows():
                if STOP.is_set():
                    break
                if flow["enabled"]:
                    try:
                        poll_flow(flow["id"])
                    except Exception:
                        pass  # Sanitized error is persisted on the flow.
            process_next_job()
        except Exception:
            import logging
            logging.getLogger(__name__).exception("Data-flow worker failed")
        STOP.wait(1)
    from .kafka import close_receivers
    close_receivers()


def start_worker():
    from .main import database
    global WORKER
    if WORKER and WORKER.is_alive():
        return
    STOP.clear()
    with database() as db:
        db.execute("UPDATE jobs SET status='uncertain',detail=?,updated=? WHERE status='processing'", (json.dumps({"error":"Server stopped during delivery; reconcile destination before retrying"}), now()))
        db.execute("UPDATE as2_receipts SET status='queued' WHERE status='sending'")
        db.execute("UPDATE as2_outbox SET status='uncertain',detail='Server stopped during send; reconcile with partner' WHERE status='sending'")
    WORKER = threading.Thread(target=worker, name="relay-flow-worker", daemon=True)
    WORKER.start()


def stop_worker():
    STOP.set()
    if WORKER:
        WORKER.join(timeout=5)


def poll_connectors():
    from .main import connections,database
    from .kafka import poll_receiver,close_receivers
    active=[connection for connection in connections() if connection["protocol"]=="KAFKA" and connection["config"].get("enable_receiver")]
    close_receivers({connection["id"] for connection in active})
    for connection in active:
        with database() as db:
            previous=db.execute("SELECT last_poll FROM receivers WHERE connection_id=?",(connection["id"],)).fetchone()
        if previous and time.time()-previous[0]<connection["config"].get("poll_interval_ms",1000)/1000:
            continue
        received,error=0,None
        try:
            received=poll_receiver(connection)
        except Exception as exc:
            error=safe_error(exc)
        with database() as db:
            db.execute("INSERT INTO receivers VALUES (?,?,?,?) ON CONFLICT(connection_id) DO UPDATE SET last_poll=excluded.last_poll,received=receivers.received+excluded.received,error=excluded.error",
                (connection["id"],time.time(),received,error))
