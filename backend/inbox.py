"""Transactional inbound store shared by HTTP and Kafka."""
import hashlib
import json
import time
from uuid import uuid4

from .protocols import MAX_BYTES, ConnectorError, relative_path


def schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS documents (
        id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, source_key TEXT NOT NULL,
        path TEXT NOT NULL, digest TEXT NOT NULL, payload BLOB NOT NULL,
        content_type TEXT NOT NULL, created REAL NOT NULL,
        UNIQUE(connection_id,source_key))""")
    db.execute("CREATE INDEX IF NOT EXISTS document_paths ON documents(connection_id,path)")


def accept(connection_id, source_key, path, payload, content_type="application/octet-stream", exclusive_path=False):
    from .main import database, record_transfer
    from .flows import stage_inbound
    relative_path(path)
    if len(payload)>MAX_BYTES:
        raise ConnectorError("Inbound document exceeds the 10 MiB limit")
    digest=hashlib.sha256(payload).hexdigest()
    with database() as db:
        db.execute("BEGIN IMMEDIATE")
        previous=db.execute("SELECT id,digest FROM documents WHERE connection_id=? AND source_key=?",(connection_id,source_key)).fetchone()
        if previous:
            if previous[1]!=digest:
                raise ConnectorError("Inbound identity was reused for different document content")
            return {"id":previous[0],"duplicate":True,"bytes":len(payload),"jobs":[]}
        if exclusive_path and db.execute("SELECT 1 FROM documents WHERE connection_id=? AND path=?",(connection_id,path)).fetchone():
            raise FileExistsError("Inbound document path already exists")
        document_id=str(uuid4())
        db.execute("INSERT INTO documents VALUES (?,?,?,?,?,?,?,?)",(document_id,connection_id,source_key,path,digest,payload,content_type,time.time()))
        transfer_id=record_transfer(connection_id,"inbound",path,"success",{"document_id":document_id,"bytes":len(payload)},payload=payload,transaction=db)
        jobs=stage_inbound(db,connection_id,transfer_id,path,payload,content_type)
    return {"id":document_id,"duplicate":False,"bytes":len(payload),"jobs":jobs}


def read(connection_id,path):
    from .main import database
    with database() as db:
        row=db.execute("SELECT payload,content_type FROM documents WHERE connection_id=? AND path=? ORDER BY created DESC LIMIT 1",(connection_id,path)).fetchone()
    return row


def listing(connection_id,path,limit):
    from .main import database
    prefix=path.rstrip("/")+"/" if path else ""
    with database() as db:
        # Prefix bounds do not interpret wildcard characters in filenames as SQL LIKE.
        rows=db.execute("SELECT path,length(payload),max(created) FROM documents WHERE connection_id=? AND substr(path,1,?)=? GROUP BY path ORDER BY path LIMIT ?",(connection_id,len(prefix),prefix,limit+1)).fetchall()
    entries=[]
    for name,size,created in rows:
        tail=name[len(prefix):]
        entries.append({"name":tail,"size":size,"directory":False,"modified":created})
    return {"entries":entries[:limit],"truncated":len(entries)>limit}
