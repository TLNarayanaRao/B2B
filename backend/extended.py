"""Standard transfer adapters. See CONNECTORS.md for capabilities and limitations."""
import io
import ftplib
import ssl
import socket
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import urlsplit, unquote, quote

import httpx
from .protocols import ConnectorError, MAX_BYTES, secret, relative_path, bounded_read


def tls_context(reference=""):
    context = ssl.create_default_context()
    if reference:
        context.load_verify_locations(cadata=secret(reference, True))
    return context


class ImplicitFTP(ftplib.FTP_TLS):
    """Wrap the command channel before reading the server greeting (implicit TLS)."""
    def connect(self, host="", port=990, timeout=-999, source_address=None):
        self.host, self.port = host, port
        if timeout != -999:
            self.timeout = timeout
        self.sock = socket.create_connection((host, port), self.timeout, source_address)
        self.af = self.sock.family
        self.sock = self.context.wrap_socket(self.sock, server_hostname=host)
        self.file = self.sock.makefile("r", encoding=self.encoding)
        self.welcome = self.getresp()
        return self.welcome


@contextmanager
def ftp_session(protocol, endpoint, options):
    parsed = urlsplit(endpoint)
    if not options.username:
        raise ConnectorError("FTP requires a configured username")
    if protocol == "FTPS":
        cls = ImplicitFTP if options.tls_mode == "implicit" else ftplib.FTP_TLS
        client = cls(context=tls_context(options.ca_certificate_env), timeout=options.timeout)
    else:
        client = ftplib.FTP(timeout=options.timeout)
    try:
        client.connect(parsed.hostname, parsed.port or (990 if protocol == "FTPS" and options.tls_mode == "implicit" else 21))
        client.login(options.username, secret(options.password_env, True))
        if protocol == "FTPS":
            client.prot_p()
        client.set_pasv(options.passive)
        client.cwd(unquote(parsed.path) or "/")
        yield client
    finally:
        client.close()


def ftp_operation(protocol, endpoint, options, operation, path, data):
    with ftp_session(protocol, endpoint, options) as client:
        if operation == "test":
            client.voidcmd("NOOP")
            return {"reachable": True}
        if operation == "list":
            rows = []
            # MLSD is standardized; avoid unreliable parsing of localized LIST output.
            for name, facts in client.mlsd(path, facts=["type", "size", "modify"]):
                if facts.get("type") in ("cdir", "pdir") or name in (".", ".."):
                    continue
                relative_path(name)
                if len(rows) > options.max_entries:
                    break
                modified = None
                if facts.get("modify"):
                    modified = datetime.strptime(facts["modify"].split(".")[0], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp()
                rows.append({"name":name, "directory":facts.get("type")=="dir", "size":int(facts.get("size",0)), "modified":modified})
            return {"entries":rows[:options.max_entries], "truncated":len(rows)>options.max_entries}
        if operation == "receive":
            result = bytearray()
            def collect(chunk):
                if len(result)+len(chunk)>MAX_BYTES:
                    raise ConnectorError("File exceeds the 10 MiB transfer limit")
                result.extend(chunk)
            client.retrbinary("RETR "+path, collect)
            return bytes(result)
        # FTP has no portable atomic create-if-absent. Do a preflight and disclose
        # this limitation; use a unique destination template for competing writers.
        parent, _, filename = path.rpartition("/")
        for name, _facts in client.mlsd(parent):
            if name == filename:
                raise FileExistsError("FTP destination already exists")
        client.storbinary("STOR "+path, io.BytesIO(data))
        return {"bytes":len(data), "path":path, "overwrite_guard":"preflight; use unique names for concurrent writers"}


def http_session(options):
    headers = {}
    if options.token_env:
        headers["Authorization"] = "Bearer "+secret(options.token_env, True)
    auth = (options.username,secret(options.password_env,True)) if options.username else None
    return httpx.Client(timeout=options.timeout, verify=tls_context(options.ca_certificate_env),
                        trust_env=False, follow_redirects=False, headers=headers, auth=auth)


def http_operation(endpoint, options, operation, path, data, content_type):
    url = endpoint.rstrip("/") + ("/"+quote(path,safe="/") if path else "")
    with http_session(options) as client:
        method = options.send_method if operation=="send" else "HEAD" if operation=="test" else "GET"
        headers={"Content-Type":content_type,"If-None-Match":"*"} if operation=="send" else {}
        params={"list":"1"} if operation=="list" else None
        with client.stream(method,url,headers=headers,params=params,content=data if operation=="send" else None) as response:
            if response.status_code in (409,412):
                raise FileExistsError("HTTP destination already exists")
            response.raise_for_status()
            if operation=="test":
                return {"reachable":True,"http_status":response.status_code}
            body=bytearray()
            for chunk in response.iter_bytes():
                if len(body)+len(chunk)>MAX_BYTES:
                    raise ConnectorError("HTTP response exceeds transfer limit")
                body.extend(chunk)
            if operation=="receive":
                return bytes(body)
            if operation=="list":
                import json
                from pydantic import BaseModel, Field
                class Entry(BaseModel):
                    name:str
                    directory:bool=False
                    size:int=Field(default=0,ge=0)
                    modified:float|None=None
                parsed=json.loads(body)
                rows=parsed["entries"]
                if not isinstance(rows,list):
                    raise ConnectorError("HTTP directory listing must contain entries")
                result=[]
                for row in rows[:options.max_entries+1]:
                    entry=Entry.model_validate(row).model_dump()
                    relative_path(entry["name"])
                    result.append(entry)
                return {"entries":result[:options.max_entries],"truncated":bool(parsed.get("truncated")) or len(result)>options.max_entries}
            return {"bytes":len(data),"path":path,"http_status":response.status_code}


def operate_extended(connection,options,operation,path,data,content_type):
    protocol=connection["protocol"];endpoint=connection["endpoint"]
    if protocol in ("FTP","FTPS"):
        return ftp_operation(protocol,endpoint,options,operation,path,data)
    if protocol in ("HTTP","HTTPS"):
        return http_operation(endpoint,options,operation,path,data,content_type)
    raise ConnectorError("Protocol adapter is unavailable")
