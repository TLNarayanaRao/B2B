"""Document-library transfers using app credentials."""
from datetime import datetime
from urllib.parse import urlsplit, quote

import httpx
from .protocols import MAX_BYTES, ConnectorError, secret

GRAPH="https://graph.microsoft.com/v1.0"


def encoded(value):
    return quote(value,safe="")


def trusted_download(url):
    parsed=urlsplit(url)
    if parsed.scheme!="https" or parsed.username or parsed.password or not parsed.hostname or parsed.port not in (None,443):
        raise ConnectorError("Document library returned an invalid transfer URL")
    if not (parsed.hostname.endswith(".sharepoint.com") or parsed.hostname.endswith(".sharepoint-df.com") or parsed.hostname.endswith(".1drv.com")):
        raise ConnectorError("Document library transfer host is outside approved cloud domains")
    return url


class Graph:
    def __init__(self,connection,options):
        self.connection,self.options=connection,options
        self.client=httpx.Client(timeout=options.timeout,trust_env=False,follow_redirects=False)
        self.authorization={}
        self.drive=None

    def __enter__(self):
        try:
            if not self.options.tenant_id or not self.options.application_id:
                raise ConnectorError("Document library Tenant ID and Application ID are required")
            result=self.client.post(f"https://login.microsoftonline.com/{encoded(self.options.tenant_id)}/oauth2/v2.0/token",
                data={"client_id":self.options.application_id,
                    "client_secret":secret(self.options.application_secret_env,True),
                    "scope":"https://graph.microsoft.com/.default","grant_type":"client_credentials"})
            result.raise_for_status()
            self.authorization={"Authorization":"Bearer "+result.json()["access_token"]}
            return self
        except Exception:
            self.client.close()
            raise

    def __exit__(self,*args):
        self.client.close()

    def request(self,method,path,**kwargs):
        return self.client.request(method,GRAPH+path,headers=self.authorization,**kwargs)

    def json(self,path):
        response=self.request("GET",path);response.raise_for_status()
        return response.json()

    def pages(self,path):
        seen=set()
        url=GRAPH+path
        while url:
            if url in seen or not url.startswith(GRAPH+"/"):
                raise ConnectorError("Document library returned an invalid pagination URL")
            seen.add(url)
            response=self.client.get(url,headers=self.authorization);response.raise_for_status()
            data=response.json()
            yield from data.get("value",[])
            url=data.get("@odata.nextLink")
            if len(seen)>100:
                raise ConnectorError("Document library enumeration exceeds page limit")

    def resolve_drive(self):
        parsed=urlsplit(self.connection["endpoint"])
        site_path=parsed.path.rstrip("/")
        site=self.json("/sites/"+encoded(parsed.hostname)+( ":"+quote(site_path,safe="/") if site_path else ""))
        for drive in self.pages("/sites/"+encoded(site["id"])+"/drives"):
            if drive["name"]==self.options.drive_name:
                self.drive=drive["id"];return self.drive
        raise ConnectorError("Document library document library was not found; verify Drive name and application permissions")

    def item_path(self,path):
        full="/".join(p for p in (self.options.root_path,path) if p)
        root="/drives/"+encoded(self.drive)+"/root"
        return root+(":/"+quote(full,safe="/")+":" if full else "")


def operate_sharepoint(connection,options,operation,path,data,content_type):
    with Graph(connection,options) as graph:
        graph.resolve_drive()
        item_path=graph.item_path(path)
        if operation=="test":
            graph.json(item_path)
            return {"reachable":True,"drive":options.drive_name}
        if operation=="list":
            rows=[]
            for item in graph.pages(item_path+"/children"):
                if len(rows)>=options.max_entries+1:
                    break
                rows.append({"name":item["name"],"size":item.get("size",0),"directory":"folder" in item,
                    "modified":datetime.fromisoformat(item["lastModifiedDateTime"].replace("Z","+00:00")).timestamp() if item.get("lastModifiedDateTime") else None})
            return {"entries":rows[:options.max_entries],"truncated":len(rows)>options.max_entries}
        if operation=="receive":
            info=graph.json(item_path)
            if "folder" in info:
                raise ConnectorError("Document library source is a folder")
            if info.get("size",MAX_BYTES+1)>MAX_BYTES:
                raise ConnectorError("Document library file exceeds transfer limit")
            response=graph.client.get(GRAPH+item_path+"/content",headers={**graph.authorization,"If-Match":info["eTag"]})
            if response.status_code==302:
                url=trusted_download(response.headers["location"])
                with graph.client.stream("GET",url) as download:
                    download.raise_for_status()
                    result=bytearray()
                    for chunk in download.iter_bytes():
                        if len(result)+len(chunk)>MAX_BYTES:
                            raise ConnectorError("Document library download exceeds transfer limit")
                        result.extend(chunk)
                    return bytes(result)
            response.raise_for_status()
            if len(response.content)>MAX_BYTES:
                raise ConnectorError("Document library download exceeds transfer limit")
            return response.content
        # Upload sessions explicitly support fail-on-conflict; plain PUT otherwise
        # replaces existing files by default. Chunks must be multiples of 320 KiB.
        if not data:
            raise ConnectorError("Document library upload-session adapter requires a nonempty document")
        response=graph.request("POST",item_path+"/createUploadSession",
            json={"item":{"@microsoft.graph.conflictBehavior":"fail","name":path.split("/")[-1]}})
        if response.status_code==409:
            raise FileExistsError("Document library destination already exists")
        response.raise_for_status()
        upload_url=trusted_download(response.json()["uploadUrl"])
        chunk_size=320*1024*10
        try:
            final=None
            for offset in range(0,len(data),chunk_size):
                chunk=data[offset:offset+chunk_size]
                final=graph.client.put(upload_url,content=chunk,headers={
                    "Content-Length":str(len(chunk)),
                    "Content-Range":f"bytes {offset}-{offset+len(chunk)-1}/{len(data)}"})
                if final.status_code==409:
                    raise FileExistsError("Document library destination already exists")
                final.raise_for_status()
                if offset+len(chunk)<len(data) and final.status_code!=202:
                    raise ConnectorError("Document library upload completed before all document bytes were sent")
            if final.status_code not in (200,201):
                raise ConnectorError("Document library upload was not confirmed; delivery is uncertain")
            return {"bytes":len(data),"path":path,"item_id":final.json().get("id")}
        except Exception:
            try:
                graph.client.delete(upload_url)
            except Exception:
                pass
            raise
