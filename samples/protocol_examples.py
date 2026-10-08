"""Configure an example endpoint and explicitly run file-transfer actions."""
import argparse
import base64
import json
from pathlib import Path
from uuid import uuid4
import httpx

ROOT=Path(__file__).resolve().parent


def call(api,path,body=None):
    response=httpx.request("POST" if body is not None else "GET",api.rstrip("/")+"/api/"+path,
                           json=body,timeout=120,trust_env=False)
    response.raise_for_status()
    return response.json()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol",choices=["AS2","SFTP","SMB","GCS","FILE","EMS","FTP","FTPS","HTTP","HTTPS","KAFKA","LDAP","SHAREPOINT"])
    parser.add_argument("--api",default="http://127.0.0.1:8000")
    parser.add_argument("--config",type=Path)
    parser.add_argument("--endpoint",help="Override the sample endpoint")
    parser.add_argument("--action",choices=["configure","test","list","send","receive","roundtrip","consume","users","authenticate"],default="configure")
    parser.add_argument("--path",default="")
    parser.add_argument("--input",type=Path,default=ROOT/"payloads"/"purchase-order.xml")
    parser.add_argument("--output",type=Path,help="Required download destination for receive/roundtrip")
    parser.add_argument("--username",help="LDAP authentication username")
    parser.add_argument("--password-env",help="LDAP authentication password environment variable (client-side)")
    args=parser.parse_args()
    if args.action=="roundtrip" and args.protocol in ("AS2","EMS","KAFKA","LDAP"):
        parser.error("AS2 uses its demo; Kafka consumes asynchronously; LDAP is a directory; EMS publishes only")
    if args.action in ("receive","roundtrip") and not args.output:
        parser.error("--output is required for downloads")
    if args.action=="receive" and not args.path:
        parser.error("--path is required for receive")
    config=json.loads((args.config or ROOT/args.protocol.lower()/"connection.json").read_text(encoding="utf-8"))
    if args.endpoint:
        config["endpoint"]=args.endpoint
    if args.protocol=="FILE" and config["endpoint"].startswith("<"):
        parser.error("--endpoint must specify an existing absolute mounted NAS folder")
    connection=call(args.api,"connections",config)
    print("Created endpoint:",connection["name"],connection["id"])
    if args.action=="configure":
        return connection
    if args.action in ("users","authenticate","consume"):
        if args.action=="users":
            if args.protocol!="LDAP":parser.error("users is an LDAP action")
            result=call(args.api,f"connections/{connection['id']}/users")
        elif args.action=="authenticate":
            if args.protocol!="LDAP":parser.error("authenticate is an LDAP action")
            import os,getpass
            password=os.environ[args.password_env] if args.password_env else getpass.getpass("Directory password: ")
            result=call(args.api,f"connections/{connection['id']}/authenticate",{"username":args.username or input("Username: "),"password":password})
        else:
            if args.protocol!="KAFKA":parser.error("consume is a Kafka action")
            result=call(args.api,f"connections/{connection['id']}/consume",{})
        print(json.dumps(result,indent=2));return result
    path=args.path
    action=args.action
    if action in ("send","roundtrip"):
        path=path or "sample-"+uuid4().hex[:10]+"-"+args.input.name
        content=args.input.read_bytes()
        result=call(args.api,f"connections/{connection['id']}/operate",
                    {"operation":"send","path":path,"content_type":"application/xml",
                     "content_base64":base64.b64encode(content).decode()})
        print(json.dumps(result,indent=2))
        if action=="send":
            return result
        action="receive"
    result=call(args.api,f"connections/{connection['id']}/operate",{"operation":action,"path":path})
    if action=="receive":
        response=httpx.get(args.api.rstrip("/")+result["download_url"],timeout=120,trust_env=False)
        response.raise_for_status()
        with args.output.open("xb") as target:
            target.write(response.content)
        if args.action=="roundtrip" and args.output.read_bytes()!=args.input.read_bytes():
            raise RuntimeError("Round-trip file bytes differ")
        print("Downloaded:",args.output)
    print(json.dumps(result,indent=2))
    return result


if __name__=="__main__":
    main()
