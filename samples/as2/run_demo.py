"""Two real HTTP servers: NAS -> signed/encrypted AS2 -> NAS, in both directions."""
import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
from .certificates import identity

ROOT = Path(__file__).resolve().parents[2]


def port():
    with socket.socket() as server:
        server.bind(("127.0.0.1",0))
        return server.getsockname()[1]


def api(base,path,body=None,method="POST"):
    response=httpx.request(method if body is not None else "GET",base+"/api/"+path,
                           json=body,timeout=30,trust_env=False)
    response.raise_for_status()
    return response.json()


def start_server(folder,environment):
    assigned=port()
    base=f"http://127.0.0.1:{assigned}"
    log=(folder/"server.log").open("w",encoding="utf-8")
    process=subprocess.Popen([sys.executable,"-m","uvicorn","backend.main:app",
                              "--host","127.0.0.1","--port",str(assigned)],
        cwd=ROOT,env={**os.environ,**environment,"RELAY_DB_PATH":str(folder/"relay.db")},
        stdout=log,stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0)
    try:
        deadline=time.monotonic()+25
        while time.monotonic()<deadline:
            if process.poll() is not None:
                raise RuntimeError("Demo server exited; inspect "+str(folder/"server.log"))
            try:
                if httpx.get(base+"/api/health",timeout=1,trust_env=False).status_code==200:
                    return process,log,base
            except httpx.HTTPError:
                pass
            time.sleep(.2)
        raise RuntimeError("Demo server did not become ready")
    except Exception:
        process.terminate()
        process.wait(timeout=10)
        log.close()
        raise


def endpoint(base,name,protocol,url,config=None):
    return api(base,"connections",{"name":name,"protocol":protocol,"endpoint":url,"config":config or {}})


def flow(base,name,source,destination):
    return api(base,"flows",{"name":name,"source_id":source["id"],"destination_id":destination["id"],
        "file_pattern":"*.xml","destination_pattern":"{job_id}-{filename}","min_age_seconds":0,
        "interval_seconds":5,"enabled":True})


def wait_for_delivery(bases,outputs,expected):
    deadline=time.monotonic()+40
    while time.monotonic()<deadline:
        documents=[list(folder.glob("*.xml")) for folder in outputs]
        jobs=[api(base,"jobs") for base in bases]
        if all(documents) and all(len(items)==2 and all(j["status"]=="success" for j in items) for items in jobs):
            for document,content in zip(documents,expected):
                if len(document)!=1 or document[0].read_bytes()!=content:
                    raise RuntimeError("Received file bytes do not match the source")
            return jobs
        time.sleep(.3)
    raise RuntimeError("Delivery timed out. Inspect demo logs, jobs and AS2 messages in the runtime folder.")


def run_mode(mode,runtime):
    work=runtime/mode.lower()
    for name in ("alice","bob"):
        for folder in ("outbound","inbound"):
            (work/name/folder).mkdir(parents=True,exist_ok=True)
    alice_key,alice_cert=identity("DEMO-ALICE")
    bob_key,bob_cert=identity("DEMO-BOB")
    servers=[]
    try:
        a=start_server(work/"alice",{"DEMO_LOCAL_KEY":alice_key,"DEMO_PARTNER_CERT":bob_cert})
        servers.append(a)
        b=start_server(work/"bob",{"DEMO_LOCAL_KEY":bob_key,"DEMO_PARTNER_CERT":alice_cert})
        servers.append(b)
        abase,bbase=a[2],b[2]
        common={"signing_key_env":"DEMO_LOCAL_KEY","decryption_key_env":"DEMO_LOCAL_KEY",
                "partner_certificate_env":"DEMO_PARTNER_CERT","sign":True,"encrypt":True,
                "compress":True,"require_signed_mdn":True}
        ac=endpoint(abase,"Bob AS2","AS2",bbase+"/api/health",
                    {**common,"local_as2_id":"DEMO-ALICE","partner_as2_id":"DEMO-BOB"})
        bc=endpoint(bbase,"Alice AS2","AS2",abase+"/api/health",
                    {**common,"local_as2_id":"DEMO-BOB","partner_as2_id":"DEMO-ALICE"})
        for base,peer,own,other in ((abase,bbase,ac,bc),(bbase,abase,bc,ac)):
            config={**own["config"],"mdn_mode":mode,
                    "mdn_url":base+"/as2/"+own["id"]+"/mdn" if mode=="ASYNC" else "",
                    "partner_mdn_url":peer+"/as2/"+other["id"]+"/mdn" if mode=="ASYNC" else ""}
            api(base,"connections/"+own["id"],{"name":own["name"],"protocol":"AS2",
                "endpoint":peer+"/as2/"+other["id"],"config":config},method="PUT")
        aout=endpoint(abase,"Alice NAS outbound","FILE",str(work/"alice"/"outbound"))
        ain=endpoint(abase,"Alice NAS inbound","FILE",str(work/"alice"/"inbound"))
        bout=endpoint(bbase,"Bob NAS outbound","FILE",str(work/"bob"/"outbound"))
        bin_=endpoint(bbase,"Bob NAS inbound","FILE",str(work/"bob"/"inbound"))
        af=flow(abase,"Alice NAS to Bob AS2",aout,ac)
        flow(abase,"Bob AS2 to Alice NAS",ac,ain)
        bf=flow(bbase,"Bob NAS to Alice AS2",bout,bc)
        flow(bbase,"Alice AS2 to Bob NAS",bc,bin_)
        order=(ROOT/"samples"/"payloads"/"purchase-order.xml").read_bytes()
        acknowledgement=(ROOT/"samples"/"payloads"/"order-acknowledgement.xml").read_bytes()
        (work/"alice"/"outbound"/"purchase-order.xml").write_bytes(order)
        (work/"bob"/"outbound"/"order-acknowledgement.xml").write_bytes(acknowledgement)
        api(abase,"flows/"+af["id"]+"/run",{})
        api(bbase,"flows/"+bf["id"]+"/run",{})
        jobs=wait_for_delivery([abase,bbase],[work/"alice"/"inbound",work/"bob"/"inbound"],
                               [acknowledgement,order])
        evidence={}
        for name,base in (("alice",abase),("bob",bbase)):
            messages=api(base,"as2/messages")
            if not messages["outbound"] or any(m["status"]!="confirmed" for m in messages["outbound"]):
                raise RuntimeError("No verified MDN for the outbound exchange")
            for message in messages["outbound"]:
                response=httpx.get(base+"/api/as2/messages/"+message["message_id"]+"/receipt",trust_env=False)
                response.raise_for_status()
                (work/name/"verified-mdn.eml").write_bytes(response.content)
            evidence[name]={"messages":messages,"jobs":api(base,"jobs")}
        summary={"mode":mode,"result":"PASS","directions":["Alice NAS -> Bob AS2 -> Bob NAS",
                    "Bob NAS -> Alice AS2 -> Alice NAS"],"signing":"SHA-256",
                 "encryption":"AES-256-CBC","compression":"ZLIB",
                 "purchase_order_sha256":hashlib.sha256(order).hexdigest(),
                 "acknowledgement_sha256":hashlib.sha256(acknowledgement).hexdigest(),
                 "evidence":evidence}
        (work/"result.json").write_text(json.dumps(summary,indent=2),encoding="utf-8")
        print(f"PASS {mode}: both directions, exact file bytes, durable jobs and verified signed MDNs")
        return summary
    finally:
        for process,log,_ in servers:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            log.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mdn-mode",choices=["SYNC","ASYNC","BOTH"],default="BOTH")
    parser.add_argument("--output",type=Path,help="New runtime folder; defaults to a unique ignored folder")
    args=parser.parse_args()
    runtime=(args.output or ROOT/".samples-runtime"/("as2-"+uuid4().hex[:12])).resolve()
    runtime.mkdir(parents=True,exist_ok=False)
    results=[run_mode(mode,runtime) for mode in (["SYNC","ASYNC"] if args.mdn_mode=="BOTH" else [args.mdn_mode])]
    print("Demo evidence:",runtime)
    return results


if __name__=="__main__":
    main()
