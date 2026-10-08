"""Run NAS -> a real isolated stream broker -> NAS; requires Kafka files and Java 17."""
import argparse
import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from confluent_kafka.admin import AdminClient,NewTopic
from fastapi.testclient import TestClient
from backend import main,flows,kafka


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0));return sock.getsockname()[1]


def main_demo():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-home",type=Path,required=True,help="Extracted compatible stream broker 4.x distribution")
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    runtime=(args.output or root/".samples-runtime"/("kafka-"+uuid4().hex[:10])).resolve()
    runtime.mkdir(parents=True,exist_ok=False)
    kafka_home=args.kafka_home.resolve()
    java=os.environ.get("RELAY_JAVA") or shutil.which("java")
    if not java or not (kafka_home/"libs").is_dir():
        raise RuntimeError("Provide Java 17 and an extracted Kafka distribution")
    port,controller=free_port(),free_port()
    while controller==port:controller=free_port()
    config=runtime/"broker.properties"
    config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={(runtime/'logs').as_posix()}
num.partitions=1
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
""",encoding="utf-8")
    base=[java,"-Xms256m","-Xmx512m","-cp",str(kafka_home/"libs"/"*")]
    flags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0
    cluster=subprocess.run(base+["kafka.tools.StorageTool","random-uuid"],capture_output=True,text=True,check=True,creationflags=flags).stdout.strip().splitlines()[-1]
    subprocess.run(base+["kafka.tools.StorageTool","format","--standalone","-t",cluster,"-c",str(config)],capture_output=True,text=True,check=True,creationflags=flags)
    logfile=(runtime/"broker.log").open("wb")
    process=subprocess.Popen(base+["kafka.Kafka",str(config)],stdout=logfile,stderr=logfile,creationflags=flags)
    old_db=main.DB
    main.DB=runtime/"relay.db"
    try:
        admin=AdminClient({"bootstrap.servers":f"127.0.0.1:{port}","log_level":0,"socket.timeout.ms":2000})
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            if process.poll() is not None:
                raise RuntimeError("Kafka exited; inspect "+str(runtime/"broker.log"))
            try:
                if admin.list_topics(timeout=1).brokers:break
            except Exception:
                time.sleep(.2)
        else:raise RuntimeError("Kafka readiness timeout")
        admin.create_topics([NewTopic("b2b-orders",num_partitions=1,replication_factor=1)])["b2b-orders"].result(15)
        client=TestClient(main.app)  # The broker is real; API calls use an isolated ASGI client.
        def create(protocol,endpoint,options=None):
            result=client.post("/api/connections",json={"name":"Sample "+protocol,"protocol":protocol,"endpoint":endpoint,"config":options or {}})
            result.raise_for_status();return result.json()
        out=runtime/"nas-out";out.mkdir()
        incoming=runtime/"nas-in";incoming.mkdir()
        payload=(root/"samples"/"payloads"/"purchase-order.xml").read_bytes()
        (out/"purchase-order.xml").write_bytes(payload)
        source=create("FILE",str(out));destination=create("FILE",str(incoming))
        broker=create("KAFKA",f"kafka://127.0.0.1:{port}",{"topic":"b2b-orders","security_protocol":"PLAINTEXT",
            "enable_receiver":True,"consumer_group_id":"relay-demo-"+uuid4().hex[:8],"auto_offset_reset":"earliest"})
        def route(source,destination):
            response=client.post("/api/flows",json={"name":"Demo route","source_id":source["id"],"destination_id":destination["id"],"min_age_seconds":0})
            response.raise_for_status();return response.json()
        outbound=route(source,broker);route(broker,destination)
        assert flows.poll_flow(outbound["id"],force=True)["queued"]==1
        assert flows.process_next_job()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if kafka.poll_receiver(broker):break
        else:raise RuntimeError("No Kafka message was received")
        assert flows.process_next_job()
        assert (incoming/"purchase-order.xml").read_bytes()==payload
        assert all(job["status"]=="success" for job in flows.list_jobs())
        evidence={"broker_version":kafka_home.name,"document_bytes":len(payload),
            "jobs":flows.list_jobs(),"inbox":client.get("/api/connectors/inbox").json()}
        (runtime/"result.json").write_text(json.dumps(evidence,indent=2),encoding="utf-8")
        print("PASS: NAS -> real Kafka broker -> durable inbox -> NAS, exact bytes, committed offsets")
        print("Demo evidence:",runtime)
    finally:
        kafka.close_receivers()
        main.DB=old_db
        process.terminate()
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        logfile.close()


if __name__=="__main__":
    main_demo()
