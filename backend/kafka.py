"""Event stream file publishing and durable consumption with explicit offset commits."""
import logging
import threading
from urllib.parse import urlsplit

from .protocols import ConnectorError, secret

LOCK=threading.Lock()
CONSUMERS={}


def configuration(connection,options):
    parsed=urlsplit(connection["endpoint"])
    servers=options.bootstrap_servers or parsed.netloc
    config={"bootstrap.servers":servers,"client.id":options.client_id,
            "security.protocol":options.security_protocol,
            "socket.timeout.ms":options.timeout*1000,"allow.auto.create.topics":False,
            "log_level":0}
    if options.security_protocol.startswith("SASL"):
        if not options.username:
            raise ConnectorError("Event stream SASL requires a username")
        config.update({"sasl.mechanism":options.sasl_mechanism,"sasl.username":options.username,
                       "sasl.password":secret(options.password_env,True)})
    if "SSL" in options.security_protocol:
        config["ssl.endpoint.identification.algorithm"]="https"
        if options.ca_certificate_env:
            config["ssl.ca.pem"]=secret(options.ca_certificate_env,True)
        if options.client_certificate_env:
            config["ssl.certificate.pem"]=secret(options.client_certificate_env,True)
            config["ssl.key.pem"]=secret(options.client_key_env,True)
            if options.client_key_password_env:
                config["ssl.key.password"]=secret(options.client_key_password_env,True)
    return config


def operate_kafka(connection,options,operation,path,data,content_type):
    from confluent_kafka import Producer
    if operation not in ("test","send"):
        raise ConnectorError("Event stream inbound uses Enable receiver and the durable inbox; it has no file directory")
    if not options.topic:
        raise ConnectorError("Event stream topic must be configured")
    config=configuration(connection,options)
    config.update({"enable.idempotence":True,"acks":"all","message.max.bytes":options.message_max_bytes+4096,
                   "delivery.timeout.ms":options.timeout*1000,"request.timeout.ms":min(options.timeout*1000,10000),
                   "compression.type":options.compression_type})
    producer=Producer(config,logger=logging.getLogger("relay.kafka"))
    if operation=="test":
        metadata=producer.list_topics(timeout=options.timeout)
        topic=metadata.topics.get(options.topic)
        if not topic or topic.error:
            raise ConnectorError("Event stream topic is unavailable or access was denied")
        return {"reachable":True,"topic":options.topic,"partitions":len(topic.partitions)}
    if len(data)>options.message_max_bytes:
        raise ConnectorError("Document exceeds configured Kafka Message Max Size Bytes")
    outcome=[]
    def delivered(error,message):
        outcome.append((error,message))
    producer.produce(options.topic,value=data,key=options.record_key or None,partition=options.partition,
                     headers={"filename":path.encode(),"content-type":content_type.encode()},on_delivery=delivered)
    pending=producer.flush(options.timeout+1)
    if pending or not outcome or outcome[0][0]:
        raise ConnectorError("Event stream acknowledgement failed or timed out; delivery is uncertain")
    message=outcome[0][1]
    return {"bytes":len(data),"path":path,"topic":message.topic(),"partition":message.partition(),"offset":message.offset()}


def poll_receiver(connection):
    """One consumer per connection, shared by every route; commit only after SQLite commit."""
    from confluent_kafka import Consumer
    from .protocols import KafkaOptions, MAX_BYTES
    from .inbox import accept
    from .flows import filename_only
    options=KafkaOptions.model_validate(connection["config"])
    if not options.enable_receiver:
        return 0
    if not options.topic or not options.consumer_group_id:
        raise ConnectorError("Event stream receiver requires a topic and Consumer Group Id")
    with LOCK:
        key=connection["id"]
        signature=(connection["endpoint"],options.model_dump_json())
        current=CONSUMERS.get(key)
        if current and current[0]!=signature:
            current[1].close();CONSUMERS.pop(key);current=None
        if not current:
            config=configuration(connection,options)
            config.update({"group.id":options.consumer_group_id,"enable.auto.commit":False,
                "enable.auto.offset.store":False,"auto.offset.reset":options.auto_offset_reset,
                "isolation.level":options.isolation_level,"fetch.message.max.bytes":options.message_max_bytes+4096})
            consumer=Consumer(config,logger=logging.getLogger("relay.kafka"))
            consumer.subscribe([options.topic])
            current=(signature,consumer);CONSUMERS[key]=current
        consumer=current[1]
        count=0
        try:
            for _ in range(options.max_poll_records):
                message=consumer.poll(options.poll_timeout_ms/1000 if count==0 else 0)
                if message is None:
                    break
                if message.error():
                    raise ConnectorError("Event stream receiver failed; verify broker and consumer permissions")
                value=message.value()
                # Tombstones carry no document; retain an empty payload plus source identity.
                value=b"" if value is None else value
                if len(value)>min(options.message_max_bytes,MAX_BYTES):
                    raise ConnectorError("Event stream inbound document exceeds configured size; offset was not committed")
                headers=dict(message.headers() or [])
                filename=headers.get("filename")
                filename=filename.decode("utf-8",errors="replace") if filename else f"{message.topic()}-{message.partition()}-{message.offset()}.bin"
                source=f"{message.topic()}:{message.partition()}:{message.offset()}"
                accept(key,source,filename_only(filename),value)
                # A crash between SQLite commit and this commit replays idempotently.
                partitions=consumer.commit(message=message,asynchronous=False)
                if any(getattr(partition,"error",None) for partition in partitions or []):
                    raise ConnectorError("Event stream offset commit failed; durable inbox will deduplicate a replay")
                count+=1
        except Exception:
            consumer.close();CONSUMERS.pop(key,None)
            raise
        return count


def close_receivers(active_ids=None):
    with LOCK:
        for key in list(CONSUMERS):
            if active_ids is None or key not in active_ids:
                CONSUMERS.pop(key)[1].close()
