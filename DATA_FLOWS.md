# Relay file routing

Data Flows moves files between saved endpoint profiles with a durable queue. Sources are preserved. Each job records its source, destination, payload, and delivery outcome.

## NAS to AS2

1. Save a FILE profile for an existing folder, or an SMB profile for a network share.
2. Save an AS2 profile with the partner endpoint, IDs, and certificates.
3. Create a flow from the NAS profile to the AS2 profile.
4. Set the source subdirectory to outbound, filter to *.edi, and destination template to {filename}.
5. Enable the flow and select Run now to scan immediately.

Folder scans are nonrecursive. The default interval is 30 seconds and minimum age is 5 seconds. Producers should complete a temporary file before renaming it to its final matching name. Age checks alone cannot prove a writer has finished.

The worker snapshots file bytes in SQLite before delivery. Source relative path and SHA-256 digest identify previously queued content. Unchanged content at the same path is skipped on later scans; changed content is a new delivery.

## AS2 to NAS

Create an AS2 source and a FILE/SMB destination. Use destination template inbound/{job_id}-{filename}; prepare the destination folder or configure supported parent creation. Share /as2/{source_connection_id} with the partner.

Relay verifies and stores accepted payloads and matching jobs in one transaction before returning acceptance. A processed receipt means durable inbound acceptance; the NAS write has a separate job status. Multiple flows may route the same inbound document to different destinations.

## HTTP and stream inboxes

Enable an HTTP inbox with its inbound token and inbound_only option for event-driven routing. Partner uploads use /http/{connection_id}/{filename}. Accepted documents and matching jobs commit together before acknowledgement.

For a stream source, enable its receiver and set the consumer group. Offsets commit after inbox storage. Incoming records fan out to enabled matching flows. Activity shows accepted inbox documents and receiver status. FTP listener uploads reach NAS first; route them with a FILE source.

## NAS or AS2 to JMS

Install Java 17 and the broker-compatible client/API JARs required by the bridge. Set RELAY_EMS_CLASSPATH to their real paths and EMS_PASSWORD before starting the backend.

~~~powershell
$env:RELAY_EMS_CLASSPATH = 'D:\connectors\jms\broker-client.jar;D:\connectors\jms\jms-api.jar'
$env:EMS_PASSWORD = '<broker-password>'
$env:RELAY_JAVA = 'D:\runtimes\jdk-17\bin\java.exe'
~~~

Replace these illustrative JAR paths with the actual installed files. The bridge depends on its existing broker connection factory; arbitrary JMS implementations are not interchangeable. Use matching client/API JARs for javax.jms or jakarta.jms.

Save a JMS profile with protocol EMS, broker URL, credential/classpath references, destination, and bytes or text message type. Create a flow from NAS or AS2 to this destination. The path template sets RelayFilename; RelayContentType is also included. Persistent transacted publication confirms broker acceptance. Subscriptions are not implemented.

## Job states and recovery

- queued: waiting for the worker.
- processing: delivery is running.
- awaiting_mdn: an AS2 document is waiting for its asynchronous receipt.
- succeeded: connector delivery is confirmed.
- failed: delivery failed and needs review.
- uncertain: destination acceptance cannot be ruled out.
- held: a paused flow stopped its pending delivery.

The worker starts with the backend. Queue data survives restarts. Interrupted processing becomes uncertain. Run one backend worker for the local SQLite store.

Pause prevents new scans and holds queued deliveries when encountered. An in-progress delivery may finish. Resume the flow and explicitly retry held jobs. Failed and uncertain documents are not automatically resent. Check destination evidence before retrying an uncertain job.

Editing a flow affects new jobs. Jobs retain the destination profile ID selected when queued; editing that profile affects future attempts. Use unique destination names to avoid collisions.

## Current limits

Files are limited to 10 MiB. There is no automatic spool or inbox retention cleanup. Most file destinations reject overwrite; remote FTP's preflight can race other writers. Partial remote writes can remain after interruptions.

Flows move original bytes. Inline transformation steps, distributed coordination, transfer resume, and scheduled calendar execution are not implemented.
