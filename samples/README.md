# Relay runnable samples

Run commands from D:\B2BSoftware\B2B. Open Samples in the interface to review connection templates. Prepare the endpoint and credentials first: saving a sample runs a real connection check and rejects unreachable settings.

## Two-peer AS2 demonstration

~~~powershell
.\.venv\Scripts\python -m samples.as2.run_demo --mdn-mode BOTH
~~~

The demonstration starts two isolated local servers, generates ephemeral certificates, and uses separate databases and NAS folders. It sends an order and acknowledgement in opposite directions with signing, encryption, and compression. SYNC and ASYNC modes verify message correlation, document MIC, receipt signatures, durable jobs, and exact destination bytes.

Evidence is retained under .samples-runtime: result.json, files, logs, and verified MDN messages. The servers stop after the demonstration.

For a real partner, prepare samples/as2/connection.json. Set AS2_LOCAL_KEY to the local PEM private key/certificate and AS2_PARTNER_CERT to the partner public certificate. Exchange IDs, certificates, and public inbound URLs. ASYNC requires your mdn_url and the exact allowed partner_mdn_url. Keep receipt evidence for reconciliation.

## Folder and object transfers

Start the API first. Replace template placeholders and configure secrets in the API server environment.

~~~powershell
.\.venv\Scripts\python -m samples.protocol_examples SFTP --api http://127.0.0.1:8010 --action roundtrip --output received-sftp.xml
.\.venv\Scripts\python -m samples.protocol_examples SMB --api http://127.0.0.1:8010 --action roundtrip --output received-smb.xml
.\.venv\Scripts\python -m samples.protocol_examples GCS --api http://127.0.0.1:8010 --action roundtrip --output received-bucket.xml
.\.venv\Scripts\python -m samples.protocol_examples FILE --api http://127.0.0.1:8010 --endpoint D:\NAS\orders --action roundtrip --output received-nas.xml
.\.venv\Scripts\python -m samples.protocol_examples EMS --api http://127.0.0.1:8010 --action send
~~~

Roundtrip creates a uniquely named remote file, downloads it, and compares exact bytes. Local downloads use exclusive creation. Remote sample uploads remain available for inspection.

- SFTP: configure password or private-key access and independently verify the pinned SSH host fingerprint.
- SMB: configure share access, credentials, and encryption.
- Object storage: configure bucket/project and service-account JSON or default application credentials.
- Mounted NAS: use an existing absolute folder accessible to the backend service account.
- JMS messaging: install Java 17 and the compatible broker client/API JARs, configure RELAY_EMS_CLASSPATH and EMS_PASSWORD, and choose a queue/topic.

The JMS sample publishes only; consumption is not implemented.

## Individual actions

Use --action configure, test, list, send, receive, or roundtrip as supported. Receive requires --path and --output. Use --config with your own profile and --api to select a backend URL when the API is running on a different port.

~~~powershell
.\.venv\Scripts\python -m samples.protocol_examples SFTP --api http://127.0.0.1:8010 --action test
~~~

Protocol identifiers in commands are compatibility keys. The interface uses descriptive connector names.

## Additional connector examples

See samples/CONNECTORS.md for HTTP/HTTPS, FTP/FTPS, stream messaging, LDAP directory, and document-library examples. Data Flows routes accepted AS2/HTTP documents or stream records to NAS; folder profiles can feed partner destinations.
