# Relay connector examples

Use the connection.json template in each sample folder. Replace placeholders and configure credentials before starting the API. Select Test & save in the interface; a failed connection check prevents creation.

## HTTP and HTTPS

Configure the partner URL and outbound basic or bearer credentials. For inbound uploads enable the inbox and configure HTTP_INBOUND_TOKEN. Share /http/{connection_id}/{filename}; accepted raw bytes are stored durably before acknowledgement.

~~~powershell
$env:HTTP_PARTNER_TOKEN = '<partner-secret>'
$env:HTTP_INBOUND_TOKEN = '<inbound-secret>'
.\.venv\Scripts\python -m samples.protocol_examples HTTP --api http://127.0.0.1:8010 --action roundtrip --output received-http.xml
.\.venv\Scripts\python -m samples.protocol_examples HTTPS --api http://127.0.0.1:8010 --action roundtrip --output received-https.xml
~~~

Point the sample at an existing partner or another configured Relay inbox. The inbox requires bearer authentication, returns 412 for existing paths, and supports GET downloads and GET ?list=1 listings. HTTPS certificates are verified. For event-driven NAS routing enable inbound_only and create a flow to FILE or SMB.

## FTP and FTPS

Configure username, FTP_PASSWORD, the endpoint, and passive/active mode. Directory listings require MLSD. FTPS supports explicit or implicit TLS with verified certificates and protected data channels.

~~~powershell
$env:FTP_USERNAME = 'relay'
$env:FTP_PASSWORD = '<password>'
.\.venv\Scripts\python -m backend.ftp_listener --root D:\NAS\orders --port 2121
~~~

In another terminal, with the API running:

~~~powershell
.\.venv\Scripts\python -m samples.protocol_examples FTP --api http://127.0.0.1:8010 --action roundtrip --output received-ftp.xml
.\.venv\Scripts\python -m samples.protocol_examples FTPS --api http://127.0.0.1:8010 --action roundtrip --output received-ftps.xml
~~~

For explicit FTPS on the listener add --certificate server-cert.pem --key server-key.pem. Set FTP_CA_CERT to the issuing PEM certificate when using a private CA. The listener denies overwrite, append, rename, and deletion. It stages partial uploads privately and publishes complete files with an exclusive hard link before acknowledgement; the NAS filesystem must support hard links.

Use a FILE-source flow to forward completed listener uploads. Remote FTP overwrite checks can race concurrent writers; use unique filenames.

## Mounted folders, SSH, and network shares

Use FILE, SFTP, and SMB roundtrip examples in samples/README.md. Optional parent-directory creation stays within the configured root. SFTP supports compression and requires a trusted host fingerprint. SMB accepts a separate domain or DOMAIN\user.

Optional FILE post-processing resolves a JSON argument array from post_process_command_env. Supported tokens are file, filename, path, base, noext, and ext. Execution has a timeout and no implicit shell. A failure leaves the written file in place and requires review before retrying.

## Event stream

Create the topic before saving. Configure the bootstrap servers and security. Enable the receiver and assign a consumer group for inbound records. The consumer stores document bytes and routing jobs before committing the offset; replay does not duplicate accepted documents.

~~~powershell
.\.venv\Scripts\python -m samples.protocol_examples KAFKA --api http://127.0.0.1:8010 --action send
.\.venv\Scripts\python -m samples.protocol_examples KAFKA --api http://127.0.0.1:8010 --action consume
~~~

Use SASL_SSL or SSL for secured brokers, with trusted CA and optional client certificate/key references. SCRAM-SHA-256, SCRAM-SHA-512, and PLAIN are supported. The loopback example uses PLAINTEXT. Default message_max_bytes is 1,000,000; broker/topic limits must permit it.

The isolated real-broker demonstration requires a compatible broker distribution and Java 17:

~~~powershell
.\.venv\Scripts\python -m samples.kafka.run_demo --kafka-home D:\tools\kafka_2.13-4.2.2
~~~

It uses random loopback ports, its own database and NAS folders, stops its broker after execution, and retains logs/results under .samples-runtime. Broker idempotence does not imply exactly-once delivery across the entire NAS pipeline.

## LDAP directory

Configure synchronization-account DN/password, Base DN, filter, and attributes. Use LDAPS or StartTLS. Filters use RFC 4515 parenthesized syntax. The connector supports DNS SRV discovery and ordered manual failover.

~~~powershell
$env:LDAP_SYNC_PASSWORD = '<sync-password>'
.\.venv\Scripts\python -m samples.protocol_examples LDAP --api http://127.0.0.1:8010 --action users
.\.venv\Scripts\python -m samples.protocol_examples LDAP --api http://127.0.0.1:8010 --action authenticate --username alice
~~~

Authentication prompts for a password rather than exposing it in a command. The connector is for lookup and user bind checks, not administrator login or file flow transport.

## Document library

Register an application for the intended site/library and grant application-client permissions with administrator approval. Configure tenant_id, application_id, application_secret_env, drive_name, and root_path. The endpoint is the actual site URL; leave that service address intact.

~~~powershell
$env:SHAREPOINT_APPLICATION_SECRET = '<application-secret>'
.\.venv\Scripts\python -m samples.protocol_examples SHAREPOINT --api http://127.0.0.1:8010 --action roundtrip --output received-library.xml
~~~

SHAREPOINT is the existing API identifier for this connector. Upload sessions reject conflicts and use 320 KiB-aligned chunks. Downloads check ETags and approved transfer hosts; access tokens are not sent to preauthenticated transfer URLs. The connector supports one configured site/library and nonempty files up to 10 MiB.

## Verification

Backend tests cover FTP/FTPS sockets, HTTP inbox routing, durable stream offsets, LDAP binding and filters, and library upload/download contracts. The stream demonstration tests an actual broker. Live external endpoints still require credentials and authorized network access.
