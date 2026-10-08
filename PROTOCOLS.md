# Relay protocol setup

This guide describes Relay's implemented connectors and delivery behavior. The API configuration keys below are retained for integration compatibility; the interface uses neutral connector labels.

## Connections and credentials

A profile contains name, protocol, endpoint, and config. Test & save checks these settings before creating or updating it. Failed checks do not save settings. Successful checks record connection_test.success and tested_at. POST /api/connections/test validates a candidate without persisting it; every save still runs a fresh check.

Set credential values on the Python server and put only their environment variable names in config fields ending in _env. Restart the server after changing its environment. Endpoints must not embed passwords. Timeouts range from 1 to 120 seconds. File transfers are limited to 10 MiB and browse limits to 1-1500 entries.

## AS2 partner exchange

Use an HTTP or HTTPS partner endpoint. Configure local_as2_id, partner_as2_id, local signing/decryption key references, and partner public certificate references. A local key value contains a PEM private key and certificate. Separate partner signing and encryption certificates are supported.

Outbound documents support signing, encryption, and compression. Inbound documents use configured identity, signature, and encryption policies. The public inbound path is /as2/{connection_id}. Exchange IDs, certificates, and URLs with the partner before live use.

SYNC receipts are returned with the document response. ASYNC receipts use /as2/{connection_id}/mdn. Configure mdn_url for your own callback and partner_mdn_url as the exact allowed partner callback. Receipt signatures, document MIC, and message correlation are verified. A successful HTTP response alone does not confirm delivery.

Identical accepted Message-ID replays return the stored receipt without duplicate routing. Changed content with the same ID is rejected. Missing receipts can leave delivery uncertain. Documents are not automatically resent; receipt callbacks retry up to three times.

## Folder and storage connectors

FILE reads and writes an existing absolute local or mounted NAS directory under the backend service account. SMB uses a rooted network share with credentials, optional domain, and encryption. SFTP uses a rooted SSH endpoint, password or private key authentication, and a trusted host fingerprint. Verify fingerprints with the partner independently.

Paths must remain within the configured root. Traversal is rejected. FILE, SMB, and SFTP use exclusive destination creation. Optional parent directory creation stays inside the root. Interrupted remote sends may leave partial files; inspect them before retrying.

GCS identifies an object-storage bucket using a gs:// URL. Supply a service account JSON environment reference or configure default application credentials on the server. Reads use generation checks and uploads require a new object.

FILE can run an optional post-process command after writing. The command environment value is a JSON argument array; tokens include file, filename, path, base, noext, and ext. Commands run without an implicit shell and with a timeout. Failure does not remove the saved file or automatically resend it.

## HTTP and FTP transfers

HTTP and HTTPS support partner PUT/POST uploads, GET downloads, and JSON directory listings. Basic or bearer authentication is supported. TLS certificates are verified. The partner must support the selected request method and directory-list response format.

An enabled HTTP inbox requires an inbound bearer token. Its public path is /http/{connection_id}/{filename}. PUT/POST accepts raw bytes into durable storage; GET retrieves files and GET ?list=1 lists them. Existing paths return 412. HTTPS enforcement requires the application to observe a trusted HTTPS request.

FTP uses binary STOR/RETR and MLSD listing, with passive or active data channels. FTPS adds explicit or implicit verified TLS. The separate NAS listener supports FTP or explicit FTPS. It hides partial uploads in staging and publishes complete files before acknowledgement; the filesystem must support hard links.

Remote FTP checks for an existing name before upload, but concurrent partner writes can race this check. Use unique filenames. Resume, ASCII conversion, and LIST fallback are not implemented.

## Stream messaging

The KAFKA connector publishes files as records and consumes configured topic records into the durable inbox. Configure bootstrap servers, topic, security settings, and optional record key/partition. The topic must exist before saving.

Producer delivery requires broker acknowledgement. Receivers need enable_receiver and a consumer_group_id. Topic/partition/offset identity prevents duplicate accepted documents and routing jobs on replay. Offsets commit only after durable inbox storage. Broker idempotence does not make the entire file pipeline exactly once.

SASL/TLS and PEM client certificates are supported. Broker size limits must permit the configured record size. Java keystores, multi-topic publication, and automatic offset commit are not supported.

## LDAP directory

Configure the directory URL, synchronization-account DN, password reference, Base DN, search filter, and attribute names. Use LDAPS or StartTLS with a trusted certificate. Filters use parenthesized RFC 4515 syntax. DNS SRV lookup and ordered server failover are supported.

The connector binds, lists users, and validates user credentials through a single-use bind. It does not implement workspace administrator login or file transfer. Test user passwords are not saved.

## Document library

The SHAREPOINT connector uses application-client OAuth credentials for a configured site and document library. Configure tenant_id, application_id, application_secret_env, drive_name, and root_path. Grant the application permission to the intended site/library.

Browse, upload, and download stay within the configured root. Upload sessions reject conflicts and use aligned chunks. Downloads check ETags and approved transfer hosts; access tokens are not forwarded to preauthenticated download URLs. Nonempty files up to 10 MiB are supported.

Cross-site browsing, rename/move/delete, folder creation, and alternative cloud endpoints are not implemented.

## JMS messaging

The EMS connector publishes persistent bytes or UTF-8 text messages through a Java 17 bridge. Configure the installed broker client JAR classpath, broker endpoint, credential references, queue/topic, and JMS namespace. The bridge requires client libraries compatible with its existing connection-factory integration.

A committed publish confirms broker acceptance, not consumer processing. TLS requires a trusted PEM certificate reference. Consumption, client-certificate authentication, OAuth, directory-based connection factories, and failover URL lists are not implemented.

## Verification and dependency licensing

Backend tests cover save-time gates, local files, signed AS2 exchanges, FTP/FTPS sockets, HTTP routing, and adapter contracts. Samples include real HTTP AS2 peers and a real stream broker. External SSH, network-share, directory, library, and cloud-storage interoperability still requires authorized endpoints and credentials.

Dependency identifiers in backend/requirements.txt and package manifests are required for installation. Their licenses remain applicable. The AS2 dependency pyas2lib is GPL-3.0; its redistribution obligations need review before distributing Relay. Protocol compatibility names and machine-level service addresses remain part of the integration configuration.
