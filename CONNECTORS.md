# Relay connector reference

This reference describes Relay capabilities. Detailed configuration is in PROTOCOLS.md and sample commands are in samples/CONNECTORS.md.

HTTP/HTTPS and FTP/FTPS share connector families. The remaining connectors have individual profiles.

## HTTP / HTTPS

API identifier: HTTP. Role: File transfer.

Partner PUT/POST/GET, JSON directory listings, Basic/Bearer auth, verified TLS, authenticated durable inbox and NAS routing.

Verification: Local HTTP roundtrip and NAS-routing tests.

Current limitations: Server-specific CONNECT/CONFIRM choreography, multipart forms, automatic retries and full action language.

## FTP / FTPS

API identifier: FTP. Role: File transfer.

Binary STOR/RETR, MLSD listing, passive/active mode, verified explicit/implicit TLS client and explicit TLS NAS listener.

Verification: Real FTP and explicit FTPS socket roundtrips.

Current limitations: ASCII translation, LIST fallback, resume, proxy/dial-up, raw QUOTE/SITE commands. Remote FTP overwrite protection is preflight only.

## File

API identifier: FILE. Role: File transfer.

Rooted native/NAS reads and writes, browse, exclusive file creation, optional parent directories and token-based post-processing.

Verification: Actual local NAS files and post-processing tests.

Current limitations: Unrestricted absolute paths, arbitrary JavaScript/date expressions, custom macro expansion and command-output log capture.

## Event stream

API identifier: KAFKA. Role: Messaging.

Topic publish/consume, record keys/partitions, SASL/TLS and client certificates, durable inbox before explicit offset commit.

Verification: Real stream broker NAS-to-broker-to-NAS demo and replay/failure tests.

Current limitations: Runtime-specific tuning, Java keystores, multi-topic publishing, automatic offset commit. Idempotent producer does not make the complete NAS pipeline exactly-once.

## LDAP

API identifier: LDAP. Role: Authentication.

Sync bind, filtered user search, escaped username filters, user bind authentication, LDAPS/StartTLS, DNS SRV and manual failover.

Verification: Adapter/API contract and filter-injection tests; live directory required.

Current limitations: Comma-separated filter shorthand and URI overrides, SAML provisioning, automatic workspace login and full Users-host policy model.

## SFTP

API identifier: SFTP. Role: File transfer.

SSH browse/upload/download, password/private-key login, trusted host fingerprint, compression and optional parent directories.

Verification: Adapter tests; live partner SSH endpoint required.

Current limitations: OpenPGP packaging, per-algorithm negotiation UI, transfer resume, proxy rotation, virtual Users-host folders and custom URI schemes.

## Document library

API identifier: SHAREPOINT. Role: File transfer.

Application-client OAuth, library discovery, rooted listings, conflict-rejecting chunked uploads and ETag-conditioned downloads.

Verification: Document-library adapter contract and URL/auth/conflict tests; live tenant required.

Current limitations: Cross-site/cross-drive URI browsing, rename/move/delete, folder creation and sovereign-cloud endpoints.

## SMB

API identifier: SMB. Role: File transfer.

Rooted network-share browse/read/write, domain credentials, SMB3 encryption, exclusive creation and optional parent folders.

Verification: Adapter tests; live NAS/share credentials required.

Current limitations: SMB1 legacy/FIPS mode, selectable dialect bounds, client-specific tuning and cache controls.

## AS2

API identifier: AS2. Role: Partner exchange.

Signed, encrypted and compressed documents; verified synchronous/asynchronous receipts; durable inbound acceptance and replay protection.

Verification: Real local HTTP exchange in both directions with verified signed receipts.

Current limitations: Reachability checks require a document exchange for full partner verification; no automatic document resend after uncertain delivery.

## Object storage

API identifier: GCS. Role: File transfer.

Rooted bucket listings, generation-checked downloads and exclusive new-object uploads with service account or default credentials.

Verification: Adapter contracts; live bucket access required.

Current limitations: No object deletion, rename or resumable uploads.

## JMS messaging

API identifier: EMS. Role: Outbound messaging.

Persistent transacted bytes/text publication to a queue or topic through an installed broker client and Java 17 bridge.

Verification: Bridge contract and source compilation; installed broker client required.

Current limitations: Outbound only; no subscriptions, OAuth or broker failover URL lists.
