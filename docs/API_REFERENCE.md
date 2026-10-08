# Relay API reference

The development API is local. Start it on the configured port and open /docs for interactive request schemas. Configure external ingress separately from administrative access.

## Profiles

- GET /api/health: backend availability; HEAD is supported.
- GET /api/connections: saved profiles with configuration and optional connection_test.
- POST /api/connections/test: test an unsaved profile.
- POST /api/connections: test and create a profile; returns 201.
- PUT /api/connections/{id}: test and replace an existing profile.
- POST /api/connections/{id}/operate: run test, list, send, or receive.

A candidate profile has name, protocol, endpoint, and config. Protocol identifiers are AS2, HTTP, HTTPS, FTP, FTPS, FILE, SFTP, SMB, GCS, KAFKA, LDAP, SHAREPOINT, and EMS. Interface labels describe their roles.

Validation and failed save tests return 422. An unknown profile returns 404. A failed check writes nothing. Operation requests include operation, relative path, and content_base64 for sends, plus content_type. File size is limited to 10 MiB.

## AS2 endpoints

- POST /as2/{connection_id}: accept a partner document.
- HEAD /as2/{connection_id}: listener reachability.
- POST /as2/{connection_id}/mdn: accept an asynchronous receipt.
- GET /api/connections/{id}/as2-info: public onboarding information.
- GET /api/as2/messages: message confirmation states.
- GET /api/as2/messages/{message_id}/receipt: download receipt evidence.
- POST /api/as2/receipts/{receipt_id}/retry: retry an outgoing receipt callback.

Use signed/encrypted message MIME payloads and partner identity headers. Callback URLs are configured and allowlisted; they are not arbitrary relay destinations.

## HTTP inbox

PUT or POST /http/{connection_id}/{path} uploads raw document bytes. GET retrieves a stored path. HEAD checks the listener, and GET ?list=1 returns listing information. Every request requires the configured inbound bearer token. Enable the inbox before use. An existing upload path returns 412.

## Messaging and directory actions

- POST /api/connections/{id}/consume: poll a stream receiver.
- GET /api/connectors/inbox: recent received documents and receiver status.
- GET /api/connectors/inbox/{document_id}: download an accepted document.
- GET /api/connections/{id}/users: directory lookup.
- POST /api/connections/{id}/authenticate: single-use directory authentication.

Directory user passwords are request-only inputs. Inbox records are accepted durably before broker offset commit.

## Workspace operations

- GET /api/flows: list flows.
- POST /api/flows: create a flow.
- PUT /api/flows/{id}: update a flow.
- POST /api/flows/{id}/run: scan a source.
- GET /api/jobs: list jobs.
- POST /api/jobs/{id}/retry: explicitly retry after review.
- GET /api/transfers: transfer history.
- POST /api/transform: convert JSON/XML with optional field mapping.
- GET /api/runs: transformation history.

Request schemas are listed in the running /docs reference. Data Flows supports creating flows, changing enabled state, running a scan, inspecting jobs, and explicitly retrying a delivery. Transformations support JSON/XML conversion and field mapping.

GET /api/connector-catalog returns Relay's capabilities and current limitations. GET /api/documentation lists local guides. GET /api/documentation/{slug} returns a guide body. Guides are served from an explicit local allowlist.
