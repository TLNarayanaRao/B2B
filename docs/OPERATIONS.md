# Relay operations guide

## Server environment

Configure credential values in the environment of the Python process. Profiles store variable names, not passwords, keys, or certificates. Set the values before starting the backend; restart after changing them.

Use RELAY_DB_PATH to select the SQLite store. The default is backend/relay.db. Use RELAY_API_URL in the frontend terminal to point the development proxy at the backend. Keep the API bound to a loopback address for local development.

## Connection check failures

Confirm the endpoint URL, port, protocol, and configured root. Check server network access, credentials, permissions, and certificate trust. For SSH verify the pinned host fingerprint. For streams create the topic first. For mounted folders ensure the server service account can read the existing root.

A failed save keeps the profile unchanged. The recorded successful test describes access at the time of saving; use the Operate tab to check again when investigating an outage. AS2 reachability does not prove receipt interoperability.

## Delivery recovery

Review jobs and receipt evidence before retrying. Failed jobs may require corrected configuration or folder permissions. Uncertain jobs may already have reached the destination. Confirm with the partner or broker before resending. Duplicate or partial remote files need reconciliation.

Pause flows during planned endpoint maintenance. Existing running jobs may finish. Enable the flow and explicitly retry held jobs after maintenance.

## Storage and backup

The SQLite database contains configuration, received documents, queued payloads, delivery jobs, transformation history, and receipt evidence. Stop the worker before a filesystem backup or use a consistent database backup procedure. Back up the database together with separately managed credentials and configuration. There is no automatic retention cleanup; monitor storage growth.

Run one backend worker against this store. Multiple independent replicas require a coordinated worker design. Sample demonstrations use isolated runtime folders under .samples-runtime and do not use production partner credentials.

## Deployment requirements

The local development API has no administrator authentication. External deployment needs administrative authentication and authorization, TLS termination, controlled ingress, restricted service-account permissions, credential management, content checks, database backups, retention, and endpoint interoperability testing.

For an HTTPS-only inbox, ensure the application observes a trusted HTTPS request. Do not trust arbitrary forwarded headers. Separate public partner ingress from administrative routes.

## Verification

Install development dependencies, run the backend tests, and build the frontend.

~~~powershell
.\.venv\Scripts\python -m pip install -r backend/requirements-dev.txt
.\.venv\Scripts\python -m pytest backend -q --basetemp .test-runtime/new-verification-run
cd frontend
npm.cmd run build
~~~

The AS2 sample runs real local HTTP exchanges in both directions with signed receipts. The stream sample tests a real broker. Live external storage, SSH, network-share, directory, library, and JMS endpoints still require your authorized environment.
