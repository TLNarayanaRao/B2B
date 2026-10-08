# Relay B2B workspace

Relay connects partner endpoints, moves files between them, records delivery evidence, and transforms flat JSON/XML documents. The workspace uses a React interface, a Python API, and a local SQLite store.

## Start or restart

Run these commands from the project folder. Initial setup creates the environment and installs dependencies.

~~~powershell
cd D:\B2BSoftware\B2B
python -m venv .venv
.\.venv\Scripts\python -m pip install -r backend/requirements.txt
.\.venv\Scripts\python -m uvicorn backend.main:app --host 127.0.0.1 --port 8010 --reload --reload-dir backend
~~~

In another terminal:

~~~powershell
cd D:\B2BSoftware\B2B\frontend
$env:RELAY_API_URL = "http://127.0.0.1:8010"
npm.cmd install
npm.cmd run dev -- --port 5174 --strictPort
~~~

Open http://127.0.0.1:5174. API reference: http://127.0.0.1:8010/docs.
To restart, press Ctrl+C in each server terminal and rerun its start command.
Use npm.cmd in PowerShell if execution policy blocks npm.ps1. Run frontend commands inside frontend, where package.json lives. If a port is unavailable, choose another unused port and update RELAY_API_URL accordingly.

The frontend proxy defaults to the Python API at `http://127.0.0.1:8010`. Restart the frontend after changing `RELAY_API_URL`. If a connection test reports an HTTP 500/502 API error, first check that `http://127.0.0.1:8010/api/health` responds and that the frontend targets the same API port. A Kafka broker is a separate service: the sample requires a running broker at `127.0.0.1:9092`, an existing `b2b-orders` topic, and `PLAINTEXT` security; secured brokers require matching TLS/SASL settings.

## Using the workspace

- Connections: create and test endpoint profiles; browse, send, receive, or perform directory actions.
- Data Flows: route files from a source to a destination with durable delivery jobs.
- Transformations: transform flat JSON objects or direct XML child elements; use a mapping from target field names to source field names.
- Activity: review transfers, inbound documents, delivery jobs, and AS2 receipt evidence.
- Samples: prepare a connection template and run a demonstration.
- Documentation: read Relay's own guides inside the app.

## Documentation

- [User guide](docs/USER_GUIDE.md)
- [Connector reference](CONNECTORS.md)
- [Protocol setup and delivery semantics](PROTOCOLS.md)
- [File routing and recovery](DATA_FLOWS.md)
- [API reference](docs/API_REFERENCE.md)
- [Operations guide](docs/OPERATIONS.md)
- [Runnable samples](samples/README.md)

New connections and configuration updates are tested before saving. Failed tests leave existing settings unchanged. Credentials resolve from server environment variables; profiles store variable names.

Kafka also supports named server properties files: copy `configuration/Kafka.xyz.properties.example` to `configuration/Kafka.xyz.properties`, fill in the broker and credentials, then enter `xyz` in the connection's **Configuration key** field. The endpoint may be blank. See [named Kafka properties files](PROTOCOLS.md#named-kafka-properties-files) for supported settings and reload behavior.

The same approach works for **all connection types**: `<CONNECTION_TYPE>.<PARTNER_KEY>.properties` in the server configuration folder. Examples for every type are included under `configuration`. Enter the partner key in **Configuration key / partner**; the file can supply the endpoint, connector settings, and credentials. See [shared properties format](PROTOCOLS.md#connectiontypepartnertype-properties-files).

The current workspace runs locally. File operations are limited to 10 MiB. Directory connections provide lookup and authentication rather than file transport. JMS messaging is outbound only. Transformation steps inside Data Flows are not yet implemented.
