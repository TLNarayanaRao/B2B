# Relay user guide

Relay is a B2B workspace for partner connections, file delivery, receipt tracking, and JSON/XML transformations.

## Set up your workspace

Start the Python API and React interface using the repository README. Open the interface and confirm the API connected indicator. Create the source folders and destination folders needed for your first transfer. Configure credentials in the environment of the Python server before starting it.

## Create a connection

Open Connections and select New connection. Enter a descriptive name, select the connector, and enter the endpoint. Configure authentication through environment variable references. The secret value belongs on the server; the form stores its reference.

Select Test & save connection. Relay tests the exact candidate settings before writing the profile. The fields remain locked while the test runs. A failed test keeps the form open and shows an error; a failed update leaves the saved configuration unchanged. A successful save records its test time. This is a check at save time, not continuous monitoring.

Existing profiles without a saved test result display Not tested at save. Their next successful configuration save adds the result.

## Operate a connection

Open a saved connection. Select Test connection, Browse files, Send file, or Receive file, where available. Paths are relative to the configured connector root. Choose a file before sending; use a unique destination name. Received files are available for download.

AS2 confirms documents through verified receipts; its basic connection test checks HTTP reachability and identity configuration. Run a signed test exchange to verify partner interoperability. Directory connections offer List users and Test authentication. Messaging consumers place received documents in Activity.

## Create a data flow

Create and successfully save the source and destination profiles first. Open Data Flows, select Create data flow, and choose both endpoints. Enter a filename pattern and destination path template. Use {filename} and {job_id} in the destination template.

For folder sources, configure a polling interval and minimum file age. For AS2 and HTTP inboxes, accepted inbound documents trigger routing. Enable a stream receiver to route broker records. Directory connections cannot participate in file flows. JMS messaging is a destination only.

Flows move original bytes. The transformation studio is a separate operation.

## Transform a document

Open Transformations, choose JSON or XML as source and target, and paste a document. The studio supports flat JSON objects and direct XML child elements. The mapping is a JSON object: each target field points to a source field. Use an empty object to preserve fields. Select Run transformation to view the output and save successful run history.

## Review activity

Activity displays transfer history, connector inbox documents, and AS2 message/receipt evidence. Data Flows displays delivery jobs. An accepted inbound document has been stored durably; downstream delivery has its own job status.

Failed jobs require correction and review. An uncertain outcome may mean the destination accepted the document but the response was lost. Reconcile with the partner before retrying. Pause a flow to stop new scans; already running deliveries may finish.

## Use samples

Open Samples to prepare a connection template. Replace all endpoint placeholders and configure credentials before saving. Templates are starting points, not live partner accounts. The AS2 demonstration starts two local peers and tests both receipt modes. The stream demonstration needs an installed broker distribution and Java 17.

See the connector reference for supported operations and current limitations.
