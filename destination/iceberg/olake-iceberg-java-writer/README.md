# Java Iceberg Sink

This project is a standalone gRPC writer that OLake uses to write data into Iceberg. It does not use or depend on Debezium. It was originally derived from [debezium-server-iceberg](https://github.com/memiiso/debezium-server-iceberg) (Apache License 2.0) and has since been rewritten for OLake; see `Olake-changes-notice.txt` for attribution details.

## Architecture

The data flow in this project is as follows:


Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

(Check out the Olake Iceberg Writer code to understand how data is sent to Java via gRPC.)

## Development and Testing

For detailed instructions on setting up the development environment, prerequisites, running, debugging, and testing this component, please refer to the [CONTRIBUTING.md](./CONTRIBUTING.md) file.

## Catalog HTTP diagnostics

The shared JVM logs every Apache HttpClient 5 request/response attempt, including
429 responses subsequently hidden by successful retries. `Catalog HTTP request`
and `Catalog HTTP response` lines contain the method, sanitized path, response
host/status, and time to response headers. `attempt_id` joins each request,
response, and its allowlisted response headers (`x-amzn-requestid`,
`x-amz-request-id`, and `retry-after`). Each retry gets a new attempt ID; these
IDs are local to the JVM, not AWS request IDs. Use the pod/run metadata and log
timestamp when correlating attempts across syncs.

Iceberg 1.10.2 does not expose a transport interceptor hook. A dedicated,
non-additive Log4j appender consumes Apache's typed header events synchronously
and emits only the fields above. Raw header events never reach parent appenders.
Queries, URI user information, arbitrary headers, and request/response bodies
are not emitted. Control characters are replaced and diagnostic values are
limited to 512 characters. Keep the header-event regression test when upgrading
HttpClient or changing logging configuration; do not enable HTTP wire logging.

The method/path identifies the public REST request, not a proven internal AWS
operation. Supply the AWS response request ID to Support for native-operation
and quota attribution.

Run `mvn -Dtest=CatalogHttpLoggingTest test` for a local HTTP server exercising
429-to-success retries, GET/POST requests, AWS ID correlation, and secret redaction.
