# Contributing to OLake

We truly value and appreciate all contributions — every effort makes a difference, and proper credit will always be given.  
To ensure consistency, we follow a structured contribution process. All guidelines and details have been consolidated into the official OLake documentation. Please refer to the links below for more information:

- [Contribute To OLake](https://olake.io/docs/community/contributing)
- [How To Raise A PR](https://olake.io/docs/community/issues-and-prs)
- [Setting Up A Development Environment](https://olake.io/docs/community/setting-up-a-dev-env)
- **Running tests locally** — run `make help` for all local database-stack and test targets (e.g. `make test.integration.<driver>` for a driver's whole suite, `make test.discover.<driver>` / `make test.sync.<driver>` / `make test.2pc.<driver>` for one of them, `make test.unit`)
- **Backward compatibility tests** — see
  [tests/testutils/compatibility/README.md](tests/testutils/compatibility/README.md)
  for what the suite asserts, how `constants/state-versions.json` drives it,
  and what a PR introducing a state version has to add

## Metadata-only automatic table enrollment

`discover-new-tables` is a PostgreSQL-to-Iceberg preflight, not a sync. It reads
catalog metadata and writes one proposal artifact; it does not start the Iceberg
JVM, read source rows, alter publications, create/advance slots, reset state, or
create/drop destination tables. The worker must atomically persist the returned
catalog additions before ordinary `sync` performs their initial snapshots. Adding
an empty table is valid: its snapshot is empty and subsequent CDC delivers its
first real data. New initial snapshots delay whole-job CDC; retain sufficient WAL.

Pass `--config`, `--destination`, `--enrollment-policy`, and either `--streams` or
both `--available-streams` and `--selected-streams`. Policy files contain the policy
object directly, optionally augmented with `target_query_engines`. For example:

```json
{"enabled":true,"schemas":[{"namespace":"public","destination_database":"enrollment_smoke"}],"defaults":{"normalization":true,"append_mode":false,"update_type":"pos","sync_new_columns":false},"target_query_engines":["athena"]}
```

All defaults must be explicit. Only approved schemas already present in the saved
available catalog qualify; newly created schemas never enroll automatically. A
known available table remains excluded when unselected. Existing stream definitions,
selection overrides, filters, and unknown metadata are retained. Missing/null legacy
selection means all saved tables were implicitly selected: the proposal materializes
their old effective defaults, retaining null/all-future selected columns, before
adding new selections. Explicit empty selections stay empty except for approved new
tables. Unsupported candidates appear in `skipped` with reasons and are not added
to available streams, allowing a later repaired PK/publication to be retried.

Destination mappings are stored exactly; the ordinary writer resolves database names
through `ConfiguredStream.GetDestinationDatabase` (`utils.Reformat`). For example,
`litellm:public` is the single actual namespace `litellm_public`, not a namespace path.
New table names use `GenerateDestinationDetails`. Collisions are checked against all
saved destinations (including unselected tables) and all new candidates. Native S3
Tables names are validated against the actual resolved namespace/table name. For
native S3 Tables, every otherwise eligible candidate is also checked with the AWS
SDK's read-only `GetTable` API using its warehouse table-bucket ARN and effective
namespace/table name. Only a 404 permits enrollment: an existing table, access denial,
credential failure, or any other lookup error skips the candidate visibly. The normal
AWS credential chain supports IRSA; destination profile/static/session credentials
are also honored. Grant `s3tables:GetTable` for this read-only check. No JVM or table
metadata/data files are read. Native detection uses the catalog/service configuration
or warehouse ARN, including non-default AWS partitions. Generic non-S3-Tables
Iceberg catalogs retain an explicit job-reserved destination-namespace prerequisite:
their remote out-of-job tables are not inventoried. Delete-format/read compatibility
is validated before proposing any stream.

### Isolated command smoke

Use an isolated PostgreSQL database with logical WAL, read/replication privileges,
an existing `pgoutput` slot and publication. Do not point these checks at production.
The source account can be read-only apart from its already configured replication
capability. Prepare saved definitions for selected `public.existing_kept` and
unselected `public.intentionally_excluded`, plus source tables `public.new_empty`
(empty with PK), `public.new_filled` (with PK and rows), `public.no_primary_key`,
`public.new_unpublished` (outside the publication), `public._invalid_name`, and
`outside_scope.new_not_approved`. The last schema must not be approved by policy.

For the isolated `enrollment_smoke` fixture, source configuration is:

```json
{"host":"127.0.0.1","port":5432,"database":"enrollment_smoke","username":"olake_smoke","password":"","ssl":{"mode":"disable"},"schemas":["public","outside_scope"],"update_method":{"replication_slot":"olake_smoke_slot","publication":"olake_smoke_pub","initial_wait_time":120}}
```

The native destination below requires a local metadata fixture for an AWS-free smoke:

```json
{"type":"ICEBERG","writer":{"catalog_type":"s3tables","iceberg_s3_path":"arn:aws:s3tables:us-east-1:000000000000:bucket/enrollment-smoke"}}
```

Run a local HTTP fixture reachable from the connector at `http://127.0.0.1:18764`
(inside the same network namespace when running in Docker). It must accept only
`GET /get-table` with `tableBucketARN`, `namespace`, and `name` query parameters.
For absent tables return status 404, `Content-Type: application/json`, and
`{"__type":"NotFoundException","message":"not found"}`. Set the standard AWS SDK
endpoint override and inert signing credentials in that isolated connector process:

```sh
export AWS_ENDPOINT_URL_S3TABLES=http://127.0.0.1:18764
export AWS_ACCESS_KEY_ID=fixture-access AWS_SECRET_ACCESS_KEY=fixture-secret AWS_SESSION_TOKEN=fixture-session
```

Returning 200 for a candidate must produce an existing-table skip; returning 403
must produce an access-denied skip. Neither may add the candidate to a catalog.
`TestNativeEnrollmentDestinationHTTPBoundary` runs these real SDK/HTTP boundary
checks with `httptest` and an injected SDK endpoint, with no external AWS requests.
Alternatively, a generic REST fixture writer (`catalog_type: rest`,
`rest_catalog_url: http://iceberg-fixture.invalid`,
`iceberg_s3_path: s3://enrollment-smoke/warehouse`) makes no destination calls but
requires its namespace to be reserved. That generic smoke also permits leading
underscores such as `_invalid_name`, which only native S3 Tables forbids.

With fixture files mounted at `/mnt/config`, run the connector binary:

```sh
olake discover-new-tables --config /mnt/config/source.json --destination /mnt/config/destination.json --streams /mnt/config/streams.json --enrollment-policy /mnt/config/enrollment-policy.json --enrollment-result /mnt/config/enrollment-result.json
```

Expect enrolled identities `public.new_empty` and `public.new_filled`, explicit skip
reasons for missing PK/publication and invalid S3 name, no schema fetch for known or
unapproved tables, and no catalog input/state changes. The result embeds the complete
resulting catalog as JSON strings in `streams_config`, or in both
`available_streams_config` and `selected_streams_config` for split input; the other
format's fields are empty. Persist those returned strings and repeat the command:
the enrolled list must now be empty and returned catalogs unchanged. Disabled policy
also returns unchanged catalogs without setting up a source connection. A canceled
or failed scan never publishes a partial result. Use an isolated per-run config
directory so a caller cannot consume a stale result from an earlier attempt.

`OLAKE_S3_CONFIG_FOLDER` and `OLAKE_STORAGE_MODE=s3` use the existing storage routing.
An S3 result is an output, not downloaded as an input; only `enrollment-result.json`
is uploaded, never saved input catalogs/state. The output URI may be explicit.

After integration, run `go test ./protocol ./types ./utils` at repository root and
`go test ./internal` in `drivers/postgres`. Build the CLI there with
`CGO_ENABLED=0 go build -o /tmp/olake-postgres .` for the isolated smoke. No JVM or
index PVC is required. Native S3 Tables requires read-only metadata credentials;
generic reserved-namespace smoke does not contact a destination.

---

## ❄️ Social Winter of Code (SWoC 2026) @OLake

OLake is officially open for **Social Winter of Code 2026 contributions**! 🚀

If you’re participating in SWoC 2026, look out for issues labeled:

* **`SWoC26`**
* **`beginner`**
* **`intermediate`**
* **`advanced`**

These labels help you choose issues based on your experience level.
We welcome all kinds of contributions — bug fixes, documentation improvements, tests, and feature enhancements.

👉 [Check our open issues here](../../issues)

---
Let’s hack, learn, and grow together this Hacktoberfest. Happy contributing & happy engineering! ⚡ 
---

## Getting Help

For any questions, concerns, or queries:
- Start a discussion on our [**Slack**](https://olake.io/slack/) channel.  
- Check [GitHub Discussions](https://github.com/datazip-inc/olake/discussions) for ongoing conversations.  
- Don’t hesitate to open an issue if something is unclear.

---

### 💡 We look forward to your feedback and contributions to improve this project!
<!----variables----> 
[CLA]: https://docs.google.com/forms/d/e/1FAIpQLSdze2q6gn81fmbIp2bW5cIpAXcpv7Y5OQjQyXflNvoYWiO4OQ/viewform
