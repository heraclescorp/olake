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

## Atomic single-table resync

Use `resync` with the complete saved source, destination, catalog, and state inputs used by `sync`, plus `--resync-namespace <source namespace> --resync-table <source table> --resync-id <stable operation ID>`. The worker supplies the runtime fence and admitted launch identity described below; never overlap resync with sync or Clear Destination. Explicit standalone CLI usage without runtime flags remains unfenced and requires operator-enforced exclusive ownership. The target must already exist and be selected in the saved catalog. Clear Destination remains destructive and is not a resync fallback.

The original Iceberg table UUID, name, and location remain unchanged. Readers keep the old complete snapshot while the replacement is built; one catalog commit publishes the completed replacement. Downstream readers see it after their normal refresh. Unsupported schema transitions or query-engine limitations are not bypassed by resync.

Each fresh admitted attempt is limited to 12 hours, measured before staging starts and checked again before publication. This is below the minimum one-day S3 Tables unreferenced-file retention interval. An expired attempt that never dispatched publication leaves the old snapshot and authoritative checkpoint intact. Never interpret a publication error, including a final REST HTTP 409 after an earlier retry, as proof that no catalog request remains in flight. A fresh generation is permitted only after the worker has sealed admission and confirmed retirement of the previous launch; recovery never extends an old attempt's retention window.

PostgreSQL CDC targets use existing replication privileges and one spare replication slot to establish a temporary logical slot's exported snapshot and consistent WAL boundary. Full-refresh and incremental targets use a shared repeatable-read transaction's exported snapshot. All target chunks import that snapshot from the writer database, including when a separate backfill host is configured; unrelated streams retain their normal read paths. Target CDC skips only transactions already included at the snapshot boundary and catches up before publication. The full saved CDC stream set is retained so a shared replication acknowledgement cannot skip unrelated tables. Unsupported source implementations fail closed.

When a resync includes CDC streams, their saved global replication checkpoint must exist. Resync refuses to initialize or advance a missing main-slot checkpoint before publication; runs without CDC streams do not need a global CDC checkpoint.

The `olake_2pc` and resync metadata properties are connector-owned. Do not edit them directly or configure independent jobs to own the same destination table. Catalog publication checks the original identity, main reference, schema, partitioning, and sort order; harmless concurrent maintenance properties need not prevent publication.

Successful execution emits `state.json` and then `resync-result.json`, binding the stable `resync_id`, target, unchanged table UUID, committed snapshot, and SHA-256 of canonical final state JSON. Controlled success and failure receipts also bind `execution_id` (this runtime) and `launch_id` (the original admitted data launch). Workers must validate both identities before accepting evidence or persisting state. A serialized, confirmed abort may emit `resync-failure.json` with `publication: "not_published"`; missing or other evidence means publication is unresolved. Runtime-specific artifact directories prevent an old process from overwriting a successor's proof. A dispatched publication exception is reconciled through the durable marker or remains UNKNOWN, never converted into unpublished evidence solely by its final HTTP status.

The durable completion marker binds the stable operation ID, admitted `olake.resync.launch-id`, and original full input checkpoint. Recovery accepts only that original checkpoint or the exact published checkpoint, comparing checkpoint contents rather than cursor ordering. Different or newer target or unrelated stream state fails without restoring historical state. To reconcile an uncertain launch, the worker retires its mutating runtime and starts a separate runtime with `--resync-recover-only --resync-launch-id <original launch>` and the same `--resync-id`. This mode only reads and validates the durable marker and restores its confirmed checkpoint; PostgreSQL may acknowledge only that committed position and rejects a slot already ahead. It does not run the normal mutating connection test, stage, rebuild indexes, write schema/data, publish, or emit an unpublished proof. Missing markers/tables are UNKNOWN: keep the job paused and ownership retained; retry recovery after the delayed catalog request resolves, never relaunch data blindly.

Controlled asynchronous commands require both `--execution-id <runtime ID>` and `--execution-fence <local path or s3:// URI>`. The worker atomically creates `{"version":1,"execution_id":"<runtime ID>","state":"open"}` only if absent, and positively replaces it with CLOSED before resource retirement; that runtime identity is never reopened. The connector freshly reads this positive fence at startup and mutation boundaries, including destructive drops, JVM catalog commits/publication, and durable source acknowledgement. Missing, unreadable, malformed, wrong-ID, or closed fences reject mutations. Serialized `ABORT_RESYNC` and `RESYNC_STATUS` alone may use a valid matching CLOSED fence to conclusively retire or reconcile an attempt; they cannot publish or erase prior publication uncertainty, and a missing/wrong fence still rejects them. Controlled resync additionally requires `--resync-launch-id <admitted launch ID>`; `--resync-id` remains the stable workflow ID across RunID changes. Fenced REST runtimes inject a single-attempt client through Iceberg's public REST client factory, disabling automatic retries and redirects for every HTTP call rather than using the unsupported `rest.client.max-retries=0` setting. This prevents a hidden DELETE replay from disguising an earlier ambiguous503 as a final404. An already dispatched remote mutation can still be uncertain and must retain ownership.

Deploy matching worker, connector CLI, and bundled Java writer versions together. Older CLI images reject the new flags before mutation and must not be used as a destructive fallback. The Go launcher also requires a new read-only `EXECUTION_CAPABILITY` response from every fenced or recovery-only JVM before returning its client to any connection-check, drop, or write path. The JVM must confirm its actual startup-configured execution/launch identities, recovery mode, and freshly checked OPEN fence; old/custom jars that ignore startup JSON or reject the new payload fail closed. Both Go and Java need read access to the runtime fence in the worker's files-storage bucket or shared filesystem, independent of the destination warehouse. Java uses files-storage `OLAKE_S3_*` overrides or the captured runtime AWS/IRSA authority, not catalog-vended table-only credentials. Runtime config/artifact prefixes must be isolated per runtime. Independent jobs sharing a destination table or replication slot, and standalone source/destination edits during an operation, remain unsupported.

Controlled ordinary `sync` and `clear-destination` executions emit immutable `execution-result.json` with `version: 1`, current `execution_id`, matching ordinary `launch_id`, `command`, and `outcome`. `not_started` proves that no source/destination mutation participant started and preserves the existing database checkpoint. `settled` means every participant positively retired, not that the command succeeded: local data-processing errors may still settle after acknowledged catalog operations and joined source/writer work. It binds `state_sha256` to the complete saved checkpoint, including any acknowledged progress before a local error. `unknown`, missing, or mismatched evidence retains worker ownership. A terminal JVM `EXECUTION_RETIRE` control seals further mutation admission and reports sticky ambiguous/in-flight catalog dispatches across all REST clients/auth sessions; failed SQL source acknowledgements also retain uncertainty. The connector publishes a complete final proof only if absent (atomic local link or S3 conditional put), so a late CLOSED runtime cannot overwrite earlier evidence. Workers validate the proof and state hash only after admission closure, positive CLOSED fence, and exact native-resource termination; neither exit status nor process disappearance is publication evidence.

The worker enables this new controlled-runtime/ordinary-retirement protocol only from canonical saved PostgreSQL + Iceberg REST-family configuration: `rest`, `lakekeeper`, `nessie`, `s3tables`, `unity`, `polaris`, `biglake`, or `horizon`. `s3tables` sets Iceberg SigV4 signing name `s3tables` before normalizing to the same REST transport, so native S3 Tables is included. Other ordinary jobs retain their existing execution lifecycle rather than treating unproven SDK transport outcomes as settled or permanently requiring this new proof. A caller cannot select the domain with a public bypass flag; unsupported resync requests fail before launch.

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
