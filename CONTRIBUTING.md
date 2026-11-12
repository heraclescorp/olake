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

Use `resync` with the complete saved source, destination, catalog, and state inputs used by `sync`, plus `--resync-namespace <source namespace> --resync-table <source table> --resync-id <stable operation ID>`. Run it only under the worker's per-job execution fence; never overlap it with sync or Clear Destination. The target must already exist and be selected in the saved catalog. Clear Destination remains destructive and is not a resync fallback.

The original Iceberg table UUID, name, and location remain unchanged. Readers keep the old complete snapshot while the replacement is built; one catalog commit publishes the completed replacement. Downstream readers see it after their normal refresh. Unsupported schema transitions or query-engine limitations are not bypassed by resync.

Each fresh attempt is limited to 12 hours, measured before staging starts and checked again before publication. This is below the minimum one-day S3 Tables unreferenced-file retention interval. An expired or unpublished failed attempt leaves the old snapshot and authoritative checkpoint intact. A retry writes fresh staged files; it never extends an old attempt's retention window.

PostgreSQL CDC targets use existing replication privileges and one spare replication slot to establish a temporary logical slot's exported snapshot and consistent WAL boundary. Full-refresh and incremental targets use a shared repeatable-read transaction's exported snapshot. All target chunks import that snapshot from the writer database, including when a separate backfill host is configured; unrelated streams retain their normal read paths. Target CDC skips only transactions already included at the snapshot boundary and catches up before publication. The full saved CDC stream set is retained so a shared replication acknowledgement cannot skip unrelated tables. Unsupported source implementations fail closed.

When a resync includes CDC streams, their saved global replication checkpoint must exist. Resync refuses to initialize or advance a missing main-slot checkpoint before publication; runs without CDC streams do not need a global CDC checkpoint.

The `olake_2pc` and resync metadata properties are connector-owned. Do not edit them directly or configure independent jobs to own the same destination table. Catalog publication checks the original identity, main reference, schema, partitioning, and sort order; harmless concurrent maintenance properties need not prevent publication.

Successful execution emits `state.json` and then `resync-result.json`, binding the stable operation ID, target, unchanged table UUID, committed snapshot, and SHA-256 of canonical final state JSON. Workers must validate that receipt before persisting state. A serialized, confirmed abort may emit `resync-failure.json` with `publication: "not_published"`. Before a new attempt can prepare or publish, it durably overwrites any previous failure proof with `publication: "in_progress"`, including the S3 artifact. Only `not_published` proves an unpublished failure; missing or other evidence means publication is unresolved. Keep the job paused and retain its execution fence after an unknown outcome. Reset or retry the same Temporal workflow with the same resync ID to resolve the durable destination marker and recover the committed checkpoint without rebuilding again.

The durable completion marker also binds the operation to its original full input checkpoint. Same-ID recovery accepts only that original checkpoint or the exact published checkpoint, comparing checkpoint contents rather than cursor ordering. Retrying an old operation ID against different or newer target or unrelated stream state fails without restoring historical state; start a new resync request instead.

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
