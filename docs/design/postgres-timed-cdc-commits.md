# PostgreSQL time-based CDC commit triage

Scope: PostgreSQL source-TOAST/native Iceberg. COW reuses the existing end-of-run lifecycle; the time-based checkpoint sections below are design assessment, not implemented periodic checkpointing.

## Current behavior

- `waljs.NewReplicator` captures `IDENTIFY_SYSTEM.XLogPos` once as the run target. `pgoutput.StreamChanges` returns after reaching it outside an open transaction.
- Rows flow through source TOAST buffering and asynchronous `WriterThread.Push`/flush. A "successfully wrote" batch log is not proof of Iceberg snapshot publication.
- `AbstractDriver.streamChanges` returns a single final WAL state. Its deferred `handleWriterCleanup` joins all writers, closing them with that state. Only afterwards does `PostCDC` acknowledge the slot and update in-memory global state. Protocol/worker then persist the run state.
- MOR `nativeWriter.Close` is terminal: completes files, writes same-write superseded-row position deletes (v2) or deletion vectors (v3), and publishes them with the original immutable data files and `olake_2pc` in one table transaction. It does not reread/rewrite staged Parquet data or remap its ordinals. It cannot just be called every minute while Push continues.
- After queued writes drain, `WriterThread.Close` prepares native backfill files before acquiring the per-stream close lock. This completes Parquet footers and uploads without publishing table metadata, visible rows, recovery state or index checkpoints. Independent chunks can finalize concurrently; the existing serialized `Close` still publishes data and recovery metadata together under table coordination. Preparation failure cancels the writer and follows abort/rollback cleanup, never publication. Direct native `Close` retains file finalization for callers without the preparation phase; commit, checkpoint and source-acknowledgement ordering are unchanged.
- Keepalive replies send the original confirmed position. The new 30-second progress logs observe decoder positions only; they do not introduce durable checkpointing.
- Destination buffers independently track original source bytes and approximate decoded payload bytes. Hydrated values count toward the payload budget without inflating WAL-byte statistics; indivisible oversized rows flush alone. The source hydrator still materializes up to 128 fetched rows before emission, so this is not a strict process-memory cap.
- Native Parquet writes check page/dictionary targets after each value, including backfill and delete-file writes. The 1 MiB targets remain soft: a single large value can exceed the unchanged reader limits. Per-value checks trade throughput for bounded multi-value page overshoot.
- Ordinary CDC completion closes its replication connection even after cancellation or fence rejection, using a bounded uncanceled teardown context without authorizing acknowledgement. Resync retains separate connection ownership.
- `iceberg_equality_delete` logs actual nonempty equality-delete file usage: `staged` records attempt-local files before commit, `commit_confirmed` records successful public table publication, and `private_resync_commit` records only a private resync-stage commit. Events include table/thread, attempt, snapshot, distinct key count and delete/data file counts, never key values. These are per-table events, not global WAL acknowledgement; retries can emit multiple staging events.
- Non-FULL replica identity emits INFO when the decoder marks omitted TOAST values for configured recovery, not a data-loss warning. Without recovery it emits WARN that affected updates fail; the decoder's fail-closed behavior is unchanged. Relation metadata does not prove an individual value was omitted or successfully recovered.
- PostgreSQL `varchar[]` (OID 1015) uses the pgx array codec for text/binary CDC tuples and maps to the array conversion path, preserving escaped strings, empty arrays, NULL elements and SQL NULL arrays instead of treating the array literal as a generic string.
- `third_party/iceberg-go` contains the production sources and focused RowDelta tests from Apache Iceberg Go v0.7.0, with LICENSE/NOTICE retained. Its scoped RowDelta patch permits explicit delete references to genuinely new files added in that same delta; it rejects duplicate/colliding paths and duplicate DVs and retains existence checks for committed targets, isolation checks, and snapshot/shape fences. Root and standalone driver modules use this local replacement. Remove the replacement only after an upstream release provides equivalent validated behavior.

## Retained ordinary-sync reconciliation

The worker/UI Reconcile action resets the same retained operation, not a new data sync. A recovery epoch first proves the old Temporal run terminal, seals admission, closes its positive fence, and retires the exact provider resource. Original `execution-result.json` evidence is never rewritten. Controlled PostgreSQL-to-native-REST ordinary CDC recovery requires a frozen CDC/strict-CDC catalog, primary keys, complete GLOBAL membership, and existing table checkpoints. Interrupted initial backfill has the additional durable-evidence requirements below; unsupported or inconsistent evidence stays locked.

The connector requires paired `--ordinary-recovery-origin` and `--ordinary-recovery-result` flags plus a fresh controlled execution identity/fence and the retained launch identity. Origin hashes bind the source/destination files, legacy-or-split catalog, and an immutable baseline checkpoint. Backfill origins additionally bind the frozen provisional candidate and the authoritative saved job-state digest separately. If an original execution receipt was observed when the origin was frozen, its exact bytes remain hash-bound; otherwise its digest is empty and later receipt publication does not change that origin. Source/destination files use raw SHA256. Catalog hashing frames each present file in fixed `streams.json`, `available_streams.json`, `selected_streams.json` order as filename, NUL, decimal byte length, NUL, raw bytes; mixed/partial catalog forms are rejected. Checkpoint canonicalization preserves large JSON numbers.

S3 recovery initializes storage before reading its positive execution fence. The normal storage resolver validates that fence before downloading inputs or starting source/destination work. After exact old-runtime retirement, missing helper receipts or server-pin files do not prevent a fresh catalog-based recovery epoch. They do not prove settlement either: ownership remains retained until the new runtime confirms the common committed checkpoint and publishes complete settlement evidence.

Live PostgreSQL server/slot identity is validated before destination mutations. Any existing standalone server pin remains binding. The connector preflights every selected table's committed checkpoint and recovery marker before writing any new barrier; prior markers must bind the same origin, original launch, source identity, stream, table UUID and frozen shape. Each fresh empty-append barrier commits that source identity in table metadata, guarded by UUID, current snapshot and default shape. The companion server-pin file is published after all barriers succeed and before CDC replay. Interrupted barrier preparation therefore recovers identity from committed catalog markers without relying on a helper receipt. Old ordinary artifacts do not prove historical server identity; the first recovery binding is to the validated live source. Barriers preserve data/deletes/history while making late old snapshot-fenced mutations conflict; a property-only marker is not a barrier.

The CDC-only recovery target is the deterministic maximum authoritative selected-table checkpoint, never a log-derived LSN. Missing or below-baseline table metadata fails closed. Tables behind that boundary replay only their own missing transaction prefix; completed writers publish a common checkpoint. Logical keepalives are not decoded transactions, crossing/future transactions are withheld, and slot/server/WAL-floor validation precedes replay. Existing source TOAST hydration retains latest-state semantics: values fetched from the current source can be newer than the bounded WAL target; missing/deleted/rekeyed rows still fail closed.

Recovery is not unconditional replay of the last batch. Iceberg does not enforce primary-key uniqueness, and replaying older changes over a table already ahead can duplicate or regress rows unless the full upsert/delete sequence completes. Per-table committed CDC positions determine which transaction prefixes are skipped. Source WAL must cover every missing range, and the final boundary must be common to every selected table before acknowledgement. No table or checkpoint is reset merely because a prior client lost its commit response.

Published common-checkpoint bytes bind both recovery proof and final runtime receipt; finalization hashes those persisted bytes instead of reserializing an unordered in-memory stream set. Recovery preserves OLake's existing soft-delete representation: deleted keys retain `_op_type = 'd'` tombstones, rather than being resurrected by older replayed changes.

Acknowledgement follows writer drain and authoritative common-boundary confirmation. A separate identity/hash-bound recovery result and a new settled runtime receipt authorize the worker's atomic checkpoint-plus-settlement transaction. The schedule remains paused until explicit resume. If acknowledgement or artifact publication is interrupted, ownership remains retained; a fresh recovery epoch restores the immutable baseline, rechecks table/slot coverage, and rebarriers before recovery. Relaunching the failed runtime directly is unsupported. Never force an unknown receipt to settled, clear owner fields, or advance a slot based only on process exit or logs.

### Interrupted initial backfill

Eligible controlled PostgreSQL CDC runs write `backfill-recovery-checkpoint.json` separately from authoritative `state.json`. The candidate binds the original runtime/launch, frozen configuration hashes, live PostgreSQL system identity and replication slot, original GLOBAL LSN, completed membership, table UUID/shape bindings, and exact immutable chunk plans. Fresh empty PostgreSQL state is initialized as GLOBAL; meaningful legacy state is not reinterpreted. A plan must be durable before its data writers start. Only pending chunks shrink; `backfill_plan` retains the full original ranges after completion. Transient unbound preparation state cannot replace valid durable evidence.

Table creation uses the ordinary REST operation with an origin-bound intent. Before data work, a create-only `backfill-recovery-binding-<sha256(stream-id)>.json` journal records the actual UUID, shape and full plan. Journals survive table deletion, bind to the original operation rather than an individual recovery epoch, and detect dropped/replaced tables. An interrupted create without a journal can adopt only its matching, pristine, expected-shape table. Native S3 Tables does not support `stage-create`; recovery does not require it. See the [AWS REST limitations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html).

After exact runtime retirement, the worker freezes validated candidate bytes create-only in the stable operation directory before publishing baseline/origin. Later uploads to the original runtime candidate cannot change that frozen evidence. The worker promotes immutable binding journals from each retired epoch and copies them byte-identically into its successor. Missing, foreign, changed or inconsistent evidence never clears ownership.

Recovery validates source/slot/WAL coverage, resumes exact plans, and consults authoritative committed chunk IDs rather than trusting pending flags. Committed tails are skipped: CDC from the preserved lower LSN covers growth. Successful empty chunks retain commit IDs without inventing data rows. After backfill, a temporary logical replication slot captures a transaction-consistent replay boundary; this requires one spare logical slot/WAL sender and existing replication privileges. Destination barriers persist that boundary before replay, and successors reuse it instead of widening to later source changes. All selected tables must reach the common target before source acknowledgement and atomic checkpoint/ownership settlement.

Local checkpoint updates are provisional. Authoritative S3 state and the worker DB checkpoint remain settlement-gated. Healthy completed legacy CDC/enrollment runs do not acquire unsupported backfill evidence requirements. Historical `{}` state with no original candidate cannot be fabricated into a resumable backfill: retain the operation and partial destination, restore valid evidence from backup if available, or explicitly plan an isolated fresh backfill.

S3 object uploads remain positively fenced and drain-tracked, but their transport failures do not by themselves imply catalog publication. Existing bounded SDK retries can recover a transient upload failure. REST/Glue catalog mutations and catalog-level ambiguous failures retain the execution-wide unknown-outcome guard; this is not permission to retry ambiguous table commits.


## Index-free copy-on-write writer

Selected-stream `merge_strategy: "copy-on-write"` uses the SDK's stock filtered
`Transaction.Overwrite`, matching OLake's existing canonical string `_olake_id`.
MOR remains explicit; omitted strategy preserves saved/CLI behavior. New eligible
PostgreSQL/native normalized upsert selections in the UI explicitly default to COW.
Append ignores the saved policy. Matching connector/UI builds are required.

For composite keys, the existing `_olake_id` hashes source values joined by `|`.
This encoding is not collision-proof: values containing that delimiter can form
the same encoded input. COW inherits this MOR identity limitation; validate the
source keys before rollout. Iceberg does not enforce primary-key uniqueness.

Automatic enrollment has its own optional `defaults.merge_strategy`. When set
to COW, only newly discovered eligible tables are enrolled in COW; tables without
a source primary key are skipped with a reason. Saved tables and exclusions are
unchanged. Omitted strategy retains MOR. This setting requires the matching
connector, worker and UI; it does not migrate existing tables or delete files.

The existing terminal `nativeWriter.Close` transaction publishes data and
`olake_2pc` together, using its existing snapshot/shape fences, conflict refresh
and unknown-outcome protocol. Initial snapshots keep the existing append/chunk
path. COW adds no source transaction controls, periodic checkpoints, mid-run state
publication or recovery lifecycle. Quiet tables use existing metadata-only commits.

Each existing WriterThread buffer is deduplicated by canonical ID and staged as
typed Arrow IPC; segments replay in source order, so later versions win. Private
0600 files under 0700 directories may contain sensitive financial/TOAST values;
they bound incoming memory and permit recreating consumed readers on commit retry.
Unknown publication retains replay input. COW never opens a persistent row index;
destination TOAST/index recovery is unsupported, so use source TOAST hydration.

Existing live equality/position/Puffin deletes fail before mutation: clean them
through a separate explicit maintenance rewrite before switching. This writer
does not migrate existing tables, reset checkpoints/slots or reread the source.
New COW tables use v2; existing v3, UUID, schema and sort/partition metadata remain
unchanged. Source deletes retain the existing OLake tombstone representation.

COW eligibility reads the current snapshot's manifest list and opens only delete
manifests to inspect live entries. Data manifests are not downloaded or decoded
for this check. Retired delete entries do not block COW; unreadable delete
manifests and unknown manifest content fail closed. Writer setup and commit-time
validation use the same check, preserving concurrent-delete detection and the
existing snapshot/shape fences without scanning every data-file entry per chunk.


No SDK changes are required beyond the base's existing patches. Overwrite output
paths are not enumerated or deleted by OLake; orphan maintenance owns rejected
attempt artifacts. Incoming buffer limits do not cap SDK survivor-read/write
memory or guarantee latency. Pilot representative updates and reader versions; no
production integration or performance result is claimed.

## Native Parquet Bloom filter defaults

Native Iceberg writes default Bloom filters on for the canonical `_olake_id`,
existing scalar destination fields named by the source primary key (including
every composite-key component), and declared Iceberg identifier fields. Source
names use the stream's destination-column resolution, including preserved source
names when configured. Missing fields and struct/list/map fields receive no
implicit filter; this is not an all-columns policy or a schema change.

The standard table property
`write.parquet.bloom-filter-enabled.column.<destination-column>` always overrides
the default when present: `false` disables a key's filter, while `true` can enable
one for a non-key column. Boolean values follow Iceberg's case-insensitive Java
`true` semantics. `write.parquet.bloom-filter-max-bytes` defaults to 1 MiB per
column per row group; explicit sizes must be decimal integers from 32 through 134217728
bytes. Invalid sizes return an error before constructing Arrow writer properties,
rather than panicking or silently replacing the configured bound. Native writes
retain batch size 1, 1 MiB page/dictionary targets, the 64K row-group cap, Zstd
level 3, statistics and page indexes.

New tables persist these defaults at creation. Existing tables get missing
properties only in their next normal data transaction, alongside data and
`olake_2pc`; opening a writer or making a quiet metadata-only checkpoint does not
issue a filter-enablement commit. Each conflict attempt resolves defaults against
fresh metadata without mutating its borrowed properties. Defaults are effective
for direct and streaming-backfill output before that metadata is published. COW
stages them in the transaction before the SDK's `Overwrite` builds writers, so
both incoming data and naturally rewritten survivors use the same column policy.
No existing files are rewritten merely to enable filters; old files keep their
original filters until otherwise rewritten. Bloom filters can help compatible
readers prune equality lookups, but do not change row identity, deletes, source
hydration, index handling, checkpoints, retries or MOR/COW selection.

## Creation-time table sort policy

An optional catalog-wide sort default and explicit per-table overrides configure
new normalized native Iceberg tables. The UI's opt-in `created_at ASC NULLS LAST`
default skips streams without a compatible selected field. A missing per-stream
override inherits that policy; an empty override explicitly requests unsorted
tables. Custom ordered keys must identify distinct supported selected fields.

The connector resolves source names to actual Iceberg schema field IDs and sets
sort metadata at table creation. Existing tables retain their sort order even when
the requested policy differs; changing the policy does not trigger clear, resync,
source backfill or an existing-table migration. Sort-only policy changes therefore
do not count as destructive catalog differences. Existing recovery shape checks
remain intact, including their binding to actual sort metadata.

This policy does not claim incoming Parquet files are physically sorted. Files
written without sorting remain marked unsorted; S3 Tables auto/sort compaction can
apply the declared table order later. Previously compacted history may require a
separate rewrite. A new job pointing to an existing table keeps that table's layout;
use a distinct namespace when a separate physical dataset is intended.

## Recommended first implementation: bounded microbatches

Start with a configurable soft interval (for example 60–300 seconds), off by default until failure tests pass. Stop admitting the next transaction after the interval expires, finish the current transaction, flush all TOAST rows, drain/join destination writes, close/commit the batch, checkpoint/ack safely, then reopen writers and replication from the confirmed batch position within the same worker execution. Keep the original run target fixed so scheduling semantics do not change. A transaction or destination commit can exceed the interval: never cut one to meet a wall-clock deadline.

This reuses terminal Close instead of introducing a new concurrent FlushAndCommit API. Costs: repeated replication startup, relation/schema initialization, writer setup, catalog reads and table-index reconciliation. Equality mode avoids physical-index rebuilds but not catalog overhead. Commit all selected tables initially for a uniform boundary; optimize to touched tables only once per-table high-watermark recovery exists. With ~1,081 tables, even a one-minute interval could mean ~64,860 table metadata commits/hour; commit duration can dominate. No performance benchmark has established the best interval.

Timer design: record a pending checkpoint request, stop at the next COMMIT. Wake a blocked receive with a separate short polling deadline while preserving the configured no-data/initial wait deadline. If there is no new completed transaction, no-op the checkpoint. Do not cancel the entire execution or treat a network timeout as a successful batch end. Source hydration, schema notifications and cancellation must retain their fail-closed ordering.

## Required correctness work (not optional)

1. **Define a source boundary event.** The current callback carries rows/schema, not transaction begin/commit or checkpoint barriers. Expose an explicit complete-transaction/batch result with a known processed LSN. `decoded_commit_lsn` in diagnostics is not itself safe: TOAST and writer queues can still contain rows.
2. **Do not use advertised WAL end as a commit boundary.** Current pgoutput keepalives assign `ServerWALEnd` to `ClientXLogPos`; isolate server WAL end from actually decoded/processed transaction positions. Review termination and sparse-publication behavior with live PostgreSQL tests before reusing that field in timers. Progress logs intentionally keep those values separate without changing current replication behavior.
3. **Barrier before durability.** Finish hydration, stop the producer, join all writes, publish destination data and each table's recovery LSN, and validate success for the entire boundary. Never ack on read, batch upload, timer firing, or one table finishing.
4. **Partial multi-table commits.** Iceberg offers table-level, not cross-table atomic commits. Current ordinary recovery picks one ahead metadata LSN while iterating a Go map and skips ahead tables as a group. Multiple ahead positions can therefore not be assumed safe for rolling checkpoints. Generalize deterministic per-table high-watermarks/replay filtering (or a durable batch manifest) before enabling them. Existing resync has per-table replay boundaries but is a different staged-publication lifecycle, not a drop-in solution.
5. **Durable handoff.** Persist the full batch checkpoint and completion markers to the execution's authoritative artifact store, with checksum/identity/fencing, before acknowledging newer WAL where possible. Recovery must handle both destination-ahead-of-state and state-ahead-of-slot windows. Existing normal validation expects exact slot/state equality, and PostCDC currently acks before state durability; changing just the ordering creates a different mismatch window. Resolve that protocol explicitly. A worker DB write only at final retirement is insufficient for mid-run crash recovery unless startup selects the latest verified artifact.
6. **Atomic artifacts and cancellation.** Use atomic local replacement plus verified remote publication. Preserve the most recent committed boundary on cancellation; never let cleanup replace it with the initial `{}` input or a partially written candidate. Fix/verify the historical empty-state handoff separately.
7. **Same worker execution.** Distinguish committing a microbatch from releasing the job owner, closing source execution fences, or publishing terminal retirement receipts. Those happen only when the whole worker run settles. Retain resync-specific acknowledgement/publication restrictions.
8. **Writer restart.** Reset per-batch counters, buffered files, same-key overlay, pending equality/position deletes and source hydration state; keep table UUID, generation, filters, dedup-insert overlap state and configured schema behavior correct. Ensure quiet tables and explicit source NULLs are handled.

## Delivery slices

- A: periodic progress instrumentation (implemented separately, no checkpoint semantics change).
- B: transaction barrier + soft batch deadline + bounded receive wakeup tests; no periodic ack.
- C: destination drain/close/reopen and per-table partial-commit recovery, uniform logical boundary tests.
- D: atomic verified mid-run artifact + slot/state reconciliation + worker retry/cleanup semantics.
- E: opt-in end-to-end failure injection and performance trials; only then rollout/config defaults.

This is a connector + destination + worker lifecycle change, not a logging/config-only patch. A continuously reusable destination Commit API could reduce reopen overhead later, but is a larger first implementation.

## Acceptance tests

- Real pgoutput: interval between transactions, inside a long transaction, sparse publication/no relevant rows, idle source, blocked hydration/writer, reconnect and bounded original run target.
- Crash at every boundary: before/after table N commit, remote state publication, slot ack, worker persistence; retry must neither skip changes nor lose committed progress.
- Multiple tables with different ahead LSNs, unchanged/quiet tables, conflicting compaction, same-PK repeated updates, equality v2/v3 plus historical DVs, explicit NULL vs missing TOAST.
- Cancellation, unknown remote outcome, stale worker ownership/fences, and atomic resync coexistence.
- Measure commit latency, snapshots/hour, catalog throttling, small-file count, repeat setup/index time, throughput and actual replay window. WAL address-byte lag is not selected-row or wall-clock progress.
