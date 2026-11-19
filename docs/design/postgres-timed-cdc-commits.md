# PostgreSQL time-based CDC commit triage

Scope: PostgreSQL source-TOAST/native Iceberg. Copy-on-write uses complete-transaction microbatches; existing merge-on-read jobs retain their end-of-run commit lifecycle.

## Current behavior

- `waljs.NewReplicator` captures `IDENTIFY_SYSTEM.XLogPos` once as the run target. `pgoutput.StreamChanges` returns after reaching it outside an open transaction.
- Rows flow through source TOAST buffering and asynchronous `WriterThread.Push`/flush. A "successfully wrote" batch log is not proof of Iceberg snapshot publication.
- Without active COW, `AbstractDriver.streamChanges` returns a single final WAL state. Its deferred `handleWriterCleanup` joins all writers, closing them with that state. Only afterwards does `PostCDC` acknowledge the slot and update in-memory global state. Protocol/worker then persist the run state.
- Merge-on-read `nativeWriter.Close` completes files and publishes configured equality/position/DV deletes with `olake_2pc`. COW instead calls the SDK's exact-key filtered `Transaction.Overwrite`, publishing replacement data files and checkpoint together. Snapshot backfill retains the append-only writer and chunk-completion markers; it does not rewrite prior chunks or create a row index.
- COW distinguishes advertised WAL end from processed complete-COMMIT positions. Progress logs and batch uploads do not authorize acknowledgement. Regular COW microbatches keep the replication socket open; ordinary recovery and atomic resync retain terminal-only acknowledgement.
- Destination buffers independently track original source bytes and approximate decoded payload bytes. Hydrated values count toward the payload budget without inflating WAL-byte statistics; indivisible oversized rows flush alone. The source hydrator still materializes up to 128 fetched rows before emission, so this is not a strict process-memory cap.
- Native Parquet writes check page/dictionary targets after each value, including backfill and delete-file writes. The 1 MiB targets remain soft: a single large value can exceed the unchanged reader limits. Per-value checks trade throughput for bounded multi-value page overshoot.
- Ordinary CDC completion closes its replication connection even after cancellation or fence rejection, using a bounded uncanceled teardown context without authorizing acknowledgement. Resync retains separate connection ownership.
- `iceberg_equality_delete` logs actual nonempty equality-delete file usage: `staged` records attempt-local files before commit, `commit_confirmed` records successful public table publication, and `private_resync_commit` records only a private resync-stage commit. Events include table/thread, attempt, snapshot, distinct key count and delete/data file counts, never key values. These are per-table events, not global WAL acknowledgement; retries can emit multiple staging events.
- Non-FULL replica identity emits INFO when the decoder marks omitted TOAST values for configured recovery, not a data-loss warning. Without recovery it emits WARN that affected updates fail; the decoder's fail-closed behavior is unchanged. Relation metadata does not prove an individual value was omitted or successfully recovered.
- PostgreSQL `varchar[]` (OID 1015) uses the pgx array codec for text/binary CDC tuples and maps to the array conversion path, preserving escaped strings, empty arrays, NULL elements and SQL NULL arrays instead of treating the array literal as a generic string.
- `third_party/iceberg-go` contains Apache Iceberg Go v0.7.0 sources with LICENSE/NOTICE retained. Scoped RowDelta fixes validate same-delta references. COW extensions add staged-file observation, read/output memory controls, and bounded clustered survivor windows after partition-spec evolution; they reuse SDK `Overwrite`, not a separate rewrite engine. Root and standalone drivers use this local replacement until an upstream release supplies equivalent behavior.

## Retained ordinary-sync reconciliation

The worker/UI Reconcile action resets the same retained operation, not a new data sync. A recovery epoch first proves the old Temporal run terminal, seals admission, closes its positive fence, and retires the exact provider resource. Original `execution-result.json` evidence is never rewritten. Only controlled PostgreSQL-to-native-REST ordinary syncs with a frozen all-CDC/strict-CDC catalog, primary keys, complete GLOBAL backfill membership, and existing table checkpoints are supported; unsupported or inconsistent evidence stays locked.

The connector requires paired `--ordinary-recovery-origin` and `--ordinary-recovery-result` flags plus a fresh controlled execution identity/fence and the retained launch identity. Origin hashes bind the source/destination files, legacy-or-split catalog, and an immutable complete baseline checkpoint. If an original execution receipt was observed when the origin was frozen, its exact bytes remain hash-bound; otherwise its digest is empty and later receipt publication does not change that origin. Source/destination files use raw SHA256. Catalog hashing frames each present file in fixed `streams.json`, `available_streams.json`, `selected_streams.json` order as filename, NUL, decimal byte length, NUL, raw bytes; mixed/partial catalog forms are rejected. Checkpoint canonicalization preserves large JSON numbers.

S3 recovery initializes storage before reading its positive execution fence. The normal storage resolver validates that fence before downloading inputs or starting source/destination work. After exact old-runtime retirement, missing helper receipts or server-pin files do not prevent a fresh catalog-based recovery epoch. They do not prove settlement either: ownership remains retained until the new runtime confirms the common committed checkpoint and publishes complete settlement evidence.

Live PostgreSQL server/slot identity is validated before destination mutations. Any existing standalone server pin remains binding. The connector preflights every selected table's committed checkpoint and recovery marker before writing any new barrier; prior markers must bind the same origin, original launch, source identity, stream, table UUID and frozen shape. Each fresh empty-append barrier commits that source identity in table metadata, guarded by UUID, current snapshot and default shape. The companion server-pin file is published after all barriers succeed and before CDC replay. Interrupted barrier preparation therefore recovers identity from committed catalog markers without relying on a helper receipt. Old ordinary artifacts do not prove historical server identity; the first recovery binding is to the validated live source. Barriers preserve data/deletes/history while making late old snapshot-fenced mutations conflict; a property-only marker is not a barrier.

The recovery target is the deterministic maximum authoritative selected-table checkpoint, never a log-derived LSN. Missing or below-baseline table metadata fails closed. Tables behind that boundary replay only their own missing transaction prefix; completed writers publish a common checkpoint. Logical keepalives are not decoded transactions, crossing/future transactions are withheld, and slot/server/WAL-floor validation precedes replay. Existing source TOAST hydration retains latest-state semantics: values fetched from the current source can be newer than the bounded WAL target; missing/deleted/rekeyed rows still fail closed.

Recovery is not unconditional replay of the last batch. Iceberg does not enforce primary-key uniqueness, and replaying older changes over a table already ahead can duplicate or regress rows unless the full upsert/delete sequence completes. Per-table committed CDC positions determine which transaction prefixes are skipped. Source WAL must cover every missing range, and the final boundary must be common to every selected table before acknowledgement. No table or checkpoint is reset merely because a prior client lost its commit response.

Published common-checkpoint bytes bind both recovery proof and final runtime receipt; finalization hashes those persisted bytes instead of reserializing an unordered in-memory stream set. Recovery preserves OLake's existing soft-delete representation: deleted keys retain `_op_type = 'd'` tombstones, rather than being resurrected by older replayed changes.

Acknowledgement follows writer drain and authoritative common-boundary confirmation. A separate identity/hash-bound recovery result and a new settled runtime receipt authorize the worker's atomic checkpoint-plus-settlement transaction. The schedule remains paused until explicit resume. If acknowledgement or artifact publication is interrupted, ownership remains retained; a fresh recovery epoch restores the immutable baseline, rechecks table/slot coverage, and rebarriers before recovery. Relaunching the failed runtime directly is unsupported. Never force an unknown receipt to settled, clear owner fields, or advance a slot based only on process exit or logs.

## Index-free copy-on-write

Selected stream metadata accepts `merge_strategy: "copy-on-write"` or
`"merge-on-read"`. Omission retains MOR for saved/low-level configurations. New
eligible PostgreSQL/native-Iceberg job selections in the UI explicitly select COW;
editing an existing job does not switch its mode. MOR remains available for
write-heavy/scattered updates when consumers support its delete representation.
An Iceberg table property alone cannot change OLake's explicit RowDelta calls.

COW requires normalized CDC/strict-CDC, actual selected nonnull source primary
keys, compatible exact-filter key types, pgoutput, and source TOAST hydration.
Primary-key-changing updates and destination/index TOAST recovery fail closed.
Fresh PostgreSQL discovery advertises `supports_iceberg_copy_on_write`; runtime
validation checks the actual source/configuration, not trust in that saved marker.

The writer deduplicates bounded incoming typed segments and replays them in source
order through filtered SDK Overwrite calls in one fenced catalog transaction.
Composite predicates match exact tuples, including explicit nonnull guards;
unrelated rows, including null-key bystanders, are preserved. Later segments win
over earlier versions. A large transaction can spill without acknowledging any
partial source transaction. Replay uses Arrow IPC in 0600 files under 0700 temporary
directories because payloads can contain sensitive financial/TOAST values. There
is no persistent table-wide key-to-file/position index or index rebuild in COW.

New COW tables use format v2, even if an inactive saved delete preference is DV.
Existing v3 metadata is not downgraded. A clean current snapshot contains data
files only; historical snapshots can retain deletes. OLake source-delete
tombstones (`_op_type = 'd'`) remain ordinary after-images and require the existing
consumer filtering convention. Data-only snapshots do not prove a particular
engine's catalog or table-format compatibility.

Existing live equality, position, or Puffin deletes require explicit
`cow_options.materialize_existing_deletes: true`. Otherwise setup fails before
row writes. Authorized setup reads the complete current logical view with deletes
applied, then uses the SDK's fenced pure-rewrite builder to replace data and retire
all live delete entries. It preserves checkpoint, UUID, schema, sort/partition
metadata and applicable v3 row lineage; it does not reread PostgreSQL, reset the
slot, or physically remove retained history. The committed snapshot is checked
again for remaining live deletes. This permission is not a performance promise.

Known publication conflicts refresh metadata/checkpoint and rebuild the overwrite
from private typed input; stale file replacements are never blindly retried.
Uncertain publication retains both private replay and destination artifacts and
keeps execution ownership unresolved. Authoritative checkpoint/batch identity
recognizes landed work. No cleanup may delete potentially committed files.

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

## Transaction-aligned COW microbatches

`cow_options` defaults are 300 seconds, 100,000 incoming rows, 67,108,864 decoded
payload bytes and rewrite concurrency 2. Interval, row/byte budgets and concurrency
must be positive integers; interval seconds cannot exceed 9,223,372,036. These are
soft limits, not a latency or total-process-memory guarantee. Concatenation can
temporarily retain input/output, hydration buffers independently, and an
indivisible large row can exceed the byte target without truncation.

After the next complete COMMIT and source-hydration barrier, a due batch blocks
the producer, drains/closes every selected writer at the same explicit checkpoint,
including quiet tables, and reopens from current metadata. It atomically writes
the full GLOBAL state with membership using local fsync/rename and, when remote
storage is configured, verifies uploaded state bytes. Only then can the fenced
source acknowledgement advance. Terminal metadata can retire overlap flags even
when its source LSN already committed in a microbatch.

The original finite run target remains fixed. Idle periods do not create empty
timer commits; a transaction or rewrite can exceed the interval. Configure
`wal_sender_timeout` for the longest checkpoint pause (or disable it), since the
interval is not an upper bound on rewrite duration.

All-table boundaries preserve a uniform acknowledged watermark, but Iceberg
publication remains table-atomic, not cross-table atomic. A partial/unknown batch
does not authorize source acknowledgement or owner release. Existing retained
ordinary recovery uses the immutable baseline and deterministic per-table
high-watermarks to complete missing transaction prefixes; bounded recovery and
atomic resync do not emit normal intermediate COW checkpoints.

Every boundary incurs catalog/writer setup overhead, even for quiet tables. Mixed
jobs can still reconcile MOR indexes; COW tables never do. Large transactions
split into replay segments may rewrite a hot file more than once. Smaller files
reduce rewrite granularity but increase metadata/object overhead. No AWS S3 Tables
maintenance settings are changed by selecting COW. Pilot representative update
patterns before broad rollout; no throughput or best-interval benchmark is claimed.

## Verification and rollout limits

Focused local-file/Hadoop-catalog cases exercise exact composite keys above 2^53,
repeated updates, null bystanders, every delete-file kind, same-file compaction
conflicts, lost commit responses, typed decimal/binary/timestamp replay, partition
evolution, backfill chunks, and same-LSN terminal metadata. Source-focused cases
exercise quiet-table/no-ACK failure, transaction controls, source hydration and
complete checkpoint serialization. A local UI control smoke exercises the new
default, permission/options, MOR preference restoration and index derivation.

These are not live PostgreSQL/S3 Tables/Snowflake/ClickHouse integration or
performance results. Real pgoutput, remote-state failure injection, WAL retention,
engine/version compatibility, catalog throttling and rewrite amplification still
require rollout-environment verification. Keep the first pilot table small.
