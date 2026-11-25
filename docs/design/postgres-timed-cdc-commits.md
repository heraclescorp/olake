# PostgreSQL time-based CDC commit triage

Scope: PostgreSQL source-TOAST/native Iceberg. Retained controlled recovery uses bounded incremental microbatches. Ordinary scheduled CDC and COW outside retained recovery continue to use their existing end-of-run lifecycle.

## Current behavior

- `waljs.NewReplicator` captures `IDENTIFY_SYSTEM.XLogPos` once as the run target. `pgoutput.StreamChanges` returns after reaching it outside an open transaction.
- Rows flow through source TOAST buffering and asynchronous `WriterThread.Push`/flush. A "successfully wrote" batch log is not proof of Iceberg snapshot publication.
- `AbstractDriver.streamChanges` returns a single final WAL state. Its deferred `handleWriterCleanup` joins all writers, closing them with that state. Only afterwards does `PostCDC` acknowledge the slot and update in-memory global state. Protocol/worker then persist the run state.
- MOR `nativeWriter.Close` is terminal: completes files, writes same-write superseded-row position deletes (v2) or deletion vectors (v3), and publishes them with the original immutable data files and `olake_2pc` in one table transaction. It does not reread/rewrite staged Parquet data or remap its ordinals. It cannot just be called every minute while Push continues.
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

The CDC-only recovery target is the deterministic maximum authoritative selected-table checkpoint, never a log-derived LSN. Missing or below-baseline table metadata fails closed. Tables behind that fixed final boundary replay only their own missing transaction prefix; ahead table checkpoints are never regressed to an earlier microbatch. Logical keepalives are not decoded transactions, crossing/future transactions are withheld, and slot/server/WAL-floor validation precedes replay. Existing source TOAST hydration retains latest-state semantics: values fetched from the current source can be newer than the bounded WAL target; missing/deleted/rekeyed rows still fail closed.

Recovery is not unconditional replay of the last batch. Iceberg does not enforce primary-key uniqueness, and replaying older changes over a table already ahead can duplicate or regress rows unless the full upsert/delete sequence completes. Per-table committed CDC positions determine which transaction prefixes are skipped. Source WAL must cover every missing range, and the final boundary must be common to every selected table before acknowledgement. No table or checkpoint is reset merely because a prior client lost its commit response.

Published common-checkpoint bytes bind both recovery proof and final runtime receipt; finalization hashes those persisted bytes instead of reserializing an unordered in-memory stream set. Recovery preserves OLake's existing soft-delete representation: deleted keys retain `_op_type = 'd'` tombstones, rather than being resurrected by older replayed changes.

Each microbatch stops at a completed transaction or a validated processed keepalive outside any open transaction, drains TOAST and joins all destination writers, then confirms every selected table covers the batch boundary without advancing beyond the fixed final target. The connector creates and reads back an immutable checkpoint manifest before acknowledging that boundary. Only the final target permits `ordinary-recovery-result.json`, authoritative state publication, a settled runtime receipt and the worker's atomic checkpoint-plus-settlement transaction. Intermediate checkpoints never release ownership, close the execution fence or resume the schedule. A failed acknowledgement or publication leaves ownership retained; the next retired-runtime recovery revalidates source and every table before using durable progress. Relaunching the failed runtime directly is unsupported. Never force an unknown receipt to settled, clear owner fields, or advance a slot based only on process exit or logs.

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

Each WriterThread buffer is staged as typed Arrow IPC. At commit, COW indexes
the latest row per canonical ID across all segments and streams the winners
through one filtered Iceberg overwrite instead of rewriting files per segment.
Recovery carries forward omitted fields without treating explicit nulls as missing.
Staged payloads remain on table storage; the per-attempt index scales with the
number of distinct changed keys and is rebuilt on commit retry.
Unknown publication retains replay input. COW never opens a persistent row index;
destination TOAST/index recovery is unsupported, so use source TOAST hydration.

Overwrite predicates retain an owned, compact copy of each segment's canonical ID
bytes. The vendored SDK also owns string partition values retained in file
metadata, including identity and truncate transforms. The SDK keeps predicates
and file metadata until the table transaction ends; borrowing strings from
released IPC buffers would retain entire message payloads (or read invalid values
with an allocator that reuses released storage). Payload readers are still
released after each segment; this is not a total-memory cap.

COW survivor scans on object storage coalesce nearby Parquet reads into
1 MiB windows (up to four cached per open file, 128 MiB across the process).
Reads spanning a window or exceeding the cache budget use the direct reader.
The cache trades some extra bytes for fewer requests on dense rewrites; it does
not change rewrite selection, normal scans, or checkpoint ordering.

Parquet scan batches are also bounded by decoded binary payload, not only row
count. Dictionary expansion can exceed 2 GiB even for a small Parquet file;
putting that payload in one Arrow string/binary array wraps its 32-bit offsets
before the overwrite filter runs. The vendored reader flushes binary builders
at 64 MiB (an individual larger value remains intact), preserves those chunks
through nested struct/list reconstruction, and emits record batches aligned to
column chunk boundaries. String/binary offset widths, row order, nulls and full
payloads are unchanged. This bounds individual arrays, not all retained scan
memory, and does not add retries or permit recovering from malformed arrays.

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

## Retained recovery microbatches

`sync --recovery-checkpoint-interval` configures a positive soft Go duration, default `5m`; `OLAKE_RECOVERY_CHECKPOINT_INTERVAL` supplies the value when the flag is absent. Empty, zero, negative and malformed values fail rather than disabling checkpoints. This applies only to retained ordinary recovery, including completed interrupted-backfill recovery, not to scheduled ordinary sync or resync. A transaction, hydration or commit may exceed the interval; the timer never cancels the context or splits a transaction.

The source retains the original immutable lower LSN and fixed final target across fresh pools and replication sessions. It chooses a batch boundary only after a complete transaction, or a validated processed keepalive with no open transaction. Advertised server WAL end is not a substitute. Every batch uses terminal writer Close, authoritative per-table metadata confirmation, immutable publication, acknowledgement and bounded replication cleanup before reopening the next pool. Partial commits remain per-table replay authority: all selected tables must cover the batch boundary, and tables already ahead retain their higher checkpoints up to the final target.

Checkpoint outputs are `ordinary-recovery-checkpoints/<executionID>-<LSN-with-slash-replaced-by-dash>.json`. Each embeds the recovery result bindings with `target_lsn` equal to the completed batch, plus `final_target_lsn`, canonical full `state`, and optional paired `previous_checkpoint_sha256`/`previous_execution_id`. The state SHA256 binds the canonical checkpoint; predecessor hashes bind exact immutable manifest bytes, including across retired runtimes. Outputs use positively fenced create-only local publication with file/directory durability or create-only S3 publication, followed by authoritative exact-byte readback before acknowledgement. Logs record persisted checkpoint LSN/time and a completed-transaction source timestamp when available.

The worker preserves immutable outputs from each retired epoch, verifies a complete monotonic chain rooted in the unchanged origin, and promotes the newest proven manifest byte-identically as optional `ordinary-recovery-checkpoint.json` beside the origin in successor inputs. Source/destination/catalog/origin and the original backfill candidate remain frozen; the ordinary baseline state is still verified against its original digest before progress is considered. The connector rejects foreign runtime/origin/source/slot/range/hash or incomplete selected-table proofs, then requires fresh matching table UUIDs, positive fences and nonregressing authoritative table checkpoints before installing manifest state. Full checkpoint state preserves unrelated baseline membership. A saved proven checkpoint can exceed the source's confirmed slot after publish-before-ACK failure; replay starts from the proved checkpoint while the replication connection continues reporting the real confirmed position until the new proof-backed acknowledgement. A final checkpoint is still not final settlement, and may be reproved at the same fixed final target by a successor.

Fresh pools reset terminal writer buffers and barriers rather than introducing concurrent FlushAndCommit. Costs include replication startup, relation/schema initialization, writer setup and catalog reads for every batch. Quiet tables still participate in proof and metadata commits. No performance benchmark has established the best interval; commit duration can dominate for large selected catalogs.

## Correctness invariants

1. A successful batch boundary is a complete source transaction or validated processed keepalive, never a timer cancellation, advertised WAL tip or open transaction.
2. TOAST, producer and writer drain precede all-table confirmation; all-table confirmation precedes immutable checkpoint publication/readback; publication precedes slot acknowledgement.
3. The immutable origin, baseline, candidate, source binding, final target and table UUID/shape bindings remain fixed. Partial-table progress is authoritative only after fresh catalog validation and never regresses an ahead table.
4. Cancellation, ambiguous publication, missing evidence, stale fences and mismatched hashes fail closed. Intermediate state cannot replace original inputs or authorize terminal settlement.
5. Final settlement requires the unchanged final-only recovery result and runtime-receipt contracts. Scheduled CDC and resync retain separate lifecycles.

## Verification scope

Protocol coverage exercises publication before ACK, publication/ACK interruption, immutable predecessor hashes, resumed table-coverage regression rejection, positive interval configuration and final-only artifact publication. PostgreSQL 18 integration covers pgoutput and wal2json transaction cuts and checkpoint/slot recovery windows. The compiled PostgreSQL CLI smoke interrupts an initial backfill, interrupts recovery, then kills a successor after an intermediate checkpoint and confirmed slot acknowledgement; a fresh runtime resumes from the promoted manifest and reaches the unchanged final target in MOR and COW. Worker tests cover retired-epoch promotion and corrupt or incomplete evidence. Representative production-scale performance trials remain necessary: the configured interval is a soft boundary request, not a latency or memory guarantee.

## Acceptance tests

- Real pgoutput: interval between transactions, inside a long transaction, sparse publication/no relevant rows, idle source, blocked hydration/writer, reconnect and bounded original run target.
- Crash at every boundary: before/after table N commit, remote state publication, slot ack, worker persistence; retry must neither skip changes nor lose committed progress.
- Multiple tables with different ahead LSNs, unchanged/quiet tables, conflicting compaction, same-PK repeated updates, equality v2/v3 plus historical DVs, explicit NULL vs missing TOAST.
- Cancellation, unknown remote outcome, stale worker ownership/fences, and atomic resync coexistence.
- Measure commit latency, snapshots/hour, catalog throttling, small-file count, repeat setup/index time, throughput and actual replay window. WAL address-byte lag is not selected-row or wall-clock progress.
