# PostgreSQL time-based CDC commit triage

Scope: source-based TOAST/equality branch. This is a design assessment, not implemented periodic checkpointing.

## Current behavior

- `waljs.NewReplicator` captures `IDENTIFY_SYSTEM.XLogPos` once as the run target. `pgoutput.StreamChanges` returns after reaching it outside an open transaction.
- Rows flow through source TOAST buffering and asynchronous `WriterThread.Push`/flush. A "successfully wrote" batch log is not proof of Iceberg snapshot publication.
- `AbstractDriver.streamChanges` returns a single final WAL state. Its deferred `handleWriterCleanup` joins all writers, closing them with that state. Only afterwards does `PostCDC` acknowledge the slot and update in-memory global state. Protocol/worker then persist the run state.
- `nativeWriter.Close` is terminal: completes files, reconciles staged superseded rows, commits an Iceberg transaction with `olake_2pc`, updates index/coordinator state, and releases resources. It cannot just be called every minute while Push continues.
- Keepalive replies send the original confirmed position. The new 30-second progress logs observe decoder positions only; they do not introduce durable checkpointing.
- Destination buffers independently track original source bytes and approximate decoded payload bytes. Hydrated values count toward the payload budget without inflating WAL-byte statistics; indivisible oversized rows flush alone. The source hydrator still materializes up to 128 fetched rows before emission, so this is not a strict process-memory cap.
- Native Parquet writes check page/dictionary targets after each value, including staged rewrites and backfill. The 1 MiB targets remain soft: a single large value can exceed the unchanged reader limits. Per-value checks trade throughput for bounded multi-value page overshoot.
- Ordinary CDC completion closes its replication connection even after cancellation or fence rejection, using a bounded uncanceled teardown context without authorizing acknowledgement. Resync retains separate connection ownership.
- `iceberg_equality_delete` logs actual nonempty equality-delete file usage: `staged` records attempt-local files before commit, `commit_confirmed` records successful public table publication, and `private_resync_commit` records only a private resync-stage commit. Events include table/thread, attempt, snapshot, distinct key count and delete/data file counts, never key values. These are per-table events, not global WAL acknowledgement; retries can emit multiple staging events.

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
