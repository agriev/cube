# M3 — Wire MetaStore writes through Raft apply

Decomposing M3 into 6 sequential sub-milestones because the trait has 86
write methods and each pulls in argument-shape, return-shape, and
determinism considerations. M3.1 lands first and unblocks everything
else.

## Categorization of the 86 write methods

Categorized by argument-shape complexity (drives encode/decode work):

### Cat A: Single-id ops (~25 methods, simplest)
Shape: `(id: u64) -> Result<IdRow<T>, CubeError>` or `Result<(), CubeError>`.

Examples:
- `drop_table(table_id)`, `seal_table(id)`, `delete_partition(partition_id)`
- `mark_partition_warmed_up`, `delete_chunk`, `delete_wal`, `delete_job`
- `delete_schema_by_id`, `delete_source`, `delete_replay_handle`

These are mechanical: one variant per method, single u64 field, return
the matching IdRow / unit.

### Cat B: Two-arg primitives (~10 methods)
Shape: `(id, simple_other) -> Result<IdRow<T>, CubeError>`.

Examples:
- `update_status(job_id, JobStatus)`, `update_heart_beat(job_id)`
- `seal_table_by_id`, `update_replay_handle_failed_if_exists`
- `rename_schema(old, new)`, `rename_schema_by_id(id, new)`
- `set_current_snapshot(snapshot_id: u128)`

### Cat C: Create-with-struct (~15 methods)
Shape: takes a struct (Partition, Chunk, MultiPartition, etc.) plus
context, returns `IdRow<T>`.

Examples:
- `create_partition(Partition)`, `create_multi_partition(MultiPartition)`
- `create_chunk(...)`, `create_wal(table_id, row_count)`
- `create_partitioned_index(schema, name, columns, if_not_exists)`
- `create_or_update_source(...)`, `create_replay_handle(...)`

### Cat D: Mega-args (3 methods)
Shape: 10+ arguments, complex Option chains.

The big three:
- `create_table` — 16 args including Vec<Column>, Vec<IndexDef>,
  Option<DateTime<Utc>>, Option<StreamOffset>, etc. Hardest single
  encode target in the trait.
- `swap_active_partitions` — three Vecs of nested tuples carrying Row
  data. Atomic compound op.
- `commit_multi_partition_split` — six Vecs/u64s, atomic.

For these we initially keep the M1 "opaque payload + payload_version"
form (already in MetaCommand) and graduate to fully-typed variants
once the rest of M3 is shaping.

### Cat E: Compound atomic swaps (~8 methods)
Shape: multi-step atomic mutation.

Examples:
- `swap_chunks(...)`, `swap_chunks_without_check(...)`
- `swap_compacted_chunks(...)`, `swap_active_partitions(...)`
- `commit_multi_partition_split(...)`, `prepare_multi_partition_for_split(...)`
  (the prepare variants return data; need to be classified as reads
  or as write+read combos — TBD during M3.3)

### Cat F: Cache + scheduler ops (~15 methods)
Shape: queue, lease, lock, heartbeat operations against the job/queue
column families.

Examples:
- `add_job(job)`, `delete_job(id)`, `take_job_by_id(id)`
- `start_processing_job(...)`, `update_status(...)`,
  `update_heart_beat(...)`
- `acquire_partitioned_lock(...)`, `release_partitioned_lock(...)`

These touch the job_store / queue subsystem. Pre-aggregation refresh
worker is the heavy user. Need careful determinism review for the
heartbeat / time-based ones.

## M3 sub-milestones

| # | Title | Status | Estimated commits |
|---|---|---|---|
| **M3.1** | Catalog + 14 new variants in MetaCommand (Cat A + B) | ✅ done (`m3.1-complete`) | 1 |
| **M3.2** | `MetaCommandResult` enum with typed returns mirroring trait return shapes | ✅ done (`m3.2-complete`) | 1 |
| **M3.3** | `RocksMetaStoreApply: Apply` impl — dispatch table for every variant | in progress | 3-5 (incremental) |
| ↳ M3.3.a | Async Apply trait + dispatch for M3.1/M3.2 variants (~28) | ✅ done (`m3.3.a-complete`) | 1 |
| ↳ M3.3.b.1 | Cat C — create-with-struct (8 variants) | ✅ done (`m3.3.b.1-complete`) | 1 |
| ↳ M3.3.b.2 | Cat E + F + remaining Cat A/B (13 variants) | ✅ done (`m3.3.b.2-complete`) | 1 |
| ↳ M3.3.b.3 | Cat D — `CreateTable` structured form | ✅ done (`m3.3.b.3-complete`) | 1 |
| ↳ M3.3.b.4 | `SwapActivePartitions` structured form (Cat E with Row payload) | ✅ done (`m3.3.b.4-complete`) | 1 |
| ↳ M3.3.c | `Batch` atomicity via shared `write_operation` | pending | 1 |
| **M3.5** | `RaftMetaStore` wrapper + `CUBESTORE_HA_MODE` boot path swap | ✅ done (`m3.5-complete`) | 3 |
| ↳ M3.5.a | Wrapper skeleton + e2e schema-lifecycle test | ✅ done (`m3.5.a-complete`) | 1 |
| ↳ M3.5.b | Full `impl MetaStore for RaftMetaStore` (~121 methods) | ✅ done (`m3.5.b-complete`) | 1 |
| ↳ M3.5.c.1 | `CUBESTORE_HA_MODE` env var + ConfigObj methods | ✅ done (`m3.5.c.1-complete`) | 1 |
| ↳ M3.5.c.2 | DI swap: `RocksMetaStore`→`RaftMetaStore` when `ha_mode=raft` | ✅ done (`m3.5.c.2-complete`) | 1 |
| **M3.4** | Determinism fix: leader-resolved `Utc::now`/random in row constructors | in progress | 3 |
| ↳ M3.4.a | Pattern + `CreateChunk` (Chunk::new_pure + insert_chunk_pre_built + wrapper + dispatch) | ✅ done (`m3.4.a-complete`) | 1 |
| ↳ M3.4.b | `Table::new_pure` + `ReplayHandle::new_pure` constructors | in progress | 1 |
| ↳ M3.4.b.1 | Plumb leader-stamped now through `create_table`, `create_replay_handle`, `create_replay_handle_from_seq_pointers` apply paths | pending | 1 |
| ↳ M3.4.c | In-place stamps (`Chunk::deactivate`, `update_heart_beat`, etc.) | pending | 1 |
| **M3.4** | Determinism fix: leader-assigned IDs (`assigned_id: Option<u64>` on Cat A/B/C variants) | pending | 2 |
| **M3.5** | Config wiring: `CUBESTORE_HA_MODE` env binding + boot path swap | pending | 1 |
| **M3.6** | cubestore-sql-tests passing with HA mode (the critical-path gate) | pending | 1-3 fixing edge cases |

Ordering matters: 3.4 (determinism) MUST land before 3.5 (config wiring)
because flipping HA on without leader-assigned IDs causes silent
metastore divergence between replicas.

## Determinism notes — what to audit before M3.4

`RocksMetaStore` write methods that need leader-assigned IDs:
- Anything calling `next_id()` internally (need to grep this)
- Anything stamping `created_at: DateTime<Utc>` (need leader-assigned
  timestamp, follower applies verbatim)
- Anything reading filesystem state inside the write path

Initial grep:

```bash
grep -nE "next_id|Utc::now|SystemTime::now|Instant::now" \
  rust/cubestore/cubestore/src/metastore/mod.rs
```

…will be the first concrete step of M3.4.

## Wire-format compatibility plan

`MetaCommand` schema is versioned via `META_COMMAND_VERSION` (see
command.rs). Bumping rules during M3.x:

1. Adding a new variant: bump version, accept old version on decode
   (forward-compat for slow-rolling upgrade — old leader proposes,
   new follower applies).
2. Renaming a field: bump version, support both old and new shapes
   on decode for one minor release, then drop old.
3. Removing a variant: support decoding it (return error from apply)
   for one minor release, then drop. In practice we'll never drop
   variants — the wire schema only grows.

The `Generic { method, body }` escape hatch is the safety net during
the M1→M3 transition. Once M3.1 + M3.2 ship, all 86 methods have typed
variants and `Generic` becomes unused (but stays in the enum for
forward-compat — a future-method that lands during a rolling upgrade
can ship as Generic on a new replica and apply correctly on a follower
running the older binary).

## M3.2 — return-shape design notes

The trait's write methods produce exactly four return shapes:

| Trait return                    | `MetaCommandResult` variant | # of methods |
|---------------------------------|-----------------------------|--------------|
| `Result<(), CubeError>`         | `Unit`                      | ~25          |
| `Result<bool, CubeError>`       | `Bool`                      | 1 (`swap_compacted_chunks`) |
| `Result<IdRow<T>, CubeError>`   | `IdRow { kind, payload }`   | ~50          |
| `Result<Option<IdRow<T>>, ...>` | `OptionalIdRow { kind, .. }`| 4 (`add_job`, `start_processing_job`, `finalize_replay_handles`, `update_replay_handle_seq_pointer_if_exists` returning option) |

The eleven distinct `T` values for `IdRow<T>` are tagged via `IdRowKind`
(`Schema`, `Table`, `Partition`, `Chunk`, `Wal`, `Job`, `Source`,
`ReplayHandle`, `MultiPartition`, `MultiIndex`, `Index`).

**Why opaque payload + tag instead of typed payload variants?** The raft
module deliberately doesn't import metastore row types — that would
create a circular dependency (metastore depends on the raft module for
replication, raft module would then transitively depend on metastore).
Instead, the apply impl in M3.3 (`RocksMetaStoreApply`) calls
`MetaCommandResult::id_row(kind, &row)` which flexbuffer-encodes the
`IdRow<T>` and tags it with `kind`. The wrapper-style `RaftMetaStore:
MetaStore` impl (which legitimately depends on metastore types) calls
`result.into_id_row(IdRowKind::Schema)` to decode the typed value. The
codec is verified by 6 round-trip tests in `command.rs::tests`.

**Mismatch handling.** A variant mismatch (`into_unit` on a `Bool`,
`into_id_row::<X>(IdRowKind::Schema)` on an `IdRow{kind: Table, ..}`)
returns `MetaCommandResultMismatch` — never panics. This is the
diagnostic for a misimplemented Apply variant in M3.3 and converts
cleanly to `CubeError::internal` at the trait boundary.

## M3.4 — leader-build pattern

Audit found the determinism gap is **not** in metastore trait method
bodies (their `Utc::now()` calls are read-only filters) but in **row
constructors**:

| Constructor                  | Non-deterministic input(s)        |
|------------------------------|------------------------------------|
| `Chunk::new`                 | `created_at`, `oldest_insert_at` (`Utc::now`); `suffix` (`thread_rng`) |
| `Chunk::deactivate`          | `deactivated_at` (`Utc::now`)     |
| `Table::new`                 | `created_at` (`Utc::now`)         |
| `Job::new`                   | `last_heart_beat` (`Utc::now`)    |
| `ReplayHandle::new`          | `created_at` (`Utc::now`)         |

**Pattern (M3.4.a, exemplified by `Chunk`):**

1. Add `RowType::new_pure(args..., now, suffix...)` — every input
   explicit. `RowType::new` becomes a thin wrapper that calls
   `new_pure` with `Utc::now()` and `thread_rng()`.
2. Add inherent `RocksMetaStore::insert_<row>_pre_built(row)` —
   takes a fully-built row and inserts. Outside HA mode it's also a
   useful escape hatch for callers wanting explicit field control.
3. Replace the `MetaCommand::Create<Row> { args }` variant with
   `MetaCommand::Create<Row> { row_blob }` — ships a fully-built
   row instead of construction args.
4. Wrapper's `MetaStore::create_<row>(args)` builds the row on the
   leader (which is always self when proposing), encodes, proposes.
   Replicas decode and call `insert_<row>_pre_built`.

**Result**: every replica's RocksDB content is byte-identical for the
inserted row. Plus `next_id` is deterministic in HA mode because all
writes go through Raft in the same order, so the merge-counter
advances identically on every replica.

**M3.4.b** lands the same pattern for Table, ReplayHandle, and Job.
**M3.4.c** handles in-place stamps (`Chunk::deactivate`,
`update_heart_beat`) — those are trickier because the stamp happens
inside `write_operation` rather than in a constructor. Likely fix:
the wrapper pre-stamps and ships through new variants
(`DeactivateChunkAt`, `HeartBeatAt`).

## M3.3 — design decisions parked here

When M3.3 starts:

1. **Sync `Apply` → async `Apply`.** Today the trait is
   `fn apply(&self, cmd) -> Result<...>`. Real `RocksMetaStore`
   write methods are async (they queue through `write_operation`
   onto `rw_loop`), so the trait must become
   `async fn apply(...)` (via `#[async_trait]`).
   The Raft apply task in `state_machine::run_node` is already a
   tokio task — `drive_ready` becomes `async fn` and awaits each
   apply call. No new threads.

2. **Dispatch table location.** New file
   `src/raft/rocks_apply.rs` with
   `pub struct RocksMetaStoreApply { store: Arc<RocksMetaStore> }`
   and one match arm per `MetaCommand` variant. Each arm calls
   the matching trait method on `store` and wraps the return
   into the right `MetaCommandResult` variant via the helper
   constructors in `command.rs`.

3. **Atomicity.** `MetaCommand::Batch { commands }` must apply
   inside a single `write_operation` so the underlying RocksDB
   `WriteBatch` covers all of them. Sub-dispatch happens against
   the same `BatchPipe`. This requires a slightly different shape
   than the per-variant arms — implement `apply_batch` separately
   from `apply_single`.

4. **Determinism gate before M3.5.** M3.3 lands the dispatch but
   keeps using the existing non-deterministic `next_id()` /
   `Utc::now()` paths inside `RocksMetaStore`. M3.4 then adds
   `assigned_id: Option<u64>` and `assigned_now: Option<i64>` to
   the relevant variants and changes the trait methods to accept
   them. **Do not flip `CUBESTORE_HA_MODE=raft` (M3.5) until
   M3.4 is in.** Auditing grep: 5 `Utc::now()` calls in
   `metastore/mod.rs` (lines 2597, 3194, 3331, 4147 + one
   `SystemTime::now` for cache TTL). `next_id` calls are inside
   the per-table `RocksTable::insert` impl — N+1 grep needed.

5. **Test strategy.** Two layers:
   - per-variant unit tests in `rocks_apply.rs::tests` using a
     real temp `RocksMetaStore` instance (no Raft) to verify
     the dispatch produces the right side effects;
   - end-to-end test in `state_machine::tests` that boots a
     single-node Raft on top of `RocksMetaStoreApply` and
     proves the propose→apply→trait-return path is wired right.
