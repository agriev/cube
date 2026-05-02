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

| # | Title | Estimated commits |
|---|---|---|
| **M3.1** | Catalog + 10 representative new variants in MetaCommand (Cat A + B) | 1-2 |
| **M3.2** | `MetaCommandResult` enum with typed returns mirroring trait return shapes | 1 |
| **M3.3** | `RocksMetaStoreApply: Apply` impl — dispatch table for every variant | 3-5 (incremental) |
| **M3.4** | Determinism fix: leader-assigned IDs (`assigned_id: Option<u64>` on Cat A/B/C variants) | 2 |
| **M3.5** | Config wiring: `CUBESTORE_HA_MODE` env binding + boot path swap | 1 |
| **M3.6** | cubestore-sql-tests passing with HA mode (the critical-path gate) | 1-3 fixing edge cases |

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
