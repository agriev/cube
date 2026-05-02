# M2 prep — DI hook points (notes captured during M1 verification)

When `CUBESTORE_HA_MODE=raft` we will swap the boot wiring so the
trait-object that the rest of the code calls is a `RaftMetaStore`
wrapping the existing `RocksMetaStore`. This file documents the exact
files / lines to touch in M2 so we don't have to re-discover them.

## DI seam

`rust/cubestore/cubestore/src/config/mod.rs`:

```rust
// line 73-75 in v1.6.41
pub rocks_meta_store: Option<Arc<RocksMetaStore>>,   // concrete — used by
                                                      // upload/maintenance loops
pub meta_store: Arc<dyn MetaStore>,                   // trait object — the
                                                      // injection point
```

Everything in cubestore that does meta lookups goes through the
`Arc<dyn MetaStore>` field. Replacing it with an `Arc<RaftMetaStore>`
(which itself implements `MetaStore`) is invisible to callers. The
concrete `Option<Arc<RocksMetaStore>>` field stays — `RaftMetaStore`
holds the same `Arc<RocksMetaStore>` internally for read-side delegation
and snapshot/upload loops.

## Boot sequence

The `RocksMetaStore::new` factory is called in 7 places inside
`metastore/mod.rs`:

```
rust/cubestore/cubestore/src/metastore/mod.rs:5154   prod boot
rust/cubestore/cubestore/src/metastore/mod.rs:5228   tests
rust/cubestore/cubestore/src/metastore/mod.rs:5379   tests
rust/cubestore/cubestore/src/metastore/mod.rs:5459   tests
rust/cubestore/cubestore/src/metastore/mod.rs:5505   tests
rust/cubestore/cubestore/src/metastore/mod.rs:5614   tests
rust/cubestore/cubestore/src/metastore/mod.rs:5700   tests
```

Only the prod path (5154) needs HA-aware wrapping. Test paths can stay
on the bare `RocksMetaStore` — replication isn't on the critical path
for cubestore-sql-tests.

## Read-path delegation (single-leader)

In M2 (single-node Raft running on one router) and M4 (3-router cluster)
the `RaftMetaStore::<read method>` impls just delegate to the inner
`RocksMetaStore`. This is correct because:

1. M2: only one node → leader is always self → reads are linearizable.
2. M4 leader-only reads: clients only connect to the leader (per-pod
   `/leader` endpoint introduced in M6). Followers reject reads with a
   `RedirectToLeader` error. Read-index / lease-reads on followers
   land in Phase 3.

## Write-path proposal (M3 mechanical step)

For each of the 86 write methods on `MetaStore`, the wrapper impl is:

```rust
async fn create_schema(&self, schema_name: String, if_not_exists: bool)
    -> Result<IdRow<Schema>, CubeError>
{
    let cmd = MetaCommand::CreateSchema { schema_name, if_not_exists };
    let apply_result = self.propose_and_wait(cmd).await?;
    apply_result.into_create_schema()
}
```

Where:

- `propose_and_wait` proposes the command to the Raft group and resolves
  once the apply loop has returned the typed result for that log index.
- The apply loop deserializes each committed `MetaCommand`, dispatches
  to `RocksMetaStore::<method>` inside a single RocksDB `WriteBatch`,
  and signals completion via a oneshot keyed by log-index.
- `apply_result` is a typed enum (`MetaCommandResult`) that mirrors the
  return types of the 86 methods. Necessary so write callers get back
  exactly what they would have got from the direct path — `IdRow<Schema>`
  for `create_schema`, etc.

## ID generation determinism (plan risk #1)

`RocksMetaStore` calls `next_id()` inside several write methods. If
followers re-execute `next_id()` they will diverge. Fix in M3:

- The leader assigns the ID **before** proposing.
- The proposed `MetaCommand` carries the ID as a field
  (e.g. `MetaCommand::CreateSchema { schema_name, if_not_exists,
  assigned_id: Option<u64> }`).
- `RocksMetaStore::write_operation` is augmented with a "use this exact
  ID" path (or we add a sibling private method
  `create_schema_with_id`).
- Followers apply with `assigned_id` → no `next_id()` call → same RocksDB
  bytes on every replica.

Only the leader is allowed to mint IDs. Mid-failover, the new leader
reads `next_id` from its committed state-machine view, which is
guaranteed to be at least as advanced as any unmasterful proposal in the
old leader's outstanding batch — a basic Raft property.

## Files to modify in M2

| File | Change |
|---|---|
| `rust/cubestore/cubestore/src/raft/state_machine.rs` | `RaftMetaStore` impl, propose/apply, oneshot wiring |
| `rust/cubestore/cubestore/src/raft/storage.rs` | `raft::Storage` impl over a separate RocksDB |
| `rust/cubestore/cubestore/src/raft/transport.rs` | `cuberpc`-based message ship (M2 stub: no-op for single node) |
| `rust/cubestore/cubestore/src/config/mod.rs` | `CUBESTORE_HA_MODE` env binding; switch `meta_store` field assignment |
| `rust/cubestore/cubestore/src/bin/cubestored.rs` | Wire HA mode flag into boot sequence |

## Out of scope for M2

- Multi-node clustering (M4)
- Snapshots (M5)
- Leader-aware client routing (M6)

M2's success criterion (per PLAN.md): `RaftMetaStore::create_schema(...)`
matches `RocksMetaStore::create_schema(...)` byte-for-byte on a single
node, and `cubestore-sql-tests` suite passes with `CUBESTORE_HA_MODE=raft`.
