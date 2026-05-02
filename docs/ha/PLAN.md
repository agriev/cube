# Cube Store HA Fork — Implementation Plan

## Context

Cube.js Community Edition has a known, documented single-point-of-failure: the
Cube Store router. All metadata (schemas, table → partition mappings, chunk →
worker assignments, the job queue, the pre-aggregation registry) lives in a
single RocksDB instance on the router pod. When the router dies, the entire
cluster is unqueryable until k8s reschedules the pod and re-attaches its PVC.

The Cube docs confirm this in writing: *"The open-source version of Cube Store
doesn't support replicating any of its nodes... If any cluster node is down,
it'll lead to a complete cluster outage"*
([source](https://cube.dev/docs/product/caching/running-in-production)).

What we discovered while researching (Phase 1):

- **Cube Cloud does not actually solve this** at the metadata layer — its HA
  story is pod-level redundancy + cloud infra resilience, with the same
  non-replicating Cube Store underneath.
- The data plane is already durable (Parquet on S3/GCS); the gap is purely the
  control-plane metadata.
- The codebase exposes two clean abstraction seams (`MetaStore` trait,
  `Cluster` trait) that make a wrapper-style replication layer viable without
  invasive surgery.
- Apache 2.0 license — fork-and-ship-as-OSS is legally fine.
- Both ClickHouse Keeper and StarRocks FE prove that **embedded Raft** (no
  external coordinator) is the right pattern for this kind of system.

This work makes the existing Helm chart at `agriev/cube-stack-deployment`
genuinely production-grade: a 3-router cluster survives pod kills, network
partitions, and rolling node updates with sub-10s failover, no manual
intervention, and no data corruption.

## Recommended approach (MVP scope: router-only HA)

Confirmed scope: **router-only HA**. Workers stay single-replica per partition
in v1 — that's a documented limitation that is honest and acceptable for the
first ship. Worker partition replication is Phase 2.

Confirmed fork strategy: **living fork** at `agriev/cube` (new public OSS
repo). Track upstream `cube-js/cube` release tags, rebase quarterly, file a
single tracking issue upstream offering the work, but do not block on
upstream engagement.

### Architecture in one paragraph

Wrap (don't replace) the existing `MetaStore` trait. Introduce a new
implementation `RaftMetaStore` that owns the existing `RocksMetaStore` as its
local state-machine backend, and routes every *write* through a `raft-rs`
log: serialize the call as a `MetaCommand` enum, propose to the Raft group,
apply to local RocksDB only after commit. Reads stay local (leader-only in
v1). The rest of the codebase calls the `MetaStore` trait unchanged. Cluster
membership is bootstrapped from `CUBESTORE_RAFT_PEERS` env var; runtime
add/remove is via a new admin RPC method on `cuberpc`. New env var
`CUBESTORE_HA_MODE=raft|off` opts the whole thing in — `off` is the default
for backwards compatibility, `raft` activates the new code path.

### Milestone plan (10 milestones, units = solo-engineer-weeks)

| # | Title | Weeks | Deliverable |
|---|---|---|---|
| M1 | Add `raft-rs`, define `MetaCommand` enum | 1 | All `MetaStore` write methods round-trip through encode/decode |
| M2 | Single-node Raft over local RocksDB | 2 | `RaftMetaStore::create_schema(...)` matches `RocksMetaStore::create_schema` byte-for-byte |
| M3 | Wire all `MetaStore` write methods through Raft apply | 2 | `cubestore-sql-tests` suite passes with `CUBESTORE_HA_MODE=raft` (single node) |
| M4 | 3-node clustering, leader election, follower replication | 2 | Chaos test kills leader 100×, log indices converge in <5s |
| M5 | Snapshot + log compaction | 1 | Late-joining follower catches up via S3 snapshot path |
| M6 | Leader-aware client routing (Cube API + workers) | 1–2 | Killing leader pod → API recovers in <5s without restart |
| M7 | Helm chart updates (`router-statefulset.yaml`, env, PDB, ports) | 1 | `helm upgrade --set cubestore.ha.enabled=true` works clean |
| M8 | Chaos & soak tests (kill -9, drain, partition, disk-full) | 1–2 | CI nightly chaos suite + runbook |
| M9 | Observability (term, commit index, leader changes, apply latency) | 1 | Grafana dashboard JSON shipped in chart |
| M10 | Docs, fork README, migration guide from non-HA | 1–2 | A new operator can stand up HA cluster from README alone |

**Total: 13–22 solo-engineer-weeks.** Lower bound assumes determinism audit
goes clean; upper bound assumes 2–4 non-deterministic write side-effects
(monotonic ID gen, timestamp stamping, opportunistic FS ops) need redesign,
which is the realistic baseline for a 5-year-old single-writer codebase.

Calendar-time depends on engineering capacity (still TBD). For reference:
- Side-project (10-15h/wk): 6–12 calendar months
- Solo full-time (40h/wk): 13–22 calendar weeks (~3-5.5 months)
- Two engineers full-time (parallelizing M2/M3 with M7/M9): 8–14 weeks

### Critical files

**In the fork (`agriev/cube` — to be created):**

- `rust/cubestore/cubestore/src/metastore/mod.rs` — adds `RaftMetaStore` impl
  alongside existing `RocksMetaStore`. The trait itself is unchanged.
- `rust/cubestore/cubestore/src/cluster/mod.rs` — adds leader-aware routing in
  `Cluster::route_select` and worker-side leader discovery cache.
- `rust/cubestore/cubestore/src/raft/` — **new module**, ~2000–3000 LoC, holds
  `state_machine.rs`, `transport.rs` (over `cuberpc`), `storage.rs` (Raft log
  in separate RocksDB instance under `<data_dir>/raft-log/`).
- `rust/cubestore/cubestore/src/config/mod.rs` — adds env var bindings:
  `CUBESTORE_HA_MODE`, `CUBESTORE_NODE_ID`, `CUBESTORE_RAFT_PEERS`,
  `CUBESTORE_RAFT_PORT` (default 9100), `CUBESTORE_RAFT_ELECTION_TICK`,
  `CUBESTORE_RAFT_HEARTBEAT_TICK`, `CUBESTORE_RAFT_SNAPSHOT_INTERVAL`.
- `rust/cubestore/Cargo.toml` — adds `raft-rs = "0.7"` (TiKV's mature MIT-
  licensed Raft crate).

**In the Helm chart (`agriev/cube-stack-deployment` — already exists):**

- `charts/cube-stack/templates/cubestore/router-statefulset.yaml` —
  inject `CUBESTORE_NODE_ID` from `metadata.name`, expose port 9100, add
  `CUBESTORE_RAFT_PEERS` from a chart-rendered list, default `replicas: 3`
  when `cubestore.ha.enabled=true`.
- `charts/cube-stack/templates/cubestore/_cubestore-env.tpl` — emit the
  new env vars conditionally.
- `charts/cube-stack/values.yaml` — add `cubestore.ha` block (`enabled`,
  `electionTimeout`, `snapshotInterval`, `image.tag` override for the fork).
- `charts/cube-stack/templates/cubestore/pdb.yaml` — when HA enabled, set
  `minAvailable: 2` (quorum of 3) instead of allowing 1 down.

### Top 5 risks (with mitigations)

1. **Hidden non-determinism in write paths.** Replicated apply must be
   deterministic; clock reads, monotonic ID generators, and side-effecting FS
   ops break this silently. *Mitigation:* determinism audit before M3, move
   ID gen into Raft entry metadata (leader-assigned, follower-applied), CI
   test that compares RocksDB content hashes across replicas.
2. **`BatchPipe` atomicity vs. Raft entry boundaries.** Multi-statement DDL
   currently uses RocksDB `WriteBatch`; if a single DDL becomes multiple
   Raft entries, partial failure mid-DDL leaves split-brain metadata.
   *Mitigation:* encode each `BatchPipe` as exactly one `MetaCommand::Batch`
   Raft entry, applied as a single RocksDB `WriteBatch`.
3. **`raft-rs` storage trait quirks** (HardState durability, ConfChange
   correctness across restarts). *Mitigation:* copy the storage trait impl
   from TiKV's `raft-engine` reference, don't roll our own in M2.
4. **Worker→router routing during failover.** Workers connect to one router;
   if it's no longer leader, their RPCs fail. *Mitigation:* in M6, change
   worker→router calls to follow the leader (cache + refresh on RPC error).
5. **StatefulSet bootstrap chicken-and-egg.** All 3 pods start in parallel;
   none can elect a leader until 2 are reachable. *Mitigation:* document
   normal 10s settle time, longer `initialDelaySeconds` on readiness probe.

## Verification

End-to-end success criteria — all must hold on a 3-router cluster running on
the existing Helm chart with `cubestore.ha.enabled=true`:

1. **Failover speed:** `kubectl delete pod cubestore-router-0 --force --grace-period=0`
   (current leader) → new leader within 10s, measured by metric
   `cubestore_raft_leader_changes_total`.
2. **Query SLA during failover:** any in-flight Cube SQL query at the time
   of the kill either succeeds within 30s OR fails fast within 5s. No hangs.
3. **Metadata durability:** `CREATE TABLE` at T-1s + `kill -9` of leader at
   T+0 → table exists on new leader after failover. Verified by an e2e test
   with 1000 concurrent DDLs interleaved with random leader kills.
4. **No split-brain:** 60s network partition isolating one router → on heal,
   the partitioned router rejoins and converges without manual action.
   RocksDB content hashes match across all 3 routers post-heal.
5. **Performance bound:** DDL throughput on 3-router cluster ≥ 50% of single
   router (Raft commit overhead). Read latency unchanged ±10%.
6. **Operator surface:** upgrading the existing
   `agriev/cube-stack-deployment` Helm release from `replicas: 1` to
   `replicas: 3 + ha.enabled=true` is a single `helm upgrade` with no
   manual data migration.
7. **14-day soak:** nightly chaos test (random pod kills + partitions)
   running for 2 weeks records zero data corruption events and zero
   unrecoverable cluster states.

### How to test end-to-end during development

```bash
# Spin up local 3-router HA cluster on kind
cd cube-stack-deployment
kind create cluster --name cube-ha-test
helm upgrade --install cube ./charts/cube-stack \
  -f charts/cube-stack/values.yaml \
  --set cubestoreImage.repository=ghcr.io/agriev/cubestore \
  --set cubestoreImage.tag=v1.6.41-ha.1 \
  --set cubestore.ha.enabled=true \
  --set cubestore.router.replicas=3 \
  -n cube --create-namespace --wait

# Run chaos suite
make chaos-test  # in fork repo: spawns DDL load, kills leaders, asserts no corruption

# Watch failover live
watch -n 0.5 'kubectl -n cube get pods -l app.kubernetes.io/component=cubestore-router && \
  kubectl -n cube exec deploy/cube-cube-stack-api -- \
    curl -s http://localhost:9102/metrics | grep raft_leader'
```

## Out of scope for this MVP

- Worker partition replication (Phase 2)
- Follower reads / observer nodes (Phase 3)
- Multi-region / geo-distributed metadata (Phase 3)
- Hot DDL during failover (in-flight queries gracefully migrating leaders)
- Backwards-compatible mixed-version cluster (HA + non-HA routers in same cluster)
