# Cube Store HA Fork

This is a **fork** of [cube-js/cube](https://github.com/cube-js/cube) that adds
**high availability** to the OSS Cube Store router.

> **Status: M3, M4, M5 (storage layer), M6, M9, M10 are tagged complete
> with full CI green.** Multi-node Raft consensus over real TCP works
> end-to-end; 3-node clusters elect a leader, replicate writes, survive
> a partitioned-leader failover (~5 s budget tested in CI), persist
> snapshots, install snapshots from peers, expose `current_leader_id`
> introspection, ship 9 operator metrics + a Grafana dashboard, and
> include a step-by-step migration guide.
>
> **Remaining for production**: only M8 chaos suite at k8s scale.
> The Helm chart (M7) was verified end-to-end on docker-desktop k8s:
> `make ha-deploy && make ha-verify` brings up 3 routers, elects a
> leader, kills it, and confirms a new leader within budget. The
> entire metadata-replication core, including remote snapshot
> backup and the chart wiring, is fully landed across both repos.
> Track [ROADMAP](#roadmap) below.

## Why this fork exists

Cube.js Community Edition has a known, documented single-point-of-failure: the
Cube Store router. All metadata (schemas, partitions, chunk-to-worker
assignments, the job queue, the pre-aggregation registry) lives in **one**
RocksDB instance on the router pod. When the router dies, the entire cluster
is unqueryable until Kubernetes reschedules the pod and re-attaches its PVC.

The official Cube docs confirm this:

> *"The open-source version of Cube Store doesn't support replicating any of
> its nodes... If any cluster node is down, it'll lead to a complete cluster
> outage."*
> — [Running in production | Cube docs](https://cube.dev/docs/product/caching/running-in-production)

What's more — based on public information, **Cube Cloud does not solve this
at the metadata layer either**. Its HA story is pod-level redundancy on
managed infrastructure with the same non-replicating Cube Store underneath.

This fork adds genuine HA to the OSS edition.

## What it does (when complete)

- **3-replica router** with Raft-elected leadership
- **Sub-10s automatic failover** when the leader pod dies
- **No external coordinator** — no ZooKeeper, no etcd. Embedded Raft
  via `raft-rs`, the same approach as ClickHouse Keeper and StarRocks FE
- **Fully additive** — `CUBESTORE_HA_MODE=off` (default) keeps the original
  single-router behavior unchanged. `CUBESTORE_HA_MODE=raft` opts in.
- **Drop-in to the existing Helm chart** at
  [agriev/cube-stack-deployment](https://github.com/agriev/cube-stack-deployment) —
  one `helm upgrade --set cubestore.ha.enabled=true` flips the cluster.

## What it does NOT do (yet)

- **Worker partition replication** — worker death still makes its partitions
  unqueryable until restart. This is intentionally Phase 2: rolling solving
  the metadata SPOF first is the highest-leverage change.
- **Follower reads** — all reads route to the current leader. Phase 3.
- **Multi-region / geo-replication** — Phase 3.
- **Hot DDL during failover** — in-flight queries fail-fast within 5s of
  leader loss; clients retry. Surviving in-flight is a multi-month protocol
  problem postponed beyond v1.

## Architecture in one paragraph

The new module [`rust/cubestore/cubestore/src/raft/`](rust/cubestore/cubestore/src/raft/)
introduces `RaftMetaStore`, a **wrapper** (not replacement) around the
existing `RocksMetaStore`. Every mutating call on the `MetaStore` trait gets
encoded as a [`MetaCommand`](rust/cubestore/cubestore/src/raft/command.rs)
enum, proposed to the Raft group via `raft-rs`, and applied to the local
RocksDB only after the entry commits. Reads stay local on the leader. The
Raft log lives in a separate RocksDB instance under `<data_dir>/raft-log/`.
Cluster membership is bootstrapped from `CUBESTORE_RAFT_PEERS`. The rest of
the Cube Store codebase keeps calling the `MetaStore` trait and is oblivious
to whether replication is on. See [HA design plan](docs/ha/PLAN.md).

## Roadmap

| Milestone | Title | Status |
|---|---|---|
| M1 | `raft-rs` crate + `MetaCommand` enum scaffolding | ✅ **done** |
| M2 | Single-node Raft with local RocksDB log | ✅ **done** |
| M3 | Wire all `MetaStore` writes through the apply path | ✅ **done** (`m3-complete`) |
| M4 | 3-node clustering, leader election, follower replication | ✅ **done** (`m4-complete`) |
| M5 | Snapshot + log compaction over `RemoteFs` | ✅ **done** — storage layer (`m5-storage-complete`) + S3 upload of `snapshot.bin` (M5.x). Late-joining followers catch up peer-to-peer via raft's MsgSnapshot; remote copy covers the all-replicas-offline DR scenario. |
| M6 | Leader-aware client routing (Cube API + workers) | ✅ **done** — raft-rs auto-forwards proposes from followers; M6.1 exposes `current_leader_id` for k8s readinessProbe; M6.2 marks no-leader errors with `raft-leader-id=N` for smart retries |
| M7 | Helm chart updates (`agriev/cube-stack-deployment`) | ✅ **done** (`m7-complete` in [agriev/cube-stack-deployment](https://github.com/agriev/cube-stack-deployment)) — `cubestore.ha.enabled=true` flips a 3-router Raft cluster; verified end-to-end on docker-desktop k8s with election + sub-30s failover |
| M8 | Chaos & soak tests (kill -9, drain, partition) | todo (M4 chaos test for unit scope is in CI; k8s-scale soak still ahead) |
| M9 | Observability (Prometheus metrics, Grafana dashboard) | ✅ **done** — 9 `cs.raft.*` metrics + ready-to-import dashboard at [`docs/ha/grafana/`](docs/ha/grafana/) |
| M10 | Docs + migration guide from non-HA | ✅ **done** — see [`docs/ha/MIGRATION.md`](docs/ha/MIGRATION.md) |

**M3 sub-milestone tags** (in chronological order):
`m3.1-complete` → `m3.2-complete` → `m3.3.a-complete` →
`m3.3.b.{1,2,3,4}-complete` → `m3.3.c-complete` →
`m3.4.{a,b,b.1,b.2,c,d}-complete` →
`m3.5.{a,b,c.1,c.2}-complete` → `m3.5-complete` →
`m3.6-complete` → `m3-complete` → `m3.7-complete` →
`m3.8-complete`. CI gates (codec + cubestore cargo check + raft
test modules + cubestore-sql-tests under `CUBESTORE_HA_MODE=raft`)
all green on the linux/x64 + macOS/arm64 self-hosted runners.

Total estimated effort: **13–22 solo-engineer-weeks** to deployable MVP.

## License

This fork inherits the Apache 2.0 / MIT dual license of upstream Cube.
New code under `rust/cubestore/cubestore/src/raft/` is Apache 2.0.

## Upstream relationship

This is a **living fork**:

- `master` branch tracks `cube-js/cube` upstream verbatim.
- `ha-main` branch is the working branch — based on the latest upstream
  release tag (currently `v1.6.41`), rebased quarterly onto subsequent
  upstream releases.
- The HA work is offered upstream via a single tracking issue. The fork is
  designed to be additive (additive Cargo dep, new module, new env vars,
  no upstream-file rewrites where avoidable) so a future upstream merge is
  mechanically straightforward.

## Building / running

The fork builds identically to upstream Cube Store:

```bash
cd rust/cubestore
cargo build --release
```

To run with HA mode (single-node):

```bash
CUBESTORE_HA_MODE=raft \
CUBESTORE_NODE_ID=1 \
./target/release/cubestored
```

The metastore routes every write through Raft on the local node; the
wrapper is `RaftMetaStore`. With no `CUBESTORE_RAFT_PEERS` set this
runs as a 1-voter cluster — useful for dev/smoke tests.

To run as a multi-node cluster (3-node example, post-M4):

```bash
# router-0
CUBESTORE_HA_MODE=raft \
CUBESTORE_NODE_ID=1 \
CUBESTORE_RAFT_PEERS="1@router-0:9100,2@router-1:9100,3@router-2:9100" \
CUBESTORE_RAFT_PORT=9100 \
./target/release/cubestored

# router-1 (CUBESTORE_NODE_ID=2), router-2 (CUBESTORE_NODE_ID=3) follow
# the same pattern. Every node MUST list itself in CUBESTORE_RAFT_PEERS
# — boot panics otherwise.
```

Each replica binds `0.0.0.0:CUBESTORE_RAFT_PORT` and dials its peers
lazily. Election is timer-driven (no node calls `campaign()` at boot).
A committed write replicates to every voter's local RocksDB before
the proposer's API call returns, and a partitioned leader yields to a
new one within seconds.

The HA mode is **not yet production-ready** — snapshot / log
compaction (M5), leader-aware client routing (M6), Helm-chart
wiring (M7), and observability (M9) are still ahead.

To run the SQL test suite under HA mode:

```bash
CUBESTORE_HA_MODE=raft cargo test \
  --package cubestore-sql-tests --release --test in-process
```

CI runs this on every push to `ha-main`.

## Contact

[Anton Griev](https://github.com/agriev) — issues / PRs welcome.
