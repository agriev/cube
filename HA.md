# Cube Store HA Fork

This is a **fork** of [cube-js/cube](https://github.com/cube-js/cube) that adds
**high availability** to the OSS Cube Store router.

> **Status: M1 of 10 — scaffolding complete, no replication yet.**
> Do not deploy to production. Track [ROADMAP](#roadmap) below.

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
| M1 | `raft-rs` crate + `MetaCommand` enum scaffolding | **done** |
| M2 | Single-node Raft with local RocksDB log | todo |
| M3 | Wire all 86 `MetaStore` writes through the apply path | todo |
| M4 | 3-node clustering, leader election, follower replication | todo |
| M5 | Snapshot + log compaction over `RemoteFs` | todo |
| M6 | Leader-aware client routing (Cube API + workers) | todo |
| M7 | Helm chart updates (`agriev/cube-stack-deployment`) | todo |
| M8 | Chaos & soak tests (kill -9, drain, partition) | todo |
| M9 | Observability (Prometheus metrics, Grafana dashboard) | todo |
| M10 | Docs + migration guide from non-HA | todo |

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

For now, the fork builds identically to upstream Cube Store:

```bash
cd rust/cubestore
cargo build --release
```

The new `raft` module compiles but is unreachable code (no production code
calls it yet — that wiring lands in M2). HA-mode envs (`CUBESTORE_HA_MODE`,
`CUBESTORE_RAFT_PEERS`, etc.) are recognised but ignored.

## Contact

[Anton Griev](https://github.com/agriev) — issues / PRs welcome.
