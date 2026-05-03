# Migrating an existing Cube Store deployment to the HA fork

This guide walks through moving from a single-router upstream Cube
Store to a 3-router HA cluster running this fork's binary. The
work splits into three phases: **build the fork's image**, **roll
the data plane**, and **switch on `CUBESTORE_HA_MODE=raft`**.

The fork is fully backwards-compatible at the binary level —
`CUBESTORE_HA_MODE=off` (the default) is a drop-in replacement
for upstream cubestore. You can ship the fork's image to a single-
router deployment with no other changes; only when you flip
`CUBESTORE_HA_MODE=raft` and supply `CUBESTORE_RAFT_PEERS` does
new behavior activate.

## Pre-flight

Before you start:

- **Storage** is on the data plane (Parquet on S3/GCS). The HA
  work is purely on the control-plane metadata. **You don't need
  to migrate any partition data**.
- **Upstream metadata** lives in a single RocksDB on the router
  pod, replicated to remote storage via `RemoteFs::upload_check_point`
  on a timer. The HA fork keeps that mechanism for the inner
  `RocksMetaStore` and adds Raft on top.
- **Read your `RemoteFs` config** — the existing `metastore-` /
  `metastore-current` blobs in your bucket are still the source
  of truth for cold restarts. The HA fork doesn't touch them.

## Phase 1 — Build & ship the fork's binary

```bash
# Clone the fork.
git clone https://github.com/agriev/cube.git
cd cube/rust/cubestore

# Build a release binary identical to upstream's modulo the HA
# additions. Build prereqs are the same as upstream — see
# upstream's CONTRIBUTING.md for cmake/sasl/lz4/zstd packages.
cargo build --release --package cubestore --bin cubestored
```

Or use the existing Helm chart at
[agriev/cube-stack-deployment](https://github.com/agriev/cube-stack-deployment),
which is the canonical packaging path and ships fork-built images.

## Phase 2 — Roll the data plane (no behavior change)

Replace the upstream `cubestored` image with the fork's image,
**without** setting any of the HA env vars. The pod boots in
`HaMode::Off` mode, which is byte-equivalent to upstream
cubestore. This is your safety net — if anything breaks, the
rollback is `kubectl rollout undo`.

Validate:

- The cluster comes up and serves SQL.
- `cubestore-sql-tests` (your equivalent of an end-to-end smoke)
  passes.
- Worker→router connections continue to work (no transport
  changes for non-HA mode).

Stop here if something's off. Don't go to Phase 3 until Phase 2
is observed-stable for at least one tick of your normal release
cadence.

## Phase 3 — Flip on Raft

This is the actual HA migration. Three changes simultaneously:

1. **Scale routers from 1 to 3**. The fork expects each replica
   to advertise itself in `CUBESTORE_RAFT_PEERS`; on Kubernetes
   the obvious mapping is StatefulSet ordinal → raft node id
   (`cubestore-router-0` → 1, `-1` → 2, `-2` → 3).

2. **Set the HA env vars**:

   ```yaml
   env:
     - name: CUBESTORE_HA_MODE
       value: raft
     - name: CUBESTORE_NODE_ID
       valueFrom:
         fieldRef:
           # Convert pod ordinal "cubestore-router-N" to "N+1".
           # Helper template in agriev/cube-stack-deployment.
           fieldPath: metadata.name
     - name: CUBESTORE_RAFT_PEERS
       value: >-
         1@cubestore-router-0.cubestore-router:9100,
         2@cubestore-router-1.cubestore-router:9100,
         3@cubestore-router-2.cubestore-router:9100
     - name: CUBESTORE_RAFT_PORT
       value: "9100"
   ```

   Every replica MUST list itself in `CUBESTORE_RAFT_PEERS`.
   If `CUBESTORE_NODE_ID` isn't in the peer set, the boot path
   panics — quorum math against a voter set that doesn't include
   self is silent split-brain.

3. **Open the raft port (9100 by default)** between router pods.
   If you're on a Kubernetes Service mesh or have explicit
   NetworkPolicies, add a rule allowing 9100 between the routers'
   pod CIDRs. See [`HA.md`](../../HA.md#building--running) for the
   bind/dial expectations.

### What you'll see

- Within ~500 ms after all three pods are reachable, an election
  fires. One pod becomes leader (visible via `cs.raft.is_leader=1`
  on its `/metrics`).
- Workers and the API continue to talk to whichever router pod
  the existing Service routes them to. Writes are auto-forwarded
  to the leader internally by raft-rs (M6.2 — no client-side
  redirect needed for the common path).
- A `kubectl delete pod cubestore-router-0` on the leader fires
  a re-election; a new leader emerges within ~5 s. In-flight
  writes either succeed within ~30 s or fail-fast within ~5 s.
  See the chaos-test deliverable in
  [`docs/ha/PLAN.md`](PLAN.md#verification).

### How to back out

If Phase 3 misbehaves (election storm, write latency spike, etc.):

1. Set `CUBESTORE_HA_MODE=off` on all three router pods.
2. Scale routers back to 1 (`replicas: 1`).
3. The single surviving router boots in non-HA mode against the
   same on-disk RocksDB. **No data migration needed** — the
   metadata that the Raft log applied is also written to the
   inner RocksMetaStore on every commit, so the post-rollback
   single-router sees the same state as the cluster's last
   leader.

The Raft log directory (`<data_dir>/raft-log/` by default) is
left in place but ignored in `HaMode::Off`. You can delete it
once you're certain the rollback is permanent.

## Useful runtime commands

Operator-facing introspection (any router pod):

```bash
# Who's leader right now?
curl -s http://router:9999/metrics | grep cs_raft_is_leader

# Term + commit + applied indices (single line per replica):
curl -s http://router:9999/metrics | grep -E '^cs_raft_(term|commit_index|applied_index)'
```

For the dashboard, see
[`docs/ha/grafana/`](grafana/).

## Known caveats (as of 2026-05)

- **Worker partition replication is NOT in v1**. Worker pods are
  still single-replica per partition. A worker pod death makes
  its partitions unqueryable until restart. Phase 2 of the HA
  roadmap addresses this.
- **All reads route to the leader** — no follower reads yet. In
  practice this is invisible because writes auto-forward to the
  leader (M6.2) and reads from any router are local.
- **Snapshot S3 upload** is not yet wired. Fresh joiners catch
  up via raft-rs's MsgSnapshot peer-to-peer path; if all routers
  are simultaneously offline you'd need to recover from the
  inner RocksMetaStore's existing checkpoint (the `metastore-current`
  blob), not from a separate raft snapshot. M5.x will wire this
  up — track the roadmap.

See [`HA.md`](../../HA.md#what-it-does-not-do-yet) for the full
"NOT done yet" list.
