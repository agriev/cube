# Raft disk-full / fsync failure — runbook

Scope: `RaftMetaStore` running on the agriev/cube fork (`cubestored`
HA image). Three call sites in `raft/state_machine.rs::drive_ready`
panic on persistent storage failure:

- `panic!("raft storage apply_snapshot failed: …")` at ~line 556
- `panic!("raft storage append failed: …")` at ~line 570
- `panic!("raft storage set_hard_state failed: …")` at ~line 577

These panics are **correct by design** — they prevent committing
log entries that haven't reached durable storage. A panic crashes the
tokio task, which crashes the cube process, which trips the k8s
liveness probe and gets the pod restarted. No data loss; no silent
corruption.

When you see one of these crashes in a pod log, this runbook walks
through the recovery.

## 1. Detection

The chart's PrometheusRule (`cube-stack-ha`) ships an alert
`CubeStorePVCNearFull` that fires at 15% remaining. If you see that
alert OR a pod is in `CrashLoopBackOff` and the last log line is one
of the three panic strings above, you're in this runbook.

Quick triage:

```bash
NS=cube
SEL=app=cubestore-router

# How full are the PVCs?
kubectl -n $NS get pvc | grep cubestore-router

# Recent restarts + crash reason
kubectl -n $NS get pods -l $SEL -o wide
kubectl -n $NS logs <pod> --previous --tail 50 | grep -E 'panic|append failed|set_hard_state'
```

## 2. PVC sizing math

Cube Store's Raft log keeps every uncompacted entry. Compaction runs
every `M5.4_SNAPSHOT_INTERVAL` applied entries (default 10_000). At
typical write rates (50 ops/sec), the log is ~3 minutes deep before
the snapshot rolls.

Rough sizing:

```
PVC_size_GiB = max(
    20,                                     # floor for RocksDB SST + WAL
    snapshot_size_GiB * 3,                  # last + previous + headroom
    log_entry_size_KiB * 10_000 * 1.5 / 1_048_576
)
```

Default snapshot size on a busy metastore (~5 M chunks) is ~2 GiB, so
the headroom default of `50Gi` for the router PVC is comfortable. If
your alert fires anyway, the snapshot interval has slipped — see §4
below for forcing one.

## 3. Recovery — disk full, single pod

If only one router is full and the cluster still has quorum:

```bash
# 3a. Drain it (if not already CrashLoopBackOff'ing).
kubectl -n $NS cordon <node>     # only if the node itself is full
kubectl -n $NS delete pod <pod> --grace-period=30

# 3b. Resize the PVC. StorageClass must allow expansion (most do).
kubectl -n $NS patch pvc <pvc-name> --type='json' \
  -p='[{"op":"replace","path":"/spec/resources/requests/storage","value":"100Gi"}]'

# 3c. The StatefulSet recreates the pod with the larger volume.
# Raft re-fetches the snapshot from the leader; cluster re-converges.
kubectl -n $NS rollout status statefulset/<rel>-cubestore-router --timeout=10m

# 3d. Verify the cluster.
make -C charts/cube-stack-ha ha-verify    # one leader, term ≥ N
```

## 4. Recovery — all routers full, cluster wedged

If every router PVC is full simultaneously, Raft can't form a
quorum because the leader can't append the heartbeat. This is the
worst case.

```bash
# 4a. Suspend traffic so we don't pile on more writes.
kubectl -n $NS scale deploy/<rel>-api --replicas=0
kubectl -n $NS scale deploy/<rel>-refresh-worker --replicas=0

# 4b. Resize ALL router PVCs in parallel.
for i in 0 1 2; do
  kubectl -n $NS patch pvc data-<rel>-cubestore-router-${i} \
    --type='json' \
    -p='[{"op":"replace","path":"/spec/resources/requests/storage","value":"100Gi"}]'
done

# 4c. Bounce the whole router StatefulSet — the underlying volumes
# resize on remount.
kubectl -n $NS delete pod -l app=cubestore-router

# 4d. Wait for quorum + leader.
kubectl -n $NS rollout status statefulset/<rel>-cubestore-router --timeout=15m
make -C charts/cube-stack-ha ha-verify

# 4e. Resume traffic.
kubectl -n $NS scale deploy/<rel>-api --replicas=2
kubectl -n $NS scale deploy/<rel>-refresh-worker --replicas=1
```

If quorum **still won't form** after the resize, the snapshot file
itself may be corrupt mid-write (panic during `apply_snapshot`).
Restore from the most recent backup — see `docs/RESTORE.md`.

## 5. Prevention

- Provision PVCs at `2× expected snapshot size + 10 GiB`.
- Enable `CubeStorePVCNearFull` alert + page at 15%.
- Run the daily backup CronJob (chart's `cubestore.backup.enabled`).
- Set the kubelet eviction threshold so the kubelet doesn't OOM-kill
  the pod *before* Raft notices the disk-full and panics — panicking
  is preferable to a silent data race.
- Never disable fsync. The `CUBESTORE_NO_FSYNC=true` knob exists for
  benchmarks; it's correctness-unsafe in production.

## 6. fsync timeout (slow disk)

A different failure mode: fsync is **slow but not failing**. The
storage RwLock holds the guard across the rocksdb `write_opt`, so a
40-second fsync wedges the raft tick loop. Symptoms:

- `up{component="cubestore-router"}` stays 1, `cs_raft_is_leader`
  flaps between 0 and 1 every few seconds, term advances rapidly.
- `kubectl exec router-0 -- iostat -x 1` shows `await > 5000` (ms).

Fix: replace the disk class. The fsync wait is unavoidable as long as
the underlying volume is slow; PR-S2 splits the in-memory copy from
the persistent write so cube reads aren't blocked, but raft's
`set_hard_state` itself must complete before the leader can ack
commits.

## See also

- `docs/RESTORE.md` — full backup/restore procedure.
- `docs/ha/M3-NOTES.md` — determinism / leader-stamp design.
- `charts/cube-stack-ha/UPGRADE.md` — running upgrades through this.
