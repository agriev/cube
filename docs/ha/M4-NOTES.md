# M4 — Multi-node clustering, leader election, follower replication

Closes the second milestone of the HA fork: the `RaftMetaStore` from M3
now runs as a real 3-node cluster with leader-elected writes,
replication to all followers, and partition-survival failover.

## Sub-milestone breakdown

| Sub | Scope | Tag |
|---|---|---|
| M4.1 | `Transport` trait + `Inbound` channel + `LocalLoopback` for tests | — |
| M4.2 | `RaftNode::start_multi_node` + persisted-messages send path + inbound `step` | — |
| M4.3 | 3-node cluster test: election + replication + apply convergence | — |
| M4.4 | Leader failover test via `LocalLoopback::unregister` partition | — |
| M4.5.1 | `HaPeer` parser + `CUBESTORE_RAFT_PEERS` / `CUBESTORE_RAFT_PORT` config | — |
| M4.5.2 | `TcpTransport` (production) + `spawn_listener` + 3-node-over-real-TCP test | — |
| M4.5.3 | `RaftMetaStore::start_multi_node` + boot wiring in `configure_meta_store` | — |
| M4 chaos | 20-cycle partition test, each round converges under 5s | `m4-complete` |

## Hard-won lesson: `take_messages` vs `take_persisted_messages`

The first 3-node test sat with all three nodes stuck as Candidate, term
climbing every election timeout, no progress.

raft-rs's `Ready::take_messages()` returns messages **only when the node
is the Leader**. For non-Leader (Follower / Candidate) the messages live
in `take_persisted_messages()` — they MUST be sent only after the
hard-state and entries are durably persisted. This is raft-thesis 10.2.1:
leaders can ship MsgAppend in parallel with their own persist, but
followers' MsgRequestVote and MsgAppendResponse have to follow persist
or vote correctness breaks.

Our M4.1 patch only drained `take_messages`. So `MsgRequestVote` from
candidates never left the loop, no peer ever voted, no quorum, infinite
election storm.

Fix: drain BOTH paths in `drive_ready` after persist. Drained 100% of
the messages the first vote round produced; quorum formed on the next
tick.

This bug is invisible without inter-node logging — it only manifests in
multi-node configs. Our single-node `start_with_storage` path used
`raw.campaign()` to bypass the entire election cycle and never
exercised the candidate→message-send→follower-step pipeline.

## Wire format (M4.5.2)

```
+----------+--------+----------+-----------------+
| magic u32| ver u32| len u32  | proto payload   |
+----------+--------+----------+-----------------+
```

All BE. Magic = `0xC8EAAF01` ("Cube HA / RAFT v1") so a non-raft client
(Cube SQL, k8s liveness probe HTTP GET) fails loud. Frame cap = 16 MiB
— heartbeat / append messages are << 1 KiB, so the cap exists only
for input validation. Snapshots that exceed it stream separately
(M5).

## Connection model

Per peer: at most one `TcpStream`, lazily dialed on first send. tokio
`Mutex` per connection serializes concurrent sends. On any I/O error
the stream is dropped; the next send reconnects. Reconnect cadence is
implicit: raft-rs's heartbeat tick (~150 ms) is the natural retry
interval.

We deliberately **don't** loop reconnects on the dialer side. A
reconnect storm against a dead peer would mask outages. Self-targeted
(`msg.to == 0`) and unknown-peer messages are silent no-ops — same
shape as a real network drop, which raft-rs handles via append/vote
retry.

`TCP_NODELAY` is set on every connection: Nagle on a 4-ms tick budget
would noticeably inflate failover time for small heartbeats.

## Boot-time peer-set invariant

Every node MUST list itself in `CUBESTORE_RAFT_PEERS`. If
`CUBESTORE_NODE_ID` is missing from the peer set, raft-rs has no
Progress entry for the local node and quorum math against a voter
set that doesn't include self silently breaks. We panic at boot
rather than let the cluster come up split-brain.

## Failover budget

Plan deliverable: "kill leader 100×, log indices converge in <5s".

The CI unit-test does 20 cycles × ≤5 s budget, finishes in ~3.7 s
total. Each round takes 100–800 ms in practice — well under the
budget. The full 100×, kubernetes-grade soak lives in M8.

## Open items into M5+

- Snapshot path: late-joining follower needs a state-machine
  snapshot, not just the log.
- Listener `JoinHandle` lifetime: detached on drop now. M9 will
  store the handle so observability can `.abort()` for graceful
  shutdown.
- Mid-flight ConfChange (add/remove peer at runtime): voters list is
  set once at boot. Raft-rs supports ConfChange entries through the
  log; we just don't expose the path yet. Operational story (M7)
  will add a small admin RPC.
