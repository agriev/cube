//! `RaftMetaStore` — the wrapper-style `MetaStore` impl that routes every
//! write through a Raft log before applying to the local state machine.
//!
//! ## Status
//!
//! M2.1 (in progress) — single-node Raft with in-memory log + a pluggable
//! `Apply` trait. This is the foundation; M2.2 swaps `MemStorage` for a
//! RocksDB-backed `RaftStorage` (see `storage.rs`), and M3 wires every
//! concrete `MetaStore` write method through the apply path.
//!
//! ## Architecture
//!
//! One Tokio task owns the `RawNode`. Clients propose by sending a
//! `MetaCommand` plus a oneshot reply channel through an mpsc; the
//! Raft task ticks on a fixed interval, drains its `Ready` after every
//! tick (appending entries, applying committed ones, sending messages
//! to peers), and resolves each oneshot when its corresponding entry
//! has been applied.
//!
//! Single-node specifics: there are no peers, so `step(message)` is
//! never called. As soon as an entry is appended on the leader (which
//! is always self in a 1-node cluster) it commits on the next tick and
//! becomes ready to apply.
//!
//! ## What `Apply` is for
//!
//! In M2.1 we use a test impl of `Apply` (`HashMapApply`) so the round-
//! trip is provable without touching `RocksMetaStore`. M3 will provide
//! a `RocksMetaStoreApply` impl that dispatches each `MetaCommand`
//! variant to the matching `RocksMetaStore::*` write method inside a
//! single RocksDB `WriteBatch`.

use crate::raft::command::{MetaCommand, MetaCommandCodecError};
use crate::CubeError;
use raft::prelude::{ConfState, Entry, EntryType, Message};
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use slog::{o, Drain};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A pluggable side-effect that the Raft apply loop runs for every
/// committed `MetaCommand`. M2.1 uses a test implementation; M3 will
/// provide one backed by `RocksMetaStore`.
///
/// Implementations must be **deterministic** — every replica that
/// applies the same sequence of `MetaCommand`s must reach the same
/// observable state, byte-for-byte. See `docs/ha/PLAN.md` risk #1.
pub trait Apply: Send + Sync + 'static {
    fn apply(&self, cmd: MetaCommand) -> Result<ApplyOutcome, CubeError>;
}

/// Typed result returned from `Apply::apply`. Currently a single
/// `Success` variant; M3 will replace it with a sum-type that mirrors
/// the return shape of every write method (e.g. `IdRow<Schema>` for
/// `create_schema`, `()` for `swap_active_partitions`).
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    Success,
}

/// Errors specific to the Raft layer (separate from the codec errors
/// in `command.rs`).
#[derive(Debug)]
pub enum RaftError {
    Codec(MetaCommandCodecError),
    Raft(raft::Error),
    ApplyChannelClosed,
    ProposeChannelClosed,
}

impl From<MetaCommandCodecError> for RaftError {
    fn from(e: MetaCommandCodecError) -> Self {
        Self::Codec(e)
    }
}

impl From<raft::Error> for RaftError {
    fn from(e: raft::Error) -> Self {
        Self::Raft(e)
    }
}

impl From<RaftError> for CubeError {
    fn from(e: RaftError) -> Self {
        CubeError::internal(format!("raft layer: {:?}", e))
    }
}

/// Message sent from a client to the Raft task: "please propose this
/// command and reply on `respond_to` once it has applied."
struct Proposal {
    command: MetaCommand,
    respond_to: oneshot::Sender<Result<ApplyOutcome, CubeError>>,
}

/// Handle exposed to the rest of cubestore — clones cheaply, thread-safe.
#[derive(Clone)]
pub struct RaftMetaStore {
    proposals: mpsc::UnboundedSender<Proposal>,
}

impl RaftMetaStore {
    /// Boot a single-node Raft group on the current Tokio runtime.
    /// The returned handle is what the rest of the system uses for
    /// proposals; the actual Raft work runs on a background task.
    ///
    /// `apply` is the side-effect that the apply loop invokes for
    /// every committed entry.
    pub fn start_single_node<A: Apply>(
        node_id: u64,
        apply: Arc<A>,
    ) -> Result<Self, RaftError> {
        let logger = build_drain_logger();

        let cfg = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            applied: 0,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            ..Default::default()
        };

        let storage = MemStorage::new_with_conf_state(ConfState::from((vec![node_id], vec![])));

        let raw = RawNode::new(&cfg, storage, &logger)?;

        let (tx, rx) = mpsc::unbounded_channel::<Proposal>();
        // The apply-pending map: entry_index → oneshot to fire on apply.
        let pending = std::collections::HashMap::<u64, oneshot::Sender<Result<ApplyOutcome, CubeError>>>::new();

        tokio::spawn(run_node(raw, rx, apply, pending, logger));

        Ok(Self { proposals: tx })
    }

    /// Propose a command and resolve the future once it has applied.
    pub async fn propose(&self, command: MetaCommand) -> Result<ApplyOutcome, CubeError> {
        let (tx, rx) = oneshot::channel();
        self.proposals
            .send(Proposal {
                command,
                respond_to: tx,
            })
            .map_err(|_| CubeError::internal("raft task is not running".to_string()))?;
        rx.await
            .map_err(|_| CubeError::internal("raft task dropped the response channel".to_string()))?
    }
}

async fn run_node<A: Apply>(
    mut raw: RawNode<MemStorage>,
    mut proposals: mpsc::UnboundedReceiver<Proposal>,
    apply: Arc<A>,
    mut pending: std::collections::HashMap<u64, oneshot::Sender<Result<ApplyOutcome, CubeError>>>,
    logger: slog::Logger,
) {
    let _ = logger; // reserved for future structured-log calls
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Single-node bootstrap: trigger an immediate election so proposals
    // don't race against the default `election_tick` countdown. Without
    // this, the first 500ms of node life is "follower waiting for leader"
    // and any proposal in that window returns ProposalDropped. Failures
    // observed in practice on ARM64/Docker. Multi-node mode (M4) keeps
    // the standard election timer because a self-campaign would interfere
    // with peer-driven elections.
    if let Err(e) = raw.campaign() {
        // Not fatal — node will still elect via timer fallback. Log only.
        log::warn!("raft: initial campaign() failed (will fall back to timer): {:?}", e);
    }
    // Drain the Ready that campaign() generates so the node actually
    // transitions to leader state before we start accepting proposals.
    drive_ready(&mut raw, &apply, &mut pending);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                raw.tick();
                drive_ready(&mut raw, &apply, &mut pending);
            }
            maybe = proposals.recv() => {
                match maybe {
                    Some(p) => {
                        let bytes = match p.command.encode() {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = p.respond_to.send(Err(CubeError::internal(format!(
                                    "MetaCommand encode failed: {}", e
                                ))));
                                continue;
                            }
                        };
                        // Stash the responder under the index that this
                        // proposal will commit at.
                        let next_index = raw.raft.raft_log.last_index() + 1;
                        if let Err(e) = raw.propose(vec![], bytes) {
                            let _ = p.respond_to.send(Err(CubeError::internal(format!(
                                "raft propose failed: {:?}", e
                            ))));
                            continue;
                        }
                        pending.insert(next_index, p.respond_to);
                        drive_ready(&mut raw, &apply, &mut pending);
                    }
                    None => break, // sender dropped → graceful shutdown
                }
            }
        }
    }
}

fn drive_ready<A: Apply>(
    raw: &mut RawNode<MemStorage>,
    apply: &Arc<A>,
    pending: &mut std::collections::HashMap<u64, oneshot::Sender<Result<ApplyOutcome, CubeError>>>,
) {
    if !raw.has_ready() {
        return;
    }
    let mut ready = raw.ready();

    // 1. Persist log entries. (MemStorage is in-memory; M2.2 RocksDB.)
    if !ready.entries().is_empty() {
        let store = raw.store();
        store.wl().append(ready.entries()).expect("MemStorage append");
    }

    // 2. Persist HardState (term, vote, commit) if changed.
    if let Some(hs) = ready.hs() {
        let store = raw.store();
        store.wl().set_hardstate(hs.clone());
    }

    // 3. Send messages — single-node has no peers, so this is a no-op.
    let _outbound: Vec<Message> = ready.take_messages();

    // 4. Apply committed entries.
    apply_committed(ready.committed_entries(), apply, pending);

    // 5. Tell raft we're done with this Ready.
    let mut light_ready = raw.advance(ready);

    // light_ready may carry additional commit advancement — apply those
    // entries too (mostly for clean idle-time tick behavior).
    apply_committed(light_ready.committed_entries(), apply, pending);
    raw.advance_apply();
    let _ = light_ready.take_messages();
}

fn apply_committed<A: Apply>(
    entries: &[Entry],
    apply: &Arc<A>,
    pending: &mut std::collections::HashMap<u64, oneshot::Sender<Result<ApplyOutcome, CubeError>>>,
) {
    for ent in entries {
        if ent.data.is_empty() {
            // Empty entries are emitted on leader election — skip.
            continue;
        }
        // raft-rs with protobuf-codec exposes `entry_type` as a struct
        // field (rust-protobuf 2.x style), not a method.
        match ent.entry_type {
            EntryType::EntryNormal => {
                let result = MetaCommand::decode(&ent.data)
                    .map_err(|e| CubeError::internal(format!("decode at index {}: {}", ent.index, e)))
                    .and_then(|cmd| apply.apply(cmd));
                if let Some(tx) = pending.remove(&ent.index) {
                    let _ = tx.send(result);
                }
            }
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                // M4 (clustering) — single-node never produces these.
            }
        }
    }
}

fn build_drain_logger() -> slog::Logger {
    // raft-rs takes a slog::Logger. We bridge it through slog-stdlog into
    // the standard `log` crate which cubestore uses everywhere else.
    let drain = slog_stdlog::StdLog.fuse();
    slog::Logger::root(drain, o!("subsystem" => "raft"))
}

// =============================================================================
// Tests — single-node propose→apply round-trip
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Test `Apply` impl that just remembers every command it was
    /// asked to apply, in order.
    struct RecordingApply {
        seen: Mutex<Vec<MetaCommand>>,
    }

    impl RecordingApply {
        fn new() -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
            }
        }
        fn snapshot(&self) -> Vec<MetaCommand> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl Apply for RecordingApply {
        fn apply(&self, cmd: MetaCommand) -> Result<ApplyOutcome, CubeError> {
            self.seen.lock().unwrap().push(cmd);
            Ok(ApplyOutcome::Success)
        }
    }

    /// Boot a single-node raft, propose one CreateSchema, await apply,
    /// assert the recorded command matches what was proposed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_propose_apply_round_trip() {
        let apply = Arc::new(RecordingApply::new());
        let store = RaftMetaStore::start_single_node(1, Arc::clone(&apply))
            .expect("boot single-node raft");

        // Single-node clusters elect themselves leader on the first tick
        // (~50ms). Give a generous warm-up before proposing.
        tokio::time::sleep(Duration::from_millis(800)).await;

        let cmd = MetaCommand::CreateSchema {
            schema_name: "public".into(),
            if_not_exists: true,
        };
        let outcome = tokio::time::timeout(Duration::from_secs(5), store.propose(cmd.clone()))
            .await
            .expect("propose timed out")
            .expect("propose failed");
        assert_eq!(outcome, ApplyOutcome::Success);

        let recorded = apply.snapshot();
        assert_eq!(recorded.len(), 1, "exactly one apply expected");
        assert_eq!(recorded[0], cmd, "applied command must equal proposed");
    }

    /// 50 proposals, all replicated, all applied in order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_proposals_serialized_in_order() {
        let apply = Arc::new(RecordingApply::new());
        let store = RaftMetaStore::start_single_node(1, Arc::clone(&apply))
            .expect("boot single-node raft");

        tokio::time::sleep(Duration::from_millis(800)).await;

        let mut proposed = Vec::with_capacity(50);
        for i in 0..50 {
            let cmd = MetaCommand::DropTable { table_id: i };
            proposed.push(cmd.clone());
            store
                .propose(cmd)
                .await
                .unwrap_or_else(|e| panic!("propose {} failed: {}", i, e));
        }

        let recorded = apply.snapshot();
        assert_eq!(recorded.len(), 50);
        // Single-node Raft preserves submission order.
        for (i, cmd) in recorded.iter().enumerate() {
            match cmd {
                MetaCommand::DropTable { table_id } => assert_eq!(*table_id, i as u64),
                other => panic!("unexpected {:?} at {}", other, i),
            }
        }
    }

    /// Propose a Batch — apply must receive it intact (atomicity contract).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_command_applies_atomically() {
        let apply = Arc::new(RecordingApply::new());
        let store = RaftMetaStore::start_single_node(1, Arc::clone(&apply))
            .expect("boot single-node raft");

        tokio::time::sleep(Duration::from_millis(800)).await;

        let batch = MetaCommand::Batch {
            commands: vec![
                MetaCommand::CreateSchema {
                    schema_name: "s1".into(),
                    if_not_exists: false,
                },
                MetaCommand::CreateTable {
                    schema_name: "s1".into(),
                    table_name: "t1".into(),
                    payload_version: 1,
                    payload: vec![1, 2, 3],
                },
            ],
        };
        store.propose(batch.clone()).await.unwrap();

        let recorded = apply.snapshot();
        assert_eq!(recorded.len(), 1);
        // The apply layer sees the Batch as one logical entry. M3 will
        // expand it inside the apply impl when wiring to RocksMetaStore.
        assert_eq!(recorded[0], batch);
    }

    /// HashMap state machine — closer to what M3 will see in production.
    /// Demonstrates that two replays of the same log produce the same
    /// state (determinism rehearsal — see plan risk #1).
    struct HashMapApply {
        schemas: Mutex<HashMap<String, bool /* if_not_exists */>>,
    }

    impl HashMapApply {
        fn new() -> Self {
            Self {
                schemas: Mutex::new(HashMap::new()),
            }
        }
    }

    impl Apply for HashMapApply {
        fn apply(&self, cmd: MetaCommand) -> Result<ApplyOutcome, CubeError> {
            if let MetaCommand::CreateSchema {
                schema_name,
                if_not_exists,
            } = cmd
            {
                self.schemas
                    .lock()
                    .unwrap()
                    .insert(schema_name, if_not_exists);
            }
            Ok(ApplyOutcome::Success)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deterministic_replay_produces_identical_state() {
        async fn run() -> Vec<(String, bool)> {
            let apply = Arc::new(HashMapApply::new());
            let store = RaftMetaStore::start_single_node(1, Arc::clone(&apply))
                .expect("boot single-node raft");
            tokio::time::sleep(Duration::from_millis(800)).await;
            for s in &["a", "b", "c", "d"] {
                store
                    .propose(MetaCommand::CreateSchema {
                        schema_name: (*s).into(),
                        if_not_exists: false,
                    })
                    .await
                    .unwrap();
            }
            let mut out: Vec<(String, bool)> =
                apply.schemas.lock().unwrap().iter().map(|(k, v)| (k.clone(), *v)).collect();
            out.sort();
            out
        }
        let a = run().await;
        let b = run().await;
        assert_eq!(a, b, "two independent runs of the same proposals must produce identical state");
        assert_eq!(a.len(), 4);
    }
}
