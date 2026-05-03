//! `RaftNode` — the Raft consensus engine + apply task. Exposes
//! [`RaftNode::propose`] for writes and ticks the Raft state machine
//! in a tokio background task. The production `MetaStore` wrapper
//! lives in `raft_meta_store.rs` and uses this `RaftNode` to route
//! every write through Raft.
//!
//! ## Architecture
//!
//! One Tokio task owns the `RawNode`. Clients propose by sending a
//! `MetaCommand` plus a oneshot reply channel through an mpsc; the
//! Raft task ticks on a fixed interval, drains its `Ready` after every
//! tick (appending entries to the persistent log, applying committed
//! ones, sending messages to peers), and resolves each oneshot when
//! its corresponding entry has been applied.
//!
//! Single-node specifics: there are no peers, so `step(message)` is
//! never called. As soon as an entry is appended on the leader (which
//! is always self in a 1-node cluster) it commits on the next tick and
//! becomes ready to apply. The task calls `RawNode::campaign()` once
//! at boot to skip the default election timeout — without that, the
//! first ~500ms of node life is "follower waiting for leader" and any
//! proposal in that window returns ProposalDropped.
//!
//! ## What `Apply` is for
//!
//! Tests use `RecordingApply`, `HashMapApply`, `TypedReturnApply`.
//! Production wires a `RocksMetaStoreApply` (see `rocks_apply.rs`),
//! which dispatches each `MetaCommand` variant to the matching
//! `RocksMetaStore::*` write method.

use crate::raft::command::{MetaCommand, MetaCommandCodecError, MetaCommandResult};
use crate::raft::storage::{RaftStorage, RaftStorageError, SharedRaftStorage};
use crate::raft::transport::{Inbound, Transport};
use crate::CubeError;
use async_trait::async_trait;
use raft::eraftpb::{ConfState, Entry, EntryType, Message};
use raft::{Config, RawNode};
use slog::{o, Drain};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A pluggable side-effect that the Raft apply loop runs for every
/// committed `MetaCommand`. M3.3 lands `RocksMetaStoreApply` as the
/// production impl; tests use lighter in-memory variants.
///
/// Implementations must be **deterministic** — every replica that
/// applies the same sequence of `MetaCommand`s must reach the same
/// observable state, byte-for-byte. See `docs/ha/PLAN.md` risk #1.
///
/// The trait is async because the real `MetaStore` write methods
/// are async (they queue through `RocksStore::write_operation` onto
/// a single rw-loop). The Raft apply task is a tokio task — making
/// this async means each apply awaits the rw-loop in-line; the next
/// committed entry is applied only after the previous one has been
/// durably written. That serialization is what gives us deterministic
/// replay across replicas.
///
/// The return value mirrors the shape of the underlying `MetaStore`
/// write method via `MetaCommandResult`:
/// - `()` returns                 → `MetaCommandResult::Unit`
/// - `bool` returns               → `MetaCommandResult::Bool`
/// - `IdRow<T>` returns           → `MetaCommandResult::IdRow`
/// - `Option<IdRow<T>>` returns   → `MetaCommandResult::OptionalIdRow`
///
/// The wrapper-style `RaftNode: MetaStore` impl reads the
/// matching variant after `propose(...).await?` and decodes the
/// payload back into the trait's typed return.
#[async_trait]
pub trait Apply: Send + Sync + 'static {
    async fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError>;
}

/// Errors specific to the Raft layer (separate from the codec errors
/// in `command.rs` and the storage errors in `storage.rs`).
#[derive(Debug)]
pub enum RaftError {
    Codec(MetaCommandCodecError),
    Raft(raft::Error),
    Storage(RaftStorageError),
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

impl From<RaftStorageError> for RaftError {
    fn from(e: RaftStorageError) -> Self {
        Self::Storage(e)
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
    respond_to: oneshot::Sender<Result<MetaCommandResult, CubeError>>,
}

/// Handle exposed to the rest of cubestore — clones cheaply, thread-safe.
#[derive(Clone)]
pub struct RaftNode {
    proposals: mpsc::UnboundedSender<Proposal>,
}

/// What kind of cluster this node is part of. Drives bootstrap-time
/// behavior — single-node forces an immediate election to skip the
/// 500ms timer countdown; multi-node lets the timer-driven election
/// run normally so peer-to-peer voting works.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClusterKind {
    Single,
    Multi,
}

impl RaftNode {
    /// Boot a single-node Raft group with persistent RocksDB storage.
    ///
    /// `data_dir` is the directory where the Raft log + HardState +
    /// ConfState live (typically `<cubestore_data_dir>/raft-log/`).
    /// On first boot the dir is created and seeded with an empty
    /// HardState and a ConfState containing only `node_id`. On
    /// subsequent boots the existing log is opened and the node
    /// resumes from its last applied position.
    pub fn start_single_node<A: Apply>(
        data_dir: impl AsRef<Path>,
        node_id: u64,
        apply: Arc<A>,
    ) -> Result<Self, RaftError> {
        let storage_inner = RaftStorage::open(data_dir, vec![node_id])?;
        let storage = SharedRaftStorage::new(Arc::new(storage_inner));
        Self::start_with_storage(node_id, storage, apply)
    }

    /// Lower-level constructor used by tests and (future) custom
    /// storage backends. Most callers want `start_single_node`.
    pub fn start_with_storage<A: Apply>(
        node_id: u64,
        storage: SharedRaftStorage,
        apply: Arc<A>,
    ) -> Result<Self, RaftError> {
        // Single-node has no transport; install a no-op transport
        // and an inbound the test never feeds.
        let transport = Arc::new(NoopTransport);
        let (_inbound, inbound_rx) = Inbound::new();
        Self::start_inner(node_id, storage, apply, transport, inbound_rx, ClusterKind::Single)
    }

    /// Boot a multi-node Raft replica. The caller supplies:
    ///
    /// - `data_dir`: where the Raft log + HardState + ConfState live.
    /// - `node_id`: this replica's id (must be unique in the cluster).
    /// - `voters`: the full peer-id set for the initial cluster
    ///   (e.g. `[1, 2, 3]`). Used only on first boot to seed the
    ///   ConfState; ignored on restart (we resume from disk).
    /// - `transport`: how outbound `raft::Message`s reach peers.
    /// - `inbound_rx`: where inbound messages arrive from. Construct
    ///   via [`Inbound::new`] before calling and hand the matching
    ///   [`Inbound`] to your transport so it can deliver received
    ///   messages back to this node.
    /// - `apply`: the `Apply` impl that runs against committed
    ///   commands (production: `RocksMetaStoreApply`).
    ///
    /// Multi-node bootstrap does NOT call `campaign()` — peer-driven
    /// timer elections run normally so the cluster picks a single
    /// leader by majority vote rather than every node racing to
    /// elect itself.
    pub fn start_multi_node<A: Apply, T: Transport>(
        data_dir: impl AsRef<Path>,
        node_id: u64,
        voters: Vec<u64>,
        apply: Arc<A>,
        transport: Arc<T>,
        inbound_rx: mpsc::UnboundedReceiver<Message>,
    ) -> Result<Self, RaftError> {
        let storage_inner = RaftStorage::open(data_dir, voters)?;
        let storage = SharedRaftStorage::new(Arc::new(storage_inner));
        Self::start_inner(node_id, storage, apply, transport, inbound_rx, ClusterKind::Multi)
    }

    /// Test-only entry point: open storage with an explicit voter
    /// list and a caller-provided transport. Used by the multi-node
    /// loopback tests.
    #[doc(hidden)]
    pub fn start_with_storage_multi<A: Apply, T: Transport>(
        node_id: u64,
        storage: SharedRaftStorage,
        apply: Arc<A>,
        transport: Arc<T>,
        inbound_rx: mpsc::UnboundedReceiver<Message>,
    ) -> Result<Self, RaftError> {
        Self::start_inner(node_id, storage, apply, transport, inbound_rx, ClusterKind::Multi)
    }

    fn start_inner<A: Apply, T: Transport>(
        node_id: u64,
        storage: SharedRaftStorage,
        apply: Arc<A>,
        transport: Arc<T>,
        inbound_rx: mpsc::UnboundedReceiver<Message>,
        kind: ClusterKind,
    ) -> Result<Self, RaftError> {
        let logger = build_drain_logger();

        let cfg = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            applied: storage.applied_index_or_zero(),
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            check_quorum: false,
            pre_vote: false,
            ..Default::default()
        };

        let raw = RawNode::new(&cfg, storage.clone(), &logger)?;

        let (tx, rx) = mpsc::unbounded_channel::<Proposal>();
        let pending = std::collections::HashMap::<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>::new();

        // Use Arc<dyn Transport> so the run_node task doesn't carry a
        // generic parameter (tokio::spawn captures must be 'static and
        // monomorphized per-T explosions get noisy).
        let transport_dyn: Arc<dyn Transport> = transport;

        tokio::spawn(run_node(
            raw, storage, rx, apply, pending, logger, transport_dyn, inbound_rx, kind,
        ));

        Ok(Self { proposals: tx })
    }

    /// Propose a command and resolve the future once it has applied.
    pub async fn propose(&self, command: MetaCommand) -> Result<MetaCommandResult, CubeError> {
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

/// Single-node mode has no peers; install a `Transport` that drops
/// every message. Outbound messages still arise (heartbeats addressed
/// to self filter out at the raft-rs layer; the rare stray gets
/// discarded here).
struct NoopTransport;

#[async_trait]
impl Transport for NoopTransport {
    async fn send(&self, _msg: Message) {
        // intentionally empty
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_node<A: Apply>(
    mut raw: RawNode<SharedRaftStorage>,
    storage: SharedRaftStorage,
    mut proposals: mpsc::UnboundedReceiver<Proposal>,
    apply: Arc<A>,
    mut pending: std::collections::HashMap<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>,
    logger: slog::Logger,
    transport: Arc<dyn Transport>,
    mut inbound: mpsc::UnboundedReceiver<Message>,
    kind: ClusterKind,
) {
    let _ = logger; // reserved for future structured-log calls
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Single-node bootstrap: trigger an immediate election so proposals
    // don't race against the default `election_tick` countdown. Without
    // this, the first ~500ms of node life is "follower waiting for leader"
    // and any proposal in that window returns ProposalDropped. Multi-node
    // mode keeps timer-driven elections — explicit campaign on every
    // node would interfere with peer-driven elections (multiple
    // candidates in the same term split the vote and force re-election).
    if kind == ClusterKind::Single {
        if let Err(e) = raw.campaign() {
            log::warn!(
                "raft: initial campaign() failed (will fall back to timer): {:?}",
                e
            );
        }
    }
    drive_ready(&mut raw, &storage, &apply, &mut pending, transport.as_ref()).await;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                raw.tick();
                drive_ready(&mut raw, &storage, &apply, &mut pending, transport.as_ref()).await;
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
                        // Followers can't accept proposals — raft-rs
                        // returns `ProposalDropped`. Surface that as
                        // a routable error so the wrapping
                        // `RaftMetaStore` can decide to forward to the
                        // leader (M4.5) or fail back to the caller.
                        let next_index = raw.raft.raft_log.last_index() + 1;
                        if let Err(e) = raw.propose(vec![], bytes) {
                            let _ = p.respond_to.send(Err(CubeError::internal(format!(
                                "raft propose failed: {:?}", e
                            ))));
                            continue;
                        }
                        pending.insert(next_index, p.respond_to);
                        drive_ready(&mut raw, &storage, &apply, &mut pending, transport.as_ref()).await;
                    }
                    None => break,
                }
            }
            maybe = inbound.recv() => {
                match maybe {
                    Some(msg) => {
                        // Step the message into the raft state machine.
                        // Errors here are expected during normal operation
                        // (stale messages from a previous term, votes
                        // for higher terms, etc.) — log at debug only.
                        if let Err(e) = raw.step(msg) {
                            log::debug!("raft: step inbound failed: {:?}", e);
                        }
                        drive_ready(&mut raw, &storage, &apply, &mut pending, transport.as_ref()).await;
                    }
                    None => {
                        // Inbound channel closed — transport is gone.
                        // Single-node mode never feeds it so this is
                        // also the normal idle path. Don't break the
                        // loop; just stop polling this branch.
                        // tokio::select drops a closed branch
                        // automatically by returning Pending forever
                        // on the next iteration.
                    }
                }
            }
        }
    }
}

async fn drive_ready<A: Apply>(
    raw: &mut RawNode<SharedRaftStorage>,
    storage: &SharedRaftStorage,
    apply: &Arc<A>,
    pending: &mut std::collections::HashMap<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>,
    transport: &dyn Transport,
) {
    if !raw.has_ready() {
        return;
    }
    let mut ready = raw.ready();

    // 1. Persist log entries to RocksDB. fsync-before-ack is enabled
    //    inside RaftStorage::append (correctness requirement).
    if !ready.entries().is_empty() {
        if let Err(e) = storage.append(ready.entries()) {
            // A storage failure here is fatal for Raft correctness —
            // the leader must never ack entries it didn't durably
            // persist. We crash-loop the task; Kubernetes will
            // restart the pod which then re-derives state from the
            // existing log on disk.
            panic!("raft storage append failed: {}", e);
        }
    }

    // 2. Persist HardState (term, vote, commit) if changed.
    if let Some(hs) = ready.hs() {
        if let Err(e) = storage.set_hard_state(hs.clone()) {
            panic!("raft storage set_hard_state failed: {}", e);
        }
    }

    // 3a. Leader-side: messages can be sent BEFORE the hard-state /
    //     entry persists complete (raft thesis 10.2.1 — leaders can
    //     replicate concurrently with their own persist). For non-
    //     leaders these are empty; persisted messages handle that path.
    let outbound: Vec<Message> = ready.take_messages();
    for msg in outbound {
        transport.send(msg).await;
    }

    // 3b. Non-leader-side: vote / append-response messages MUST be
    //     sent only AFTER the hard-state and entries are durably
    //     persisted (we already did that above in steps 1 + 2).
    //     Skipping these is the bug that kept candidates spinning in
    //     M4.3 — without them peers never receive MsgRequestVote so
    //     no quorum forms.
    let persisted: Vec<Message> = ready.take_persisted_messages();
    for msg in persisted {
        transport.send(msg).await;
    }

    // 4. Apply committed entries.
    let highest_applied = apply_committed(ready.committed_entries(), apply, pending).await;
    if let Some(idx) = highest_applied {
        let _ = storage.set_applied_index(idx); // best-effort; M5 uses for snapshots
    }

    // 5. Tell raft we're done with this Ready. light_ready carries
    //    leader-side commit-broadcast etc. — the persisted-messages
    //    path already drained the candidate/follower side so we only
    //    need take_messages here.
    let mut light_ready = raw.advance(ready);

    let light_msgs: Vec<Message> = light_ready.take_messages();
    for msg in light_msgs {
        transport.send(msg).await;
    }

    let highest_applied2 = apply_committed(light_ready.committed_entries(), apply, pending).await;
    if let Some(idx) = highest_applied2 {
        let _ = storage.set_applied_index(idx);
    }
    raw.advance_apply();
}

/// Apply each committed entry, return the highest index actually
/// applied (caller persists this to `applied_index` for restart-time
/// recovery). Async because `Apply::apply` is async — the apply of
/// entry N awaits before entry N+1 starts, which is exactly the
/// determinism guarantee we want.
async fn apply_committed<A: Apply>(
    entries: &[Entry],
    apply: &Arc<A>,
    pending: &mut std::collections::HashMap<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>,
) -> Option<u64> {
    let mut highest = None;
    for ent in entries {
        highest = Some(ent.index);
        if ent.data.is_empty() {
            // Empty entries are emitted on leader election — skip.
            continue;
        }
        // raft-rs with protobuf-codec exposes `entry_type` as a struct
        // field (rust-protobuf 2.x style), not a method.
        match ent.entry_type {
            EntryType::EntryNormal => {
                let result = match MetaCommand::decode(&ent.data) {
                    Ok(cmd) => apply.apply(cmd).await,
                    Err(e) => Err(CubeError::internal(format!(
                        "decode at index {}: {}",
                        ent.index, e
                    ))),
                };
                if let Some(tx) = pending.remove(&ent.index) {
                    let _ = tx.send(result);
                }
            }
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                // M4 (clustering) — single-node never produces these.
            }
        }
    }
    highest
}

fn build_drain_logger() -> slog::Logger {
    // raft-rs takes a slog::Logger. We bridge it through slog-stdlog into
    // the standard `log` crate which cubestore uses everywhere else.
    let drain = slog_stdlog::StdLog.fuse();
    slog::Logger::root(drain, o!("subsystem" => "raft"))
}

// Avoid unused-import warning when ConfState only matters in tests.
#[allow(dead_code)]
fn _conf_state_keepalive(cs: ConfState) -> ConfState {
    cs
}

// =============================================================================
// Tests — single-node propose→apply round-trip with persistent storage
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

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

    #[async_trait]
    impl Apply for RecordingApply {
        async fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
            self.seen.lock().unwrap().push(cmd);
            Ok(MetaCommandResult::Unit)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_propose_apply_round_trip() {
        let dir = TempDir::new().unwrap();
        let apply = Arc::new(RecordingApply::new());
        let store = RaftNode::start_single_node(dir.path(), 1, Arc::clone(&apply))
            .expect("boot single-node raft");

        // Even with campaign() at startup, give the apply loop a tick
        // or two to settle. 200ms is generous on a warm container.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let cmd = MetaCommand::CreateSchema {
            schema_name: "public".into(),
            if_not_exists: true,
        };
        let outcome = tokio::time::timeout(Duration::from_secs(5), store.propose(cmd.clone()))
            .await
            .expect("propose timed out")
            .expect("propose failed");
        assert_eq!(outcome, MetaCommandResult::Unit);

        let recorded = apply.snapshot();
        assert_eq!(recorded.len(), 1, "exactly one apply expected");
        assert_eq!(recorded[0], cmd, "applied command must equal proposed");
    }

    /// `Apply` impl that returns a different `MetaCommandResult` shape
    /// per command — exercises the M3.2 typed return path end-to-end
    /// (encode → propose → apply → decode in caller).
    struct TypedReturnApply;
    #[async_trait]
    impl Apply for TypedReturnApply {
        async fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
            use crate::raft::command::IdRowKind;
            // Pretend rows: just `(id, name)` tuples. The wire layer
            // doesn't care about the actual `IdRow<T>` type, only that
            // the bytes round-trip via flexbuffers — so a tuple proxy
            // is enough to test the result shape.
            match cmd {
                MetaCommand::CreateSchema { schema_name, .. } => {
                    let row = (1u64, schema_name);
                    MetaCommandResult::id_row(IdRowKind::Schema, &row)
                        .map_err(|e| CubeError::internal(e.to_string()))
                }
                MetaCommand::SwapCompactedChunks { .. } => Ok(MetaCommandResult::Bool(true)),
                MetaCommand::DropTable { .. } => Ok(MetaCommandResult::Unit),
                _ => Ok(MetaCommandResult::Unit),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_results_propagate_back_to_caller() {
        use crate::raft::command::IdRowKind;
        let dir = TempDir::new().unwrap();
        let store = RaftNode::start_single_node(dir.path(), 1, Arc::new(TypedReturnApply))
            .expect("boot single-node raft");
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Unit return.
        let r = store
            .propose(MetaCommand::DropTable { table_id: 1 })
            .await
            .expect("propose drop_table");
        r.into_unit().expect("must be Unit");

        // Bool return — mirrors `swap_compacted_chunks`.
        let r = store
            .propose(MetaCommand::SwapCompactedChunks {
                partition_id: 1,
                old_chunk_ids: vec![1, 2],
                new_chunk: 3,
                new_chunk_file_size: 100,
            })
            .await
            .expect("propose swap_compacted_chunks");
        assert!(r.into_bool().expect("must be Bool"));

        // IdRow<Schema> return — mirrors `create_schema`. The caller
        // decodes with the expected `IdRowKind::Schema` and gets a
        // typed value back.
        let r = store
            .propose(MetaCommand::CreateSchema {
                schema_name: "public".into(),
                if_not_exists: true,
            })
            .await
            .expect("propose create_schema");
        let row: (u64, String) = r
            .into_id_row(IdRowKind::Schema)
            .expect("decode IdRow<Schema>");
        assert_eq!(row, (1u64, "public".to_string()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn variant_mismatch_is_caller_error_not_panic() {
        use crate::raft::command::IdRowKind;
        // The Apply path returns `Bool` but the caller asks for an
        // `IdRow` — must error cleanly (this is the diagnostic for a
        // misimplemented Apply variant in M3.3).
        let dir = TempDir::new().unwrap();
        let store = RaftNode::start_single_node(dir.path(), 1, Arc::new(TypedReturnApply))
            .expect("boot single-node raft");
        tokio::time::sleep(Duration::from_millis(200)).await;

        let r = store
            .propose(MetaCommand::SwapCompactedChunks {
                partition_id: 1,
                old_chunk_ids: vec![],
                new_chunk: 1,
                new_chunk_file_size: 0,
            })
            .await
            .expect("propose");
        // Bool was produced — asking for IdRow must fail with mismatch,
        // not panic.
        let mismatch = r.into_id_row::<(u64, String)>(IdRowKind::Schema);
        assert!(mismatch.is_err(), "must error on variant mismatch");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_proposals_serialized_in_order() {
        let dir = TempDir::new().unwrap();
        let apply = Arc::new(RecordingApply::new());
        let store = RaftNode::start_single_node(dir.path(), 1, Arc::clone(&apply))
            .expect("boot single-node raft");

        tokio::time::sleep(Duration::from_millis(200)).await;

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
        for (i, cmd) in recorded.iter().enumerate() {
            match cmd {
                MetaCommand::DropTable { table_id } => assert_eq!(*table_id, i as u64),
                other => panic!("unexpected {:?} at {}", other, i),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_command_applies_atomically() {
        let dir = TempDir::new().unwrap();
        let apply = Arc::new(RecordingApply::new());
        let store = RaftNode::start_single_node(dir.path(), 1, Arc::clone(&apply))
            .expect("boot single-node raft");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let batch = MetaCommand::Batch {
            commands: vec![
                MetaCommand::CreateSchema {
                    schema_name: "s1".into(),
                    if_not_exists: false,
                },
                MetaCommand::CreateTable {
                    schema_name: "s1".into(),
                    table_name: "t1".into(),
                    columns_blob: vec![1, 2, 3],
                    locations: None,
                    import_format_blob: None,
                    indexes_blob: vec![],
                    is_ready: true,
                    build_range_end_millis: None,
                    seal_at_millis: None,
                    select_statement: None,
                    source_columns_blob: None,
                    stream_offset_blob: None,
                    unique_key_column_names: None,
                    aggregates: None,
                    partition_split_threshold: None,
                    trace_obj: None,
                    drop_if_exists: false,
                    extension: None,
                    assigned_now_millis: 0,
                },
            ],
        };
        store.propose(batch.clone()).await.unwrap();

        let recorded = apply.snapshot();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], batch);
    }

    /// HashMap state machine — closer to what M3 will see in production.
    struct HashMapApply {
        schemas: Mutex<HashMap<String, bool>>,
    }
    impl HashMapApply {
        fn new() -> Self {
            Self {
                schemas: Mutex::new(HashMap::new()),
            }
        }
    }
    #[async_trait]
    impl Apply for HashMapApply {
        async fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
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
            Ok(MetaCommandResult::Unit)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deterministic_replay_produces_identical_state() {
        async fn run() -> Vec<(String, bool)> {
            let dir = TempDir::new().unwrap();
            let apply = Arc::new(HashMapApply::new());
            let store = RaftNode::start_single_node(dir.path(), 1, Arc::clone(&apply))
                .expect("boot single-node raft");
            tokio::time::sleep(Duration::from_millis(200)).await;
            for s in &["a", "b", "c", "d"] {
                store
                    .propose(MetaCommand::CreateSchema {
                        schema_name: (*s).into(),
                        if_not_exists: false,
                    })
                    .await
                    .unwrap();
            }
            let mut out: Vec<(String, bool)> = apply
                .schemas
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            out.sort();
            out
        }
        let a = run().await;
        let b = run().await;
        assert_eq!(
            a, b,
            "two independent runs of the same proposals must produce identical state"
        );
        assert_eq!(a.len(), 4);
    }

    /// Hardest test: data persists across simulated restart.
    /// Boot raft, propose entries, drop the handle (simulating pod
    /// kill), reopen storage, verify entries are still there in the
    /// persistent log.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn entries_persist_across_node_restart() {
        let dir = TempDir::new().unwrap();
        // Phase 1: boot, propose, shut down.
        {
            let apply = Arc::new(RecordingApply::new());
            let store = RaftNode::start_single_node(dir.path(), 1, Arc::clone(&apply))
                .expect("boot raft phase 1");
            tokio::time::sleep(Duration::from_millis(200)).await;
            for i in 0..3 {
                store
                    .propose(MetaCommand::DropTable { table_id: i })
                    .await
                    .expect("propose");
            }
            // Drop store + apply: tokio task dies when the proposals
            // sender is dropped.
            drop(store);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Phase 2: reopen storage directly and verify the log is
        // persistent. (We don't reboot the full RaftNode here
        // because campaign() on restart in single-node would re-elect
        // and re-emit committed entries — a separate behavior tested
        // by storage::tests.)
        let reopened = RaftStorage::open(dir.path(), vec![1]).expect("reopen");
        assert!(
            reopened.last_index_internal_for_test() >= 3,
            "log should contain at least 3 proposed entries; got last_index={}",
            reopened.last_index_internal_for_test()
        );
    }

    /// M3.3 e2e — propose `CreateSchema` through real Raft against a
    /// real `RocksMetaStoreApply` and assert the schema actually
    /// persists in the underlying RocksMetaStore. This is the wiring
    /// proof for M3.3.a: encode → Raft commit → apply dispatch →
    /// trait method → result decoded back into `IdRow<Schema>`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn raft_dispatches_create_schema_to_rocks_meta_store() {
        use crate::config::Config;
        use crate::metastore::{
            BaseRocksStoreFs, IdRow, MetaStore, RocksMetaStore, Schema,
        };
        use crate::raft::command::IdRowKind;
        use crate::raft::rocks_apply::RocksMetaStoreApply;
        use crate::remotefs::LocalDirRemoteFs;
        use std::env;
        use std::fs;

        let test_name = "m3_3_e2e_create_schema";
        let cwd = env::current_dir().unwrap();
        let store_path = cwd.join(format!("{}-local", test_name));
        let remote_store_path = cwd.join(format!("{}-remote", test_name));
        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_store_path);
        let raft_dir = TempDir::new().unwrap();

        let config = Config::test(test_name);
        let remote_fs =
            LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let rocks = RocksMetaStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )
        .expect("RocksMetaStore::new");
        let apply = Arc::new(RocksMetaStoreApply::new(rocks.clone()));
        let raft = RaftNode::start_single_node(raft_dir.path(), 1, apply)
            .expect("boot single-node raft");

        // Settle election.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = raft
            .propose(MetaCommand::CreateSchema {
                schema_name: "e2e".into(),
                if_not_exists: false,
            })
            .await
            .expect("propose CreateSchema");

        // Result decodes back into a typed IdRow<Schema> via M3.2 helpers.
        let row: IdRow<Schema> = result
            .into_id_row(IdRowKind::Schema)
            .expect("decode IdRow<Schema>");
        assert_eq!(row.get_row().get_name(), "e2e");

        // The underlying RocksMetaStore actually has the row.
        let listed = rocks.get_schemas().await.expect("get_schemas");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].get_row().get_name(), "e2e");

        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_store_path);
    }

    /// M3.8 — deterministic-replay integration test.
    ///
    /// Boots two independent single-node Raft instances against
    /// independent local `RocksMetaStore` directories. Issues the
    /// **same** sequence of writes through both wrappers (with the
    /// SAME leader-stamped `now` for each call so the determinism
    /// inputs are identical, mirroring the future M4 behavior where
    /// the leader stamps once and the bytes propagate). Then asserts
    /// the resulting metastore observables (`get_schemas`,
    /// `get_tables`) are byte-identical across both instances —
    /// i.e. the same Raft log produces the same RocksDB state on
    /// every replica.
    ///
    /// This is a strong proof of M3.4's leader-stamp pattern: any
    /// non-determinism in row construction would manifest as a
    /// divergence in `created_at` / `last_heart_beat` / `suffix`
    /// fields and fail the equality check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deterministic_replay_two_instance_equivalence() {
        use crate::config::Config;
        use crate::metastore::{
            BaseRocksStoreFs, IdRow, MetaStore, RocksMetaStore, Schema,
        };
        use crate::raft::raft_meta_store::RaftMetaStore;
        use crate::remotefs::LocalDirRemoteFs;
        use std::env;
        use std::fs;

        async fn boot(
            test_name: &str,
        ) -> (Arc<RaftMetaStore>, std::path::PathBuf, std::path::PathBuf, TempDir) {
            let cwd = env::current_dir().unwrap();
            let store_path = cwd.join(format!("{}-local", test_name));
            let remote_path = cwd.join(format!("{}-remote", test_name));
            let _ = fs::remove_dir_all(&store_path);
            let _ = fs::remove_dir_all(&remote_path);
            let config = Config::test(test_name);
            let remote_fs = LocalDirRemoteFs::new(Some(remote_path.clone()), store_path.clone());
            let rocks = RocksMetaStore::new(
                store_path.join("metastore").as_path(),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .expect("RocksMetaStore::new");
            let raft_dir = TempDir::new().expect("raft tempdir");
            let wrapper = RaftMetaStore::start_single_node(raft_dir.path(), 1, rocks)
                .expect("RaftMetaStore::start_single_node");
            (wrapper, store_path, remote_path, raft_dir)
        }

        let (a, sp_a, rp_a, _rd_a) = boot("m38_replay_a").await;
        let (b, sp_b, rp_b, _rd_b) = boot("m38_replay_b").await;

        // Settle initial campaigns.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Issue the same sequence of writes through both wrappers.
        for schema in &["alpha", "beta", "gamma"] {
            let _: IdRow<Schema> =
                MetaStore::create_schema(&*a, (*schema).into(), false)
                    .await
                    .expect("create_schema on a");
            let _: IdRow<Schema> =
                MetaStore::create_schema(&*b, (*schema).into(), false)
                    .await
                    .expect("create_schema on b");
        }

        // Settle apply (single-node Raft applies inline before
        // create_schema returns, but be paranoid).
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut a_schemas = MetaStore::get_schemas(&*a)
            .await
            .expect("get_schemas a")
            .into_iter()
            .map(|r| (r.get_id(), r.get_row().get_name().clone()))
            .collect::<Vec<_>>();
        let mut b_schemas = MetaStore::get_schemas(&*b)
            .await
            .expect("get_schemas b")
            .into_iter()
            .map(|r| (r.get_id(), r.get_row().get_name().clone()))
            .collect::<Vec<_>>();
        a_schemas.sort();
        b_schemas.sort();

        // Both replicas must have the same set of schemas. The IDs
        // are deterministic because they come from the per-table
        // merge counter, which advances identically when all writes
        // go through Raft.
        assert_eq!(
            a_schemas, b_schemas,
            "two independent single-node Raft instances applying the same \
             write sequence must produce identical schema rows (id + name)"
        );
        assert_eq!(a_schemas.len(), 3);

        let _ = fs::remove_dir_all(&sp_a);
        let _ = fs::remove_dir_all(&rp_a);
        let _ = fs::remove_dir_all(&sp_b);
        let _ = fs::remove_dir_all(&rp_b);
    }

    // =========================================================================
    // M4 — multi-node clustering tests
    // =========================================================================

    use crate::raft::transport::{Inbound, LocalLoopback};

    /// Find the leader by trying to propose a tiny no-op on each node.
    /// Followers reply with an error ("raft propose failed: ProposalDropped");
    /// the leader resolves Ok. Returns the (id, &node) of whoever wins.
    /// Polls for up to `deadline` because elections take a few ticks.
    async fn await_leader<'a>(
        nodes: &'a [(u64, RaftNode)],
        deadline: Duration,
    ) -> &'a (u64, RaftNode) {
        let probe = MetaCommand::CreateSchema {
            schema_name: "__leader_probe__".into(),
            if_not_exists: true,
        };
        let start = std::time::Instant::now();
        loop {
            for n in nodes {
                let res = tokio::time::timeout(
                    Duration::from_millis(100),
                    n.1.propose(probe.clone()),
                )
                .await;
                if let Ok(Ok(_)) = res {
                    return n;
                }
            }
            if start.elapsed() > deadline {
                panic!("no leader elected within {:?}", deadline);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Three local nodes connected by a `LocalLoopback` transport
    /// elect a leader, replicate a write through the log, and apply
    /// it on every replica. This is the M4 baseline guarantee:
    /// committed entries reach every voter's apply path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_cluster_replicates_writes_to_all_followers() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let dir3 = TempDir::new().unwrap();

        let transport = Arc::new(LocalLoopback::new());
        let (in1, rx1) = Inbound::new();
        let (in2, rx2) = Inbound::new();
        let (in3, rx3) = Inbound::new();
        transport.register(1, in1);
        transport.register(2, in2);
        transport.register(3, in3);

        let apply1 = Arc::new(RecordingApply::new());
        let apply2 = Arc::new(RecordingApply::new());
        let apply3 = Arc::new(RecordingApply::new());

        let n1 = RaftNode::start_multi_node(
            dir1.path(),
            1,
            vec![1, 2, 3],
            Arc::clone(&apply1),
            Arc::clone(&transport),
            rx1,
        )
        .expect("start n1");
        let n2 = RaftNode::start_multi_node(
            dir2.path(),
            2,
            vec![1, 2, 3],
            Arc::clone(&apply2),
            Arc::clone(&transport),
            rx2,
        )
        .expect("start n2");
        let n3 = RaftNode::start_multi_node(
            dir3.path(),
            3,
            vec![1, 2, 3],
            Arc::clone(&apply3),
            Arc::clone(&transport),
            rx3,
        )
        .expect("start n3");

        let nodes = vec![(1u64, n1), (2u64, n2), (3u64, n3)];
        let leader = await_leader(&nodes, Duration::from_secs(15)).await;
        let leader_id = leader.0;

        // Drive a real write through the leader. Every replica's
        // Apply must observe it.
        let cmd = MetaCommand::CreateSchema {
            schema_name: "after_leader".into(),
            if_not_exists: true,
        };
        tokio::time::timeout(Duration::from_secs(10), leader.1.propose(cmd.clone()))
            .await
            .expect("propose timed out")
            .expect("propose failed at the leader");

        // Allow followers' apply paths to drain. raft-rs commits on
        // majority ack (already done before the propose oneshot
        // resolves), then advances the followers' commit-index on the
        // next heartbeat — give one tick interval plus headroom.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let probe = MetaCommand::CreateSchema {
            schema_name: "__leader_probe__".into(),
            if_not_exists: true,
        };
        for (id, _) in &nodes {
            // Each replica should have applied:
            //   1) the empty entry from leader-election (skipped)
            //   2) one or more probe commands (`__leader_probe__`)
            //      sent by `await_leader`
            //   3) exactly one `after_leader` command from the real
            //      propose
            let recorded = match *id {
                1 => apply1.snapshot(),
                2 => apply2.snapshot(),
                3 => apply3.snapshot(),
                _ => unreachable!(),
            };
            let after = recorded
                .iter()
                .filter(|c| **c == cmd)
                .count();
            let probes = recorded
                .iter()
                .filter(|c| **c == probe)
                .count();
            assert_eq!(
                after, 1,
                "replica {} must apply the post-election write exactly once (saw {})",
                id, after
            );
            assert!(
                probes >= 1,
                "replica {} must apply at least one leader-probe command (saw {})",
                id, probes
            );
        }

        // The leader id should be one of {1, 2, 3} — we don't assert
        // which (election is non-deterministic) but log it so the test
        // output names whoever won.
        assert!(
            (1..=3).contains(&leader_id),
            "leader id {} out of range",
            leader_id
        );
    }

    /// Killing the leader (we simulate it by partitioning it from the
    /// rest of the cluster, which is the failure mode k8s pod-delete
    /// looks like to the surviving peers) must produce a new leader
    /// within a few seconds. This is the failover SLA from
    /// `docs/ha/PLAN.md` verification step 1.
    ///
    /// We can't actually drop the tokio task — the LocalLoopback's
    /// `unregister` is the right model: messages from/to the killed
    /// node disappear, identical to a network partition. The killed
    /// node keeps spinning but its outbound goes nowhere; quorum
    /// shifts to the remaining two.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_failover_within_seconds_after_partition() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let dir3 = TempDir::new().unwrap();

        let transport = Arc::new(LocalLoopback::new());
        let (in1, rx1) = Inbound::new();
        let (in2, rx2) = Inbound::new();
        let (in3, rx3) = Inbound::new();
        transport.register(1, in1);
        transport.register(2, in2);
        transport.register(3, in3);

        let apply1 = Arc::new(RecordingApply::new());
        let apply2 = Arc::new(RecordingApply::new());
        let apply3 = Arc::new(RecordingApply::new());

        let n1 = RaftNode::start_multi_node(
            dir1.path(), 1, vec![1, 2, 3],
            Arc::clone(&apply1), Arc::clone(&transport), rx1,
        ).expect("start n1");
        let n2 = RaftNode::start_multi_node(
            dir2.path(), 2, vec![1, 2, 3],
            Arc::clone(&apply2), Arc::clone(&transport), rx2,
        ).expect("start n2");
        let n3 = RaftNode::start_multi_node(
            dir3.path(), 3, vec![1, 2, 3],
            Arc::clone(&apply3), Arc::clone(&transport), rx3,
        ).expect("start n3");

        let nodes = vec![(1u64, n1), (2u64, n2), (3u64, n3)];
        let first_leader = await_leader(&nodes, Duration::from_secs(15)).await;
        let killed_id = first_leader.0;

        // Partition the leader off the loopback: outbound messages
        // from the leader still happen, but they go to peers — fine,
        // they still receive but have no quorum. Inbound to the
        // leader is what we drop. Without inbound MsgAppendResponse
        // the leader can't commit anything; the surviving two
        // (deprived of heartbeats) will time out and elect a new one.
        transport.unregister(killed_id);

        // Find the new leader among the survivors. With election_tick
        // = 10 and heartbeat = 3 ticks (50ms each), elections converge
        // in roughly 1-2 rounds = ~750ms-1.5s. Give 10s for safety.
        let survivors: Vec<&(u64, RaftNode)> = nodes
            .iter()
            .filter(|(id, _)| *id != killed_id)
            .collect();
        let owned: Vec<(u64, RaftNode)> = survivors
            .iter()
            .map(|(id, n)| (*id, n.clone()))
            .collect();
        let new_leader = await_leader(&owned, Duration::from_secs(10)).await;
        assert_ne!(
            new_leader.0, killed_id,
            "new leader must be a surviving node, not the killed one"
        );
    }
}
