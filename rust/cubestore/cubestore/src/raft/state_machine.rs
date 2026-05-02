//! `RaftMetaStore` — the wrapper-style `MetaStore` impl that routes every
//! write through a Raft log before applying to the local state machine.
//!
//! ## Status
//!
//! M2.3 (in progress) — single-node Raft now uses the RocksDB-backed
//! `RaftStorage` from `storage.rs`, replacing the in-memory MemStorage
//! used in M2.1. The Raft log + HardState + ConfState all survive
//! restart. M3 wires every concrete `MetaStore` write method through
//! the apply path.
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
//! M2 uses a test impl of `Apply` (`HashMapApply` in tests) so the
//! round-trip is provable without touching `RocksMetaStore`. M3 will
//! provide a `RocksMetaStoreApply` impl that dispatches each
//! `MetaCommand` variant to the matching `RocksMetaStore::*` write
//! method inside a single RocksDB `WriteBatch`.

use crate::raft::command::{MetaCommand, MetaCommandCodecError, MetaCommandResult};
use crate::raft::storage::{RaftStorage, RaftStorageError, SharedRaftStorage};
use crate::CubeError;
use raft::eraftpb::{ConfState, Entry, EntryType, Message};
use raft::{Config, RawNode};
use slog::{o, Drain};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// A pluggable side-effect that the Raft apply loop runs for every
/// committed `MetaCommand`. M2 used a test implementation; M3.3 will
/// provide one backed by `RocksMetaStore`.
///
/// Implementations must be **deterministic** — every replica that
/// applies the same sequence of `MetaCommand`s must reach the same
/// observable state, byte-for-byte. See `docs/ha/PLAN.md` risk #1.
///
/// The return value mirrors the shape of the underlying `MetaStore`
/// write method via `MetaCommandResult`:
/// - `()` returns                 → `MetaCommandResult::Unit`
/// - `bool` returns               → `MetaCommandResult::Bool`
/// - `IdRow<T>` returns           → `MetaCommandResult::IdRow`
/// - `Option<IdRow<T>>` returns   → `MetaCommandResult::OptionalIdRow`
///
/// The wrapper-style `RaftMetaStore: MetaStore` impl reads the
/// matching variant after `propose(...).await?` and decodes the
/// payload back into the trait's typed return.
pub trait Apply: Send + Sync + 'static {
    fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError>;
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
pub struct RaftMetaStore {
    proposals: mpsc::UnboundedSender<Proposal>,
}

impl RaftMetaStore {
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

        tokio::spawn(run_node(raw, storage, rx, apply, pending, logger));

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

async fn run_node<A: Apply>(
    mut raw: RawNode<SharedRaftStorage>,
    storage: SharedRaftStorage,
    mut proposals: mpsc::UnboundedReceiver<Proposal>,
    apply: Arc<A>,
    mut pending: std::collections::HashMap<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>,
    logger: slog::Logger,
) {
    let _ = logger; // reserved for future structured-log calls
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Single-node bootstrap: trigger an immediate election so proposals
    // don't race against the default `election_tick` countdown. Without
    // this, the first ~500ms of node life is "follower waiting for leader"
    // and any proposal in that window returns ProposalDropped. Multi-node
    // mode (M4) keeps timer-driven elections — explicit campaign on every
    // node would interfere with peer-driven elections.
    if let Err(e) = raw.campaign() {
        log::warn!(
            "raft: initial campaign() failed (will fall back to timer): {:?}",
            e
        );
    }
    drive_ready(&mut raw, &storage, &apply, &mut pending);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                raw.tick();
                drive_ready(&mut raw, &storage, &apply, &mut pending);
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
                        let next_index = raw.raft.raft_log.last_index() + 1;
                        if let Err(e) = raw.propose(vec![], bytes) {
                            let _ = p.respond_to.send(Err(CubeError::internal(format!(
                                "raft propose failed: {:?}", e
                            ))));
                            continue;
                        }
                        pending.insert(next_index, p.respond_to);
                        drive_ready(&mut raw, &storage, &apply, &mut pending);
                    }
                    None => break,
                }
            }
        }
    }
}

fn drive_ready<A: Apply>(
    raw: &mut RawNode<SharedRaftStorage>,
    storage: &SharedRaftStorage,
    apply: &Arc<A>,
    pending: &mut std::collections::HashMap<u64, oneshot::Sender<Result<MetaCommandResult, CubeError>>>,
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

    // 3. Send outbound messages. Single-node has no peers — no-op.
    //    M4 will route these through the cuberpc transport.
    let _outbound: Vec<Message> = ready.take_messages();

    // 4. Apply committed entries.
    let highest_applied = apply_committed(ready.committed_entries(), apply, pending);
    if let Some(idx) = highest_applied {
        let _ = storage.set_applied_index(idx); // best-effort; M5 uses for snapshots
    }

    // 5. Tell raft we're done with this Ready.
    let mut light_ready = raw.advance(ready);

    let highest_applied2 = apply_committed(light_ready.committed_entries(), apply, pending);
    if let Some(idx) = highest_applied2 {
        let _ = storage.set_applied_index(idx);
    }
    raw.advance_apply();
    let _ = light_ready.take_messages();
}

/// Apply each committed entry, return the highest index actually
/// applied (caller persists this to `applied_index` for restart-time
/// recovery).
fn apply_committed<A: Apply>(
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
                let result = MetaCommand::decode(&ent.data)
                    .map_err(|e| {
                        CubeError::internal(format!("decode at index {}: {}", ent.index, e))
                    })
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

    impl Apply for RecordingApply {
        fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
            self.seen.lock().unwrap().push(cmd);
            Ok(MetaCommandResult::Unit)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_node_propose_apply_round_trip() {
        let dir = TempDir::new().unwrap();
        let apply = Arc::new(RecordingApply::new());
        let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::clone(&apply))
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
    impl Apply for TypedReturnApply {
        fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
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
        let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::new(TypedReturnApply))
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
        let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::new(TypedReturnApply))
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
        let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::clone(&apply))
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
        let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::clone(&apply))
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
                    payload_version: 1,
                    payload: vec![1, 2, 3],
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
    impl Apply for HashMapApply {
        fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
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
            let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::clone(&apply))
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
            let store = RaftMetaStore::start_single_node(dir.path(), 1, Arc::clone(&apply))
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
        // persistent. (We don't reboot the full RaftMetaStore here
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
}
