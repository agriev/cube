//! `RaftMetaStore` — the production HA wrapper that implements the
//! `MetaStore` trait by routing every write through Raft.
//!
//! ## Architecture (M3.5)
//!
//! Each `RaftMetaStore` holds:
//! - a [`RaftNode`] for proposing commands;
//! - an `Arc<RocksMetaStore>` shared with the apply impl, used to
//!   serve **read** methods directly without going through Raft.
//!
//! Read methods (the trait's `get_*`, `all_*`, `not_ready_*`,
//! `*_table()`, etc.) delegate straight to the local
//! `RocksMetaStore`. They are **leader-only** in v1: writes are acked
//! durable on the leader's local store before the propose returns,
//! so reads are linearizable with the writer's view. Followers are
//! receive-only in v1; M4 wires read-from-follower with a small
//! consistency window.
//!
//! Write methods encode their arguments into a `MetaCommand`, call
//! `RaftNode::propose(cmd).await`, and decode the typed return back
//! out of `MetaCommandResult` via the M3.2 helpers. Argument shapes
//! that include metastore types (Column, Partition, Job, …) get
//! flex-encoded into blob fields on the `MetaCommand` variant; the
//! dispatch in `RocksMetaStoreApply` decodes them back.
//!
//! ## Coverage (post-M3.7)
//!
//! Full `impl MetaStore for RaftMetaStore` — all 121 trait methods.
//! Reads delegate to the local store; **every write that mutates
//! persistent metastore state routes through Raft.** The two
//! `prepare_multi_partition_for_split` and `prepare_multi_split_finish`
//! methods stay on local delegation despite their names — both
//! use `read_operation` internally and are read-only.
//!
//! ## Determinism (M3.4 — closed)
//!
//! Every persistent write that carries a `DateTime<Utc>` field is
//! leader-stamped:
//!
//! - **M3.4.a** — `Chunk::new` and `Job::new` resolve `Utc::now()` once
//!   on the caller, the row is serialized into a `*_blob` field on
//!   the `MetaCommand`, and replicas insert the byte-identical bytes
//!   on apply.
//! - **M3.4.b** — `CreateTable` and `CreateReplayHandle*` ship the
//!   leader's `assigned_now_millis` alongside the row args, and the
//!   apply path calls `*_with_now` constructors (`Table::new_pure`,
//!   `ReplayHandle::new_pure`) on each replica with that shared `now`.
//! - **M3.4.c/d** — `DeactivateChunk*`, `UpdateHeartBeat`,
//!   `UpdateStatus`, `StartProcessingJob` follow the same
//!   `assigned_now_millis` → `*_with_now` pattern.
//!
//! Read-only filters in `metastore/mod.rs` (`not_ready_tables`,
//! `get_orphaned_jobs`, `get_chunks_without_partition_created_seconds_ago`,
//! …) intentionally use `Utc::now()` — they don't write back, so per-
//! replica clock drift is harmless.
//!
//! Per-call-site classification: `docs/ha/M3.4-AUDIT.md`.
//! Pure-replay regression test:
//! `raft/rocks_apply.rs::tests::pure_replay_is_byte_deterministic_across_replicas`.

use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::multi_index::{MultiIndex, MultiPartition};
use crate::metastore::replay_handle::{ReplayHandle, SeqPointer};
// `RocksPropertyRow` and `RowKey` come from `metastore::rocks_store`
// (private mod) but are re-exported via `pub use rocks_store::*` at the
// metastore root, so the canonical user-facing path is bare `metastore`.
use crate::metastore::snapshot_info::SnapshotInfo;
use crate::metastore::source::{Source, SourceCredentials};
use crate::metastore::table::{StreamOffset, Table, TablePath};
use crate::metastore::{
    Chunk, ChunkMetaStoreTable, Column, IdRow, ImportFormat, Index, IndexDef, IndexMetaStoreTable,
    MetaStore, Partition, PartitionData, PartitionMetaStoreTable, RocksMetaStore,
    RocksPropertyRow, RowKey, Schema, SchemaMetaStoreTable, TableMetaStoreTable, WAL,
};
use crate::raft::command::{IdRowKind, MetaCommand, MetaCommandResultMismatch};
use crate::raft::rocks_apply::RocksMetaStoreApply;
use crate::raft::state_machine::{RaftError, RaftNode};
use crate::table::Row;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use flexbuffers::FlexbufferSerializer;
use serde::Serialize as SerdeSerialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// HA-mode `MetaStore` impl. See module docs.
pub struct RaftMetaStore {
    raft: RaftNode,
    pub(crate) store: Arc<RocksMetaStore>,
}

impl RaftMetaStore {
    /// Boot a single-node `RaftMetaStore` whose log lives at
    /// `raft_log_dir`. The caller has already constructed the local
    /// `RocksMetaStore`; we build a `RocksMetaStoreApply` from it and
    /// hand that to the Raft task.
    pub fn start_single_node(
        raft_log_dir: impl AsRef<Path>,
        node_id: u64,
        store: Arc<RocksMetaStore>,
    ) -> Result<Arc<Self>, RaftError> {
        // Concrete-type Arc — `RaftNode::start_single_node` is generic
        // over `A: Apply` (implicitly `Sized`), so an `Arc<dyn Apply>`
        // would fail the size check.
        let apply = Arc::new(RocksMetaStoreApply::new(store.clone()));
        let raft = RaftNode::start_single_node(raft_log_dir, node_id, apply)?;
        Ok(Arc::new(Self { raft, store }))
    }

    /// Boot a multi-node `RaftMetaStore`. Caller is responsible for:
    /// 1. Constructing a `Transport` and binding the matching listener
    ///    (typically `TcpTransport` + `spawn_listener` for production).
    /// 2. Wiring each peer's `Inbound` so the listener can deliver
    ///    received messages back into this node's Raft loop.
    /// 3. Building the `mpsc::UnboundedReceiver<Message>` paired with
    ///    that `Inbound` and passing it here.
    ///
    /// The `voters` list seeds ConfState on first boot; on restart
    /// the existing log/conf-state on disk takes over. Mutations
    /// after boot go through raft ConfChange entries.
    pub fn start_multi_node<T>(
        raft_log_dir: impl AsRef<Path>,
        node_id: u64,
        voters: Vec<u64>,
        store: Arc<RocksMetaStore>,
        transport: Arc<T>,
        inbound_rx: tokio::sync::mpsc::UnboundedReceiver<raft::eraftpb::Message>,
    ) -> Result<Arc<Self>, RaftError>
    where
        T: crate::raft::transport::Transport,
    {
        let apply = Arc::new(RocksMetaStoreApply::new(store.clone()));
        let raft = RaftNode::start_multi_node(
            raft_log_dir,
            node_id,
            voters,
            apply,
            transport,
            inbound_rx,
        )?;
        Ok(Arc::new(Self { raft, store }))
    }

    /// The local `RocksMetaStore`. Tests and `M3.5.c` config wiring
    /// use this. Read methods on the trait delegate here; write
    /// methods go through Raft.
    pub fn local_store(&self) -> &Arc<RocksMetaStore> {
        &self.store
    }

    /// M6.1 — current leader id as observed by THIS replica's raft
    /// state machine. `None` during election; `Some(id)` once a
    /// leader is known. Reads are lock-free.
    pub fn ha_leader_id(&self) -> Option<u64> {
        self.raft.current_leader_id()
    }

    /// M6.1 — true if THIS replica is the raft leader. Hot-path
    /// answer for readinessProbe / leader-only routing.
    pub fn ha_is_leader_self(&self) -> bool {
        self.raft.is_leader_self()
    }

    /// M6.1 — this replica's stable raft id (matches
    /// `CUBESTORE_NODE_ID` config).
    pub fn ha_node_id(&self) -> u64 {
        self.raft.self_id()
    }

    /// M6.2 — extract a `raft-leader-id=N` hint from a propose
    /// error message. Returns `None` if the error doesn't contain
    /// the marker (e.g. "no leader currently elected" during an
    /// election). The error format is stable: `raft-leader-id=N`
    /// where N is a u64 in decimal.
    ///
    /// A retry-aware client uses this to redirect the failed
    /// write to the leader. Without the marker the client should
    /// back off (the cluster is mid-election).
    pub fn parse_leader_hint(err: &CubeError) -> Option<u64> {
        let msg = &err.message;
        let needle = "raft-leader-id=";
        let idx = msg.find(needle)?;
        let tail = &msg[idx + needle.len()..];
        let end = tail
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(tail.len());
        if end == 0 {
            return None;
        }
        tail[..end].parse::<u64>().ok()
    }

    /// M5.4 + M5.5 + M5.x — explicit snapshot trigger with log
    /// compaction and best-effort remote upload.
    ///
    /// 1. Reads the current `applied_index` from the raft storage.
    /// 2. Reads the current ConfState (and looks up the term for
    ///    that index from the log).
    /// 3. Calls `build_state_machine_snapshot_bytes` against the
    ///    live `RocksMetaStore` to produce the data payload.
    /// 4. Persists the full Snapshot via `storage.save_snapshot`.
    /// 5. Compacts the log up to `snapshot.index - keep_past` so
    ///    late-arriving followers within `keep_past` entries can
    ///    still catch up via MsgAppend instead of MsgSnapshot.
    ///
    /// `staging_root` is where the temporary RocksDB checkpoint dir
    /// lives during the build (typically the same dir as the raft
    /// log, so it stays on the same FS).
    ///
    /// `keep_past` — number of entries to retain past the snapshot
    /// before compaction. 0 = compact everything up to snapshot.index;
    /// 100 = retain the last 100 entries even though they're covered
    /// by the snapshot. The default in operator-facing trigger paths
    /// (M5.4 follow-up janitor) will be ~1000 to absorb typical
    /// follower lag without forcing snapshot ship.
    ///
    /// Caller is expected to invoke this from a single janitor task —
    /// concurrent invocations would race on save_snapshot's
    /// metadata pointer (the data file rename is atomic but two
    /// concurrent rebuilds doing different checkpoints is wasted
    /// work). The future timer-driven janitor will own that.
    pub async fn trigger_snapshot(
        &self,
        staging_root: &std::path::Path,
        keep_past: u64,
    ) -> Result<(), CubeError> {
        use crate::raft::snapshot_builder;
        use raft::Storage;

        let storage = self.raft.storage();

        // Snapshot the current view of the state machine. The
        // applied_index here may advance between this read and the
        // checkpoint call below — that's fine, the checkpoint is a
        // RocksDB-level snapshot of whatever rows have committed by
        // the time it runs, and we record THAT index as the metadata.
        let applied = storage
            .applied_index()
            .map_err(|e| CubeError::internal(format!("read applied_index: {}", e)))?
            .unwrap_or(0);

        // Storage::term may return Compacted / Unavailable errors
        // for indices outside the current log range. For applied=0
        // (no entries yet) we use term=0; that's a valid snapshot
        // metadata for an "empty cluster" state.
        let term = Storage::term(&storage, applied).unwrap_or(0);

        // ConfState comes from the storage's cached value. raft-rs
        // updates it via `set_conf_state` whenever a ConfChange
        // entry applies.
        let conf_state = storage.read_conf_state();

        // Build the state-machine bytes via M5.3.
        let data =
            snapshot_builder::build_state_machine_snapshot_bytes(&self.store, staging_root)
                .await?;

        // Persist via M5.1.
        let mut snapshot = raft::eraftpb::Snapshot::default();
        let meta = snapshot.mut_metadata();
        meta.index = applied;
        meta.term = term;
        meta.set_conf_state(conf_state);
        snapshot.set_data(data.into());

        storage
            .save_snapshot(&snapshot)
            .map_err(|e| CubeError::internal(format!("save_snapshot: {}", e)))?;

        // M5.x — best-effort remote upload of `snapshot.bin` to S3
        // (or whatever backs the metastore_fs). The local persistence
        // is the source of truth — if remote upload fails we log and
        // proceed; raft-rs's peer-to-peer MsgSnapshot path still works
        // for late-joining followers. The remote copy is only
        // load-bearing for "all replicas offline at once" disaster
        // recovery, where the alternative (`metastore-current`
        // checkpoint) lags by the inner RocksMetaStore's upload
        // cadence.
        let snapshot_bin_path = storage.snapshot_data_path();
        if snapshot_bin_path.exists() {
            let metastore_fs = self.store.metastore_fs();
            if let Err(e) = metastore_fs
                .upload_raft_snapshot_file(
                    snapshot_bin_path,
                    // Single fixed remote name — we only retain the
                    // latest snapshot, mirroring local retention.
                    // A future enhancement is index-stamped names
                    // for point-in-time restore.
                    "latest.bin".to_string(),
                )
                .await
            {
                log::warn!(
                    "raft snapshot remote upload failed (local save still valid): {}",
                    e
                );
            }
        } else {
            log::debug!(
                "raft snapshot remote upload skipped: \
                 snapshot.bin not present at {:?} (was save_snapshot a no-op?)",
                snapshot_bin_path
            );
        }

        // M5.5 — log compaction.
        //
        // Drop entries up to `applied - keep_past`, but never below
        // 1 (raft-rs requires first_index() >= 1) and never past
        // `applied` itself (we still need the entry that backs
        // applied_index for `term(applied)` lookups in future
        // snapshots).
        //
        // The log layer's `compact(N)` removes entries with index < N.
        // So passing `applied - keep_past` retains everything from
        // (applied - keep_past) onward.
        if applied > keep_past {
            let compact_to = applied.saturating_sub(keep_past);
            if compact_to > 1 {
                storage
                    .compact(compact_to)
                    .map_err(|e| CubeError::internal(format!("compact: {}", e)))?;
            }
        }
        Ok(())
    }

    fn mismatch(method: &'static str, e: MetaCommandResultMismatch) -> CubeError {
        CubeError::internal(format!(
            "RaftMetaStore::{} — apply produced a wrong result shape: {}",
            method, e
        ))
    }

    fn encode_blob<T: SerdeSerialize>(
        method: &'static str,
        field: &'static str,
        value: &T,
    ) -> Result<Vec<u8>, CubeError> {
        let mut s = FlexbufferSerializer::new();
        value.serialize(&mut s).map_err(|e| {
            CubeError::internal(format!(
                "RaftMetaStore::{} — encode {}: {}",
                method, field, e
            ))
        })?;
        Ok(s.take_buffer())
    }
}

#[async_trait]
impl MetaStore for RaftMetaStore {
    // -------------------------------------------------------------------
    // Maintenance / read-only
    // -------------------------------------------------------------------
    async fn wait_for_current_seq_to_sync(&self) -> Result<(), CubeError> {
        self.store.wait_for_current_seq_to_sync().await
    }

    fn schemas_table(&self) -> SchemaMetaStoreTable {
        self.store.schemas_table()
    }

    // -------------------------------------------------------------------
    // Schemas
    // -------------------------------------------------------------------
    async fn create_schema(
        &self,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.raft
            .propose(MetaCommand::CreateSchema {
                schema_name,
                if_not_exists,
            })
            .await?
            .into_id_row(IdRowKind::Schema)
            .map_err(|e| Self::mismatch("create_schema", e))
    }
    async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError> {
        self.store.get_schemas().await
    }
    async fn get_schema_by_id(&self, schema_id: u64) -> Result<IdRow<Schema>, CubeError> {
        self.store.get_schema_by_id(schema_id).await
    }
    async fn get_schema_id(&self, schema_name: String) -> Result<u64, CubeError> {
        self.store.get_schema_id(schema_name).await
    }
    async fn get_schema(&self, schema_name: String) -> Result<IdRow<Schema>, CubeError> {
        self.store.get_schema(schema_name).await
    }
    async fn rename_schema(
        &self,
        old_schema_name: String,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.raft
            .propose(MetaCommand::RenameSchema {
                old_schema_name,
                new_schema_name,
            })
            .await?
            .into_id_row(IdRowKind::Schema)
            .map_err(|e| Self::mismatch("rename_schema", e))
    }
    async fn rename_schema_by_id(
        &self,
        schema_id: u64,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.raft
            .propose(MetaCommand::RenameSchemaById {
                schema_id,
                new_schema_name,
            })
            .await?
            .into_id_row(IdRowKind::Schema)
            .map_err(|e| Self::mismatch("rename_schema_by_id", e))
    }
    async fn delete_schema(&self, schema_name: String) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DeleteSchema { schema_name })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("delete_schema", e))
    }
    async fn delete_schema_by_id(&self, schema_id: u64) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DeleteSchemaById { schema_id })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("delete_schema_by_id", e))
    }

    // -------------------------------------------------------------------
    // Tables
    // -------------------------------------------------------------------
    fn tables_table(&self) -> TableMetaStoreTable {
        self.store.tables_table()
    }
    async fn create_table(
        &self,
        schema_name: String,
        table_name: String,
        columns: Vec<Column>,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        indexes: Vec<IndexDef>,
        is_ready: bool,
        build_range_end: Option<DateTime<Utc>>,
        seal_at: Option<DateTime<Utc>>,
        select_statement: Option<String>,
        source_coulumns: Option<Vec<Column>>,
        stream_offset: Option<StreamOffset>,
        unique_key_column_names: Option<Vec<String>>,
        aggregates: Option<Vec<(String, String)>>,
        partition_split_threshold: Option<u64>,
        trace_obj: Option<String>,
        drop_if_exists: bool,
        extension: Option<String>,
    ) -> Result<IdRow<Table>, CubeError> {
        let columns_blob = Self::encode_blob("create_table", "columns", &columns)?;
        let import_format_blob = match &import_format {
            Some(v) => Some(Self::encode_blob("create_table", "import_format", v)?),
            None => None,
        };
        let indexes_blob = Self::encode_blob("create_table", "indexes", &indexes)?;
        let source_columns_blob = match &source_coulumns {
            Some(v) => Some(Self::encode_blob("create_table", "source_columns", v)?),
            None => None,
        };
        let stream_offset_blob = match &stream_offset {
            Some(v) => Some(Self::encode_blob("create_table", "stream_offset", v)?),
            None => None,
        };
        // M3.4.b.2: stamp `now` on the leader so every replica's
        // `Table::created_at` is identical.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::CreateTable {
                schema_name,
                table_name,
                columns_blob,
                locations,
                import_format_blob,
                indexes_blob,
                is_ready,
                build_range_end_millis: build_range_end.map(|d| d.timestamp_millis()),
                seal_at_millis: seal_at.map(|d| d.timestamp_millis()),
                select_statement,
                source_columns_blob,
                stream_offset_blob,
                unique_key_column_names,
                aggregates,
                partition_split_threshold,
                trace_obj,
                drop_if_exists,
                extension,
                assigned_now_millis,
            })
            .await?
            .into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("create_table", e))
    }
    async fn table_ready(&self, id: u64, is_ready: bool) -> Result<IdRow<Table>, CubeError> {
        self.raft
            .propose(MetaCommand::TableReady {
                table_id: id,
                is_ready,
            })
            .await?
            .into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("table_ready", e))
    }
    async fn seal_table(&self, id: u64) -> Result<IdRow<Table>, CubeError> {
        self.raft
            .propose(MetaCommand::SealTable { table_id: id })
            .await?
            .into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("seal_table", e))
    }
    async fn get_trace_obj_by_table_id(&self, table_id: u64) -> Result<Option<String>, CubeError> {
        self.store.get_trace_obj_by_table_id(table_id).await
    }
    async fn update_location_download_size(
        &self,
        id: u64,
        location: String,
        download_size: u64,
    ) -> Result<IdRow<Table>, CubeError> {
        self.raft
            .propose(MetaCommand::UpdateLocationDownloadSize {
                table_id: id,
                location,
                download_size,
            })
            .await?
            .into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("update_location_download_size", e))
    }
    async fn get_table(
        &self,
        schema_name: String,
        table_name: String,
    ) -> Result<IdRow<Table>, CubeError> {
        self.store.get_table(schema_name, table_name).await
    }
    async fn get_table_by_id(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.store.get_table_by_id(table_id).await
    }
    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError> {
        self.store.get_tables().await
    }
    async fn get_tables_with_path(
        &self,
        include_non_ready: bool,
    ) -> Result<Arc<Vec<TablePath>>, CubeError> {
        self.store.get_tables_with_path(include_non_ready).await
    }
    async fn not_ready_tables(
        &self,
        created_seconds_ago: i64,
    ) -> Result<Vec<IdRow<Table>>, CubeError> {
        self.store.not_ready_tables(created_seconds_ago).await
    }
    async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        self.raft
            .propose(MetaCommand::DropTable { table_id })
            .await?
            .into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("drop_table", e))
    }

    // -------------------------------------------------------------------
    // Partitions
    // -------------------------------------------------------------------
    fn partition_table(&self) -> PartitionMetaStoreTable {
        self.store.partition_table()
    }
    async fn create_partition(&self, partition: Partition) -> Result<IdRow<Partition>, CubeError> {
        let partition_blob = Self::encode_blob("create_partition", "partition", &partition)?;
        self.raft
            .propose(MetaCommand::CreatePartition { partition_blob })
            .await?
            .into_id_row(IdRowKind::Partition)
            .map_err(|e| Self::mismatch("create_partition", e))
    }
    async fn get_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.store.get_partition(partition_id).await
    }
    async fn get_partition_out_of_queue(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        self.store.get_partition_out_of_queue(partition_id).await
    }
    async fn get_partition_for_compaction(
        &self,
        partition_id: u64,
    ) -> Result<
        (
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Option<IdRow<MultiPartition>>,
        ),
        CubeError,
    > {
        self.store.get_partition_for_compaction(partition_id).await
    }
    async fn get_partition_chunk_sizes(&self, partition_id: u64) -> Result<u64, CubeError> {
        self.store.get_partition_chunk_sizes(partition_id).await
    }
    async fn swap_compacted_chunks(
        &self,
        partition_id: u64,
        old_chunk_ids: Vec<u64>,
        new_chunk: u64,
        new_chunk_file_size: u64,
    ) -> Result<bool, CubeError> {
        self.raft
            .propose(MetaCommand::SwapCompactedChunks {
                partition_id,
                old_chunk_ids,
                new_chunk,
                new_chunk_file_size,
            })
            .await?
            .into_bool()
            .map_err(|e| Self::mismatch("swap_compacted_chunks", e))
    }
    async fn swap_active_partitions(
        &self,
        current_active: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_active: Vec<(IdRow<Partition>, u64)>,
        new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>), (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError> {
        let current_active_blob =
            Self::encode_blob("swap_active_partitions", "current_active", &current_active)?;
        let new_active_blob =
            Self::encode_blob("swap_active_partitions", "new_active", &new_active)?;
        let new_active_min_max_blob = Self::encode_blob(
            "swap_active_partitions",
            "new_active_min_max",
            &new_active_min_max,
        )?;
        self.raft
            .propose(MetaCommand::SwapActivePartitions {
                current_active_blob,
                new_active_blob,
                new_active_min_max_blob,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("swap_active_partitions", e))
    }
    async fn delete_partition(&self, partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        self.raft
            .propose(MetaCommand::DeletePartition { partition_id })
            .await?
            .into_id_row(IdRowKind::Partition)
            .map_err(|e| Self::mismatch("delete_partition", e))
    }
    async fn mark_partition_warmed_up(&self, partition_id: u64) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::MarkPartitionWarmedUp { partition_id })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("mark_partition_warmed_up", e))
    }
    async fn delete_middle_man_partition(
        &self,
        partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        self.raft
            .propose(MetaCommand::DeleteMiddleManPartition { partition_id })
            .await?
            .into_id_row(IdRowKind::Partition)
            .map_err(|e| Self::mismatch("delete_middle_man_partition", e))
    }
    async fn can_delete_partition(&self, partition_id: u64) -> Result<bool, CubeError> {
        self.store.can_delete_partition(partition_id).await
    }
    async fn can_delete_middle_man_partition(&self, partition_id: u64) -> Result<bool, CubeError> {
        self.store.can_delete_middle_man_partition(partition_id).await
    }
    async fn all_inactive_partitions_to_repartition(
        &self,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store.all_inactive_partitions_to_repartition().await
    }
    async fn all_inactive_middle_man_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store.all_inactive_middle_man_partitions().await
    }
    async fn all_just_created_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store.all_just_created_partitions().await
    }
    async fn get_partitions_with_chunks_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store
            .get_partitions_with_chunks_created_seconds_ago(seconds_ago)
            .await
    }
    async fn get_partitions_for_in_memory_compaction(
        &self,
        node: String,
    ) -> Result<
        Vec<(
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Vec<IdRow<Chunk>>,
        )>,
        CubeError,
    > {
        self.store.get_partitions_for_in_memory_compaction(node).await
    }
    async fn get_all_node_in_memory_chunks(
        &self,
        node: String,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store.get_all_node_in_memory_chunks(node).await
    }
    async fn get_chunks_without_partition_created_seconds_ago(
        &self,
        seconds_ago: i64,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store
            .get_chunks_without_partition_created_seconds_ago(seconds_ago)
            .await
    }

    // -------------------------------------------------------------------
    // Indexes (regular + partitioned + multi)
    // -------------------------------------------------------------------
    fn index_table(&self) -> IndexMetaStoreTable {
        self.store.index_table()
    }
    async fn create_index(
        &self,
        schema_name: String,
        table_name: String,
        index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        let index_def_blob = Self::encode_blob("create_index", "index_def", &index_def)?;
        self.raft
            .propose(MetaCommand::CreateIndex {
                schema_name,
                table_name,
                index_def_blob,
            })
            .await?
            .into_id_row(IdRowKind::Index)
            .map_err(|e| Self::mismatch("create_index", e))
    }
    async fn get_default_index(&self, table_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.store.get_default_index(table_id).await
    }
    async fn get_table_indexes(&self, table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError> {
        self.store.get_table_indexes(table_id).await
    }
    async fn get_table_indexes_out_of_queue(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<Index>>, CubeError> {
        self.store.get_table_indexes_out_of_queue(table_id).await
    }
    async fn get_active_partitions_by_index_id(
        &self,
        index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store.get_active_partitions_by_index_id(index_id).await
    }
    async fn get_index(&self, index_id: u64) -> Result<IdRow<Index>, CubeError> {
        self.store.get_index(index_id).await
    }
    async fn get_index_with_active_partitions_out_of_queue(
        &self,
        index_id: u64,
    ) -> Result<(IdRow<Index>, Vec<IdRow<Partition>>), CubeError> {
        self.store
            .get_index_with_active_partitions_out_of_queue(index_id)
            .await
    }
    async fn create_partitioned_index(
        &self,
        schema: String,
        name: String,
        columns: Vec<Column>,
        if_not_exists: bool,
    ) -> Result<IdRow<MultiIndex>, CubeError> {
        let columns_blob = Self::encode_blob("create_partitioned_index", "columns", &columns)?;
        self.raft
            .propose(MetaCommand::CreatePartitionedIndex {
                schema,
                name,
                columns_blob,
                if_not_exists,
            })
            .await?
            .into_id_row(IdRowKind::MultiIndex)
            .map_err(|e| Self::mismatch("create_partitioned_index", e))
    }
    async fn drop_partitioned_index(&self, schema: String, name: String) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DropPartitionedIndex { schema, name })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("drop_partitioned_index", e))
    }
    async fn get_multi_partition(&self, id: u64) -> Result<IdRow<MultiPartition>, CubeError> {
        self.store.get_multi_partition(id).await
    }
    async fn get_child_multi_partitions(
        &self,
        id: u64,
    ) -> Result<Vec<IdRow<MultiPartition>>, CubeError> {
        self.store.get_child_multi_partitions(id).await
    }
    async fn get_multi_partition_subtree(
        &self,
        multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError> {
        self.store.get_multi_partition_subtree(multi_part_ids).await
    }
    async fn create_multi_partition(
        &self,
        p: MultiPartition,
    ) -> Result<IdRow<MultiPartition>, CubeError> {
        let multi_partition_blob =
            Self::encode_blob("create_multi_partition", "multi_partition", &p)?;
        self.raft
            .propose(MetaCommand::CreateMultiPartition {
                multi_partition_blob,
            })
            .await?
            .into_id_row(IdRowKind::MultiPartition)
            .map_err(|e| Self::mismatch("create_multi_partition", e))
    }
    // M3.7 audit: actually a `read_operation` despite the name —
    // returns existing rows without mutating state. Local delegation
    // is correct under HA mode (linearizable with the leader's
    // writes since reads are leader-local in v1).
    async fn prepare_multi_partition_for_split(
        &self,
        multi_partition_id: u64,
    ) -> Result<(IdRow<MultiIndex>, IdRow<MultiPartition>, Vec<PartitionData>), CubeError> {
        self.store
            .prepare_multi_partition_for_split(multi_partition_id)
            .await
    }
    async fn commit_multi_partition_split(
        &self,
        multi_partition_id: u64,
        new_multi_partitions: Vec<u64>,
        new_multi_partition_rows: Vec<u64>,
        old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        new_partitions: Vec<(IdRow<Partition>, u64)>,
        new_partition_rows: Vec<u64>,
        initial_split: bool,
    ) -> Result<(), CubeError> {
        // M3.7: route through Raft. Old/new_partitions are flex-blobs
        // because they nest IdRow<Partition>/IdRow<Chunk> from the
        // metastore module.
        let old_partitions_blob =
            Self::encode_blob("commit_multi_partition_split", "old_partitions", &old_partitions)?;
        let new_partitions_blob =
            Self::encode_blob("commit_multi_partition_split", "new_partitions", &new_partitions)?;
        self.raft
            .propose(MetaCommand::CommitMultiPartitionSplit {
                multi_partition_id,
                new_multi_partitions,
                new_multi_partition_rows,
                old_partitions_blob,
                new_partitions_blob,
                new_partition_rows,
                initial_split,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("commit_multi_partition_split", e))
    }
    async fn find_unsplit_partitions(
        &self,
        multi_partition_id: u64,
    ) -> Result<Vec<u64>, CubeError> {
        self.store.find_unsplit_partitions(multi_partition_id).await
    }
    // M3.7 audit: also `read_operation`; same reasoning as
    // `prepare_multi_partition_for_split` above.
    async fn prepare_multi_split_finish(
        &self,
        multi_partition_id: u64,
        partition_id: u64,
    ) -> Result<(PartitionData, Vec<IdRow<MultiPartition>>), CubeError> {
        self.store
            .prepare_multi_split_finish(multi_partition_id, partition_id)
            .await
    }
    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError> {
        self.store
            .get_active_partitions_and_chunks_by_index_id_for_select(index_id)
            .await
    }
    async fn get_warmup_partitions(
        &self,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError> {
        self.store.get_warmup_partitions().await
    }
    async fn get_all_filenames(&self) -> Result<Vec<String>, CubeError> {
        self.store.get_all_filenames().await
    }

    // -------------------------------------------------------------------
    // Chunks
    // -------------------------------------------------------------------
    fn chunks_table(&self) -> ChunkMetaStoreTable {
        self.store.chunks_table()
    }
    async fn create_chunk(
        &self,
        partition_id: u64,
        row_count: usize,
        min: Option<Row>,
        max: Option<Row>,
        in_memory: bool,
    ) -> Result<IdRow<Chunk>, CubeError> {
        // M3.4.a: build the Chunk on the leader so `created_at`,
        // `oldest_insert_at` and `suffix` are resolved once and
        // shipped through the Raft log. Replicas then insert the
        // pre-built chunk byte-for-byte. See `Chunk::new_pure` and
        // `RocksMetaStore::insert_chunk_pre_built`.
        let chunk = Chunk::new(partition_id, row_count, min, max, in_memory);
        let chunk_blob = Self::encode_blob("create_chunk", "chunk", &chunk)?;
        self.raft
            .propose(MetaCommand::CreateChunk { chunk_blob })
            .await?
            .into_id_row(IdRowKind::Chunk)
            .map_err(|e| Self::mismatch("create_chunk", e))
    }
    async fn insert_chunks(&self, chunks: Vec<Chunk>) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        // M3.7: ship pre-built chunks. Caller (cubestore internals)
        // builds them on the leader; we serialize the whole list.
        let chunks_blob = Self::encode_blob("insert_chunks", "chunks", &chunks)?;
        self.raft
            .propose(MetaCommand::InsertChunks { chunks_blob })
            .await?
            .into_id_row_list(IdRowKind::Chunk)
            .map_err(|e| Self::mismatch("insert_chunks", e))
    }
    async fn get_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.store.get_chunk(chunk_id).await
    }
    async fn get_chunks_out_of_queue(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store.get_chunks_out_of_queue(ids).await
    }
    async fn get_partitions_out_of_queue(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        self.store.get_partitions_out_of_queue(ids).await
    }
    async fn get_chunks_by_partition(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store
            .get_chunks_by_partition(partition_id, include_inactive)
            .await
    }
    async fn get_used_disk_space_out_of_queue(
        &self,
        node: Option<String>,
    ) -> Result<u64, CubeError> {
        self.store.get_used_disk_space_out_of_queue(node).await
    }
    async fn get_all_partitions_and_chunks_out_of_queue(
        &self,
    ) -> Result<(Vec<IdRow<Partition>>, Vec<IdRow<Chunk>>), CubeError> {
        self.store.get_all_partitions_and_chunks_out_of_queue().await
    }
    async fn get_chunks_by_partition_out_of_queue(
        &self,
        partition_id: u64,
        include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store
            .get_chunks_by_partition_out_of_queue(partition_id, include_inactive)
            .await
    }
    async fn chunk_uploaded(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.raft
            .propose(MetaCommand::ChunkUploaded { chunk_id })
            .await?
            .into_id_row(IdRowKind::Chunk)
            .map_err(|e| Self::mismatch("chunk_uploaded", e))
    }
    async fn chunk_update_last_inserted(
        &self,
        chunk_ids: Vec<u64>,
        last_inserted_at: Option<DateTime<Utc>>,
    ) -> Result<(), CubeError> {
        // M3.7: ship the timestamp as ms-since-epoch.
        let last_inserted_at_millis = last_inserted_at.map(|d| d.timestamp_millis());
        self.raft
            .propose(MetaCommand::ChunkUpdateLastInserted {
                chunk_ids,
                last_inserted_at_millis,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("chunk_update_last_inserted", e))
    }
    async fn deactivate_chunk(&self, chunk_id: u64) -> Result<(), CubeError> {
        // M3.4.c: leader-stamped now for `Chunk::deactivated_at`.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::DeactivateChunk {
                chunk_id,
                assigned_now_millis,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("deactivate_chunk", e))
    }
    async fn deactivate_chunks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::DeactivateChunks {
                chunk_ids,
                assigned_now_millis,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("deactivate_chunks", e))
    }
    async fn swap_chunks(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::SwapChunks {
                deactivate_ids,
                uploaded_ids_and_sizes,
                new_replay_handle_id,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("swap_chunks", e))
    }
    async fn swap_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::SwapChunksWithoutCheck {
                deactivate_ids,
                uploaded_ids_and_sizes,
                new_replay_handle_id,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("swap_chunks_without_check", e))
    }
    async fn deactivate_chunks_without_check(
        &self,
        deactivate_ids: Vec<u64>,
    ) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DeactivateChunksWithoutCheck { deactivate_ids })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("deactivate_chunks_without_check", e))
    }
    async fn activate_chunks(
        &self,
        table_id: u64,
        uploaded_chunk_ids: Vec<(u64, Option<u64>)>,
        replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::ActivateChunks {
                table_id,
                uploaded_chunk_ids,
                replay_handle_id,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("activate_chunks", e))
    }
    async fn delete_chunk(&self, chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        self.raft
            .propose(MetaCommand::DeleteChunk { chunk_id })
            .await?
            .into_id_row(IdRowKind::Chunk)
            .map_err(|e| Self::mismatch("delete_chunk", e))
    }
    async fn delete_chunks_without_checks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DeleteChunksWithoutChecks { chunk_ids })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("delete_chunks_without_checks", e))
    }
    async fn all_inactive_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store.all_inactive_chunks().await
    }
    async fn all_inactive_not_uploaded_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store.all_inactive_not_uploaded_chunks().await
    }

    // -------------------------------------------------------------------
    // WAL
    // -------------------------------------------------------------------
    async fn create_wal(
        &self,
        table_id: u64,
        row_count: usize,
    ) -> Result<IdRow<WAL>, CubeError> {
        self.raft
            .propose(MetaCommand::CreateWal {
                table_id,
                row_count: row_count as u64,
            })
            .await?
            .into_id_row(IdRowKind::Wal)
            .map_err(|e| Self::mismatch("create_wal", e))
    }
    async fn get_wal(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        self.store.get_wal(wal_id).await
    }
    async fn delete_wal(&self, wal_id: u64) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::DeleteWal { wal_id })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("delete_wal", e))
    }
    async fn wal_uploaded(&self, wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        self.raft
            .propose(MetaCommand::WalUploaded { wal_id })
            .await?
            .into_id_row(IdRowKind::Wal)
            .map_err(|e| Self::mismatch("wal_uploaded", e))
    }
    async fn get_wals_for_table(&self, table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError> {
        self.store.get_wals_for_table(table_id).await
    }

    // -------------------------------------------------------------------
    // Jobs
    // -------------------------------------------------------------------
    async fn all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.store.all_jobs().await
    }
    async fn add_job(&self, job: Job) -> Result<Option<IdRow<Job>>, CubeError> {
        let job_blob = Self::encode_blob("add_job", "job", &job)?;
        self.raft
            .propose(MetaCommand::AddJob { job_blob })
            .await?
            .into_optional_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("add_job", e))
    }
    async fn get_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.store.get_job(job_id).await
    }
    async fn get_job_by_ref(
        &self,
        row_reference: RowKey,
        job_type: JobType,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        self.store.get_job_by_ref(row_reference, job_type).await
    }
    async fn get_orphaned_jobs(
        &self,
        orphaned_timeout: Duration,
    ) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.store.get_orphaned_jobs(orphaned_timeout).await
    }
    async fn get_jobs_on_non_exists_nodes(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.store.get_jobs_on_non_exists_nodes().await
    }
    async fn delete_job(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.raft
            .propose(MetaCommand::DeleteJob { job_id })
            .await?
            .into_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("delete_job", e))
    }
    async fn start_processing_job(
        &self,
        server_name: String,
        long_term: bool,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        // M3.4.d: leader-stamped now for the picked job's
        // `last_heart_beat`.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::StartProcessingJob {
                server_name,
                long_term,
                assigned_now_millis,
            })
            .await?
            .into_optional_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("start_processing_job", e))
    }
    async fn update_status(
        &self,
        job_id: u64,
        status: JobStatus,
    ) -> Result<IdRow<Job>, CubeError> {
        let status_blob = Self::encode_blob("update_status", "status", &status)?;
        // M3.4.c: leader-stamped now for `Job::last_heart_beat`.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::UpdateStatus {
                job_id,
                status_blob,
                assigned_now_millis,
            })
            .await?
            .into_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("update_status", e))
    }
    async fn update_heart_beat(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        // M3.4.c: leader-stamped now.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::UpdateHeartBeat {
                job_id,
                assigned_now_millis,
            })
            .await?
            .into_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("update_heart_beat", e))
    }
    async fn delete_all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        // M3.7: route through Raft, return all deleted jobs as
        // an IdRowList.
        self.raft
            .propose(MetaCommand::DeleteAllJobs)
            .await?
            .into_id_row_list(IdRowKind::Job)
            .map_err(|e| Self::mismatch("delete_all_jobs", e))
    }

    // -------------------------------------------------------------------
    // Sources
    // -------------------------------------------------------------------
    async fn create_or_update_source(
        &self,
        name: String,
        credentials: SourceCredentials,
    ) -> Result<IdRow<Source>, CubeError> {
        let credentials_blob =
            Self::encode_blob("create_or_update_source", "credentials", &credentials)?;
        self.raft
            .propose(MetaCommand::CreateOrUpdateSource {
                name,
                credentials_blob,
            })
            .await?
            .into_id_row(IdRowKind::Source)
            .map_err(|e| Self::mismatch("create_or_update_source", e))
    }
    async fn get_source(&self, id: u64) -> Result<IdRow<Source>, CubeError> {
        self.store.get_source(id).await
    }
    async fn get_source_by_name(&self, name: String) -> Result<IdRow<Source>, CubeError> {
        self.store.get_source_by_name(name).await
    }
    async fn delete_source(&self, id: u64) -> Result<IdRow<Source>, CubeError> {
        self.raft
            .propose(MetaCommand::DeleteSource { id })
            .await?
            .into_id_row(IdRowKind::Source)
            .map_err(|e| Self::mismatch("delete_source", e))
    }

    // -------------------------------------------------------------------
    // Replay handles
    // -------------------------------------------------------------------
    async fn create_replay_handle(
        &self,
        table_id: u64,
        location_index: usize,
        seq_pointer: SeqPointer,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        let seq_pointer_blob =
            Self::encode_blob("create_replay_handle", "seq_pointer", &seq_pointer)?;
        // M3.4.b.1: stamp `now` on the leader and ship it to followers
        // so each replica's ReplayHandle::created_at is identical.
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::CreateReplayHandle {
                table_id,
                location_index: location_index as u64,
                seq_pointer_blob,
                assigned_now_millis,
            })
            .await?
            .into_id_row(IdRowKind::ReplayHandle)
            .map_err(|e| Self::mismatch("create_replay_handle", e))
    }
    async fn create_replay_handle_from_seq_pointers(
        &self,
        table_id: u64,
        seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        let seq_pointers_blob = Self::encode_blob(
            "create_replay_handle_from_seq_pointers",
            "seq_pointers",
            &seq_pointer,
        )?;
        let assigned_now_millis = Utc::now().timestamp_millis();
        self.raft
            .propose(MetaCommand::CreateReplayHandleFromSeqPointers {
                table_id,
                seq_pointers_blob,
                assigned_now_millis,
            })
            .await?
            .into_id_row(IdRowKind::ReplayHandle)
            .map_err(|e| Self::mismatch("create_replay_handle_from_seq_pointers", e))
    }
    async fn get_replay_handles_by_table(
        &self,
        table_id: u64,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.store.get_replay_handles_by_table(table_id).await
    }
    async fn get_replay_handles_by_ids(
        &self,
        ids: Vec<u64>,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.store.get_replay_handles_by_ids(ids).await
    }
    async fn all_replay_handles(&self) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        self.store.all_replay_handles().await
    }
    async fn all_replay_handles_to_merge(
        &self,
    ) -> Result<Vec<(IdRow<ReplayHandle>, bool)>, CubeError> {
        self.store.all_replay_handles_to_merge().await
    }
    async fn update_replay_handle_failed_if_exists(
        &self,
        id: u64,
        failed: bool,
    ) -> Result<(), CubeError> {
        self.raft
            .propose(MetaCommand::UpdateReplayHandleFailedIfExists { id, failed })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("update_replay_handle_failed_if_exists", e))
    }
    async fn replace_replay_handles(
        &self,
        old_ids: Vec<u64>,
        new_seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<Option<IdRow<ReplayHandle>>, CubeError> {
        let new_seq_pointer_blob =
            Self::encode_blob("replace_replay_handles", "new_seq_pointer", &new_seq_pointer)?;
        self.raft
            .propose(MetaCommand::ReplaceReplayHandles {
                old_ids,
                new_seq_pointer_blob,
            })
            .await?
            .into_optional_id_row(IdRowKind::ReplayHandle)
            .map_err(|e| Self::mismatch("replace_replay_handles", e))
    }

    // -------------------------------------------------------------------
    // Misc
    // -------------------------------------------------------------------
    async fn get_tables_with_indexes(
        &self,
        table_name: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError> {
        self.store.get_tables_with_indexes(table_name).await
    }
    async fn debug_dump(&self, out_path: String) -> Result<(), CubeError> {
        self.store.debug_dump(out_path).await
    }
    async fn compaction(&self) -> Result<(), CubeError> {
        self.store.compaction().await
    }
    async fn healthcheck(&self) -> Result<(), CubeError> {
        self.store.healthcheck().await
    }
    async fn rocksdb_properties(&self) -> Result<Vec<RocksPropertyRow>, CubeError> {
        self.store.rocksdb_properties().await
    }
    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError> {
        self.store.get_snapshots_list().await
    }
    async fn set_current_snapshot(&self, snapshot_id: u128) -> Result<(), CubeError> {
        let snapshot_id_low = snapshot_id as u64;
        let snapshot_id_high = (snapshot_id >> 64) as u64;
        self.raft
            .propose(MetaCommand::SetCurrentSnapshot {
                snapshot_id_low,
                snapshot_id_high,
            })
            .await?
            .into_unit()
            .map_err(|e| Self::mismatch("set_current_snapshot", e))
    }
}

// `MetaStore: DIService` — same registration the inner RocksMetaStore
// uses, so `RaftMetaStore` can be DI-bound where `Arc<dyn MetaStore>`
// is expected (M3.5.c boot path swap).
crate::di_service!(RaftMetaStore, [MetaStore]);

// =============================================================================
// Tests — propose-and-read end-to-end through the production wrapper.
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::{BaseRocksStoreFs, MetaStore, RocksMetaStore};
    use crate::remotefs::LocalDirRemoteFs;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    /// Set up a temp `RocksMetaStore` and a `RaftMetaStore` wrapping
    /// it, with the Raft log in a separate temp dir.
    fn setup_wrapper(
        test_name: &str,
    ) -> (
        Arc<RaftMetaStore>,
        std::path::PathBuf,
        std::path::PathBuf,
        TempDir,
    ) {
        let config = Config::test(test_name);
        let cwd = env::current_dir().unwrap();
        let store_path = cwd.join(format!("{}-local", test_name));
        let remote_path = cwd.join(format!("{}-remote", test_name));
        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_path);

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

    fn cleanup(store_path: &std::path::Path, remote_path: &std::path::Path) {
        let _ = fs::remove_dir_all(store_path);
        let _ = fs::remove_dir_all(remote_path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn schema_lifecycle_via_metastore_trait() {
        let (wrapper, sp, rp, _raft_dir) = setup_wrapper("raft_meta_store_trait_lifecycle");

        // Settle initial campaign (single-node).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Calls go through the `MetaStore` trait — not the inherent
        // methods on `RaftMetaStore`. Proves M3.5.b's `impl MetaStore`
        // routes correctly through Raft for writes and locally for
        // reads.
        let created: IdRow<Schema> = MetaStore::create_schema(&*wrapper, "public".into(), false)
            .await
            .expect("create_schema");
        assert_eq!(created.get_row().get_name(), "public");

        let listed = MetaStore::get_schemas(&*wrapper).await.expect("get_schemas");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].get_id(), created.get_id());

        let renamed =
            MetaStore::rename_schema(&*wrapper, "public".into(), "renamed".into())
                .await
                .expect("rename_schema");
        assert_eq!(renamed.get_row().get_name(), "renamed");

        MetaStore::delete_schema(&*wrapper, "renamed".into())
            .await
            .expect("delete_schema");

        let after = MetaStore::get_schemas(&*wrapper).await.expect("get_schemas");
        assert!(after.is_empty(), "schema must be gone after delete");

        cleanup(&sp, &rp);
    }

    /// Job lifecycle through the wrapper:
    /// `add_job` → `start_processing_job` → `update_heart_beat`
    /// → `delete_job`. Exercises the
    /// `MetaCommandResult::OptionalIdRow` round-trip (returned by
    /// `add_job` and `start_processing_job`) plus the M3.4.c/d
    /// leader-stamped `now` plumbing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn job_lifecycle_via_metastore_trait() {
        use crate::metastore::job::{Job, JobType};
        // RowKey is re-exported via `pub use rocks_store::*` at the
        // metastore root, but `rocks_store` itself is `mod` (private).
        use crate::metastore::{RowKey, TableId};

        let (wrapper, sp, rp, _raft_dir) = setup_wrapper("raft_meta_store_job_lifecycle");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // add_job — returns Some(IdRow<Job>) on success.
        let job = Job::new(
            RowKey::Table(TableId::Partitions, 1),
            JobType::PartitionCompaction,
            "node1".to_string(),
        );
        let added = MetaStore::add_job(&*wrapper, job)
            .await
            .expect("add_job");
        let added = added.expect("add_job must produce a row");
        let job_id = added.get_id();

        // start_processing_job — pulls the job we just added.
        let picked = MetaStore::start_processing_job(&*wrapper, "node1".into(), false)
            .await
            .expect("start_processing_job");
        let picked = picked.expect("a queued job must be available");
        assert_eq!(picked.get_id(), job_id);

        // update_heart_beat — refreshes `last_heart_beat`.
        let beat = MetaStore::update_heart_beat(&*wrapper, job_id)
            .await
            .expect("update_heart_beat");
        assert!(
            beat.get_row().last_heart_beat() >= picked.get_row().last_heart_beat(),
            "heart_beat must be monotonic"
        );

        // delete_job — removes and returns the row.
        let deleted = MetaStore::delete_job(&*wrapper, job_id)
            .await
            .expect("delete_job");
        assert_eq!(deleted.get_id(), job_id);

        // all_jobs — empty after delete.
        let remaining = MetaStore::all_jobs(&*wrapper)
            .await
            .expect("all_jobs");
        assert!(remaining.is_empty(), "queue must be empty after delete");

        cleanup(&sp, &rp);
    }

    // =========================================================================
    // M5.4 — explicit snapshot trigger end-to-end
    // =========================================================================
    //
    // Drives a full snapshot through the production wrapper:
    // - apply some writes through the MetaStore trait
    // - call trigger_snapshot
    // - read back via storage.read_snapshot
    // - assert metadata matches the applied state and data is non-empty

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trigger_snapshot_persists_metadata_and_data() {
        let (wrapper, sp, rp, raft_dir) =
            setup_wrapper("raft_trigger_snapshot_smoke");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Plant some state through the trait so the snapshot has
        // something non-trivial to capture.
        for n in &["alpha", "beta", "gamma"] {
            wrapper
                .create_schema(n.to_string(), false)
                .await
                .expect("create schema");
        }

        // Build + persist the snapshot. Staging dir lives next to
        // the raft log so we don't cross filesystems. `keep_past=u64::MAX`
        // disables compaction so this test doesn't depend on it —
        // compaction has its own dedicated test below.
        let staging = raft_dir.path().join("snapshot-staging");
        wrapper
            .trigger_snapshot(&staging, u64::MAX)
            .await
            .expect("trigger_snapshot");

        // Read it back via the underlying storage (M5.1).
        use raft::Storage;
        let storage = wrapper.raft.storage();
        let snap = storage
            .read_snapshot()
            .expect("read_snapshot")
            .expect("snapshot must be present after trigger");

        // Metadata should reflect a non-trivial applied index — at
        // least one entry per CreateSchema command was committed.
        // ConfState voters: single-node = [1].
        let meta = snap.get_metadata();
        assert!(
            meta.index >= 3,
            "snapshot index should be >= 3 (one per create_schema), got {}",
            meta.index
        );
        assert_eq!(meta.get_conf_state().voters, vec![1]);

        // Data should be the packed RocksDB checkpoint — non-empty
        // and large enough to contain at least the CURRENT marker
        // (~16 bytes) plus a MANIFEST.
        assert!(
            snap.get_data().len() > 100,
            "snapshot data suspiciously small: {} bytes",
            snap.get_data().len()
        );

        // Storage::snapshot must satisfy any request_index up to the
        // current snapshot.index.
        let s = Storage::snapshot(&storage, meta.index, 0).expect("snapshot");
        assert_eq!(s.get_metadata().index, meta.index);

        cleanup(&sp, &rp);
    }

    // =========================================================================
    // M6.2 — leader-hint parser unit tests
    // =========================================================================

    #[test]
    fn parse_leader_hint_extracts_decimal_id() {
        let err = CubeError::internal(
            "this node is a follower; leader raft-leader-id=2 (raft propose: ProposalDropped)"
                .into(),
        );
        assert_eq!(RaftMetaStore::parse_leader_hint(&err), Some(2));
    }

    #[test]
    fn parse_leader_hint_handles_id_at_eol() {
        let err = CubeError::internal("see raft-leader-id=42".into());
        assert_eq!(RaftMetaStore::parse_leader_hint(&err), Some(42));
    }

    #[test]
    fn parse_leader_hint_returns_none_when_marker_missing() {
        let err = CubeError::internal("no leader currently elected".into());
        assert_eq!(RaftMetaStore::parse_leader_hint(&err), None);
    }

    #[test]
    fn parse_leader_hint_returns_none_for_garbage_value() {
        let err = CubeError::internal("raft-leader-id=abc".into());
        assert_eq!(RaftMetaStore::parse_leader_hint(&err), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trigger_snapshot_uploads_to_remote_fs() {
        let (wrapper, sp, rp, raft_dir) =
            setup_wrapper("raft_trigger_snapshot_remote_upload");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        wrapper
            .create_schema("upload-test".into(), false)
            .await
            .expect("create schema");

        let staging = raft_dir.path().join("snapshot-staging");
        wrapper
            .trigger_snapshot(&staging, u64::MAX)
            .await
            .expect("trigger_snapshot");

        // The fixed remote layout used by upload_raft_snapshot_file:
        // `raft-snapshots/latest.bin`. LocalDirRemoteFs writes into
        // the remote_path we constructed in setup_wrapper, so the
        // upload landed at `<rp>/raft-snapshots/latest.bin`.
        let uploaded = rp.join("raft-snapshots").join("latest.bin");
        assert!(
            uploaded.exists(),
            "snapshot.bin must be uploaded to remote at {:?}",
            uploaded
        );

        // Sanity: the uploaded file should equal the local
        // snapshot.bin byte-for-byte.
        use raft::Storage;
        let storage = wrapper.raft.storage();
        let local = storage.snapshot_data_path();
        let local_bytes = std::fs::read(&local).expect("read local snapshot.bin");
        let remote_bytes = std::fs::read(&uploaded).expect("read uploaded snapshot.bin");
        assert_eq!(
            local_bytes, remote_bytes,
            "remote snapshot must match local byte-for-byte"
        );

        // Storage::snapshot still works after upload (sanity).
        let _ = Storage::snapshot(&storage, 0, 0).expect("storage snapshot still readable");

        cleanup(&sp, &rp);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trigger_snapshot_compacts_log_past_keep_past() {
        let (wrapper, sp, rp, raft_dir) =
            setup_wrapper("raft_trigger_snapshot_compaction");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Drive enough writes that snapshot.index is well above
        // keep_past — 10 schemas → 10 raft entries. With keep_past=2,
        // compaction must drop entries [1..=8] and retain [9, 10].
        for n in 0..10 {
            wrapper
                .create_schema(format!("s{}", n), false)
                .await
                .expect("create schema");
        }

        use raft::Storage;
        let storage = wrapper.raft.storage();
        let pre_first =
            Storage::first_index(&storage).expect("pre first_index");
        let pre_last =
            Storage::last_index(&storage).expect("pre last_index");
        assert!(
            pre_last - pre_first >= 9,
            "test wants ≥10 entries; pre log has {}..={}",
            pre_first,
            pre_last
        );

        let staging = raft_dir.path().join("snapshot-staging");
        wrapper
            .trigger_snapshot(&staging, 2)
            .await
            .expect("trigger_snapshot");

        let post_first =
            Storage::first_index(&storage).expect("post first_index");
        let post_last =
            Storage::last_index(&storage).expect("post last_index");

        // last_index unchanged — compaction removes prefix only.
        assert_eq!(post_last, pre_last, "compaction must not touch tail");

        // first_index moved past the snapshot point. Specifically the
        // surviving prefix is `applied - keep_past`. We don't pin the
        // exact applied_index because it depends on whether the empty
        // post-election entry counts; just assert it advanced.
        assert!(
            post_first > pre_first,
            "first_index should advance: pre={} post={}",
            pre_first,
            post_first
        );
        assert!(
            post_last - post_first <= 4,
            "post-compaction log should be small (~keep_past + slack), got {}..={}",
            post_first,
            post_last
        );

        cleanup(&sp, &rp);
    }
}
