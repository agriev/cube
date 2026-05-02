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
//! ## What's in M3.5.b (this commit)
//!
//! Full `impl MetaStore for RaftMetaStore` — all 121 trait methods.
//! Reads delegate, writes route through Raft when a `MetaCommand`
//! variant exists, and the small set of writes without a variant
//! delegate to the local store with a `// M3.5.* TODO` annotation
//! that grep-finds at M3.5.c-completion time. Writes that fall
//! through the TODO path are **not** replicated — the production
//! gate `CUBESTORE_HA_MODE` (M3.5.c) must stay closed until those
//! holes are closed.
//!
//! Variants without a current MetaCommand (delegated locally for
//! now): `chunk_update_last_inserted`, `deactivate_chunk`,
//! `deactivate_chunks`, `insert_chunks`, `delete_all_jobs`,
//! `commit_multi_partition_split`, `prepare_multi_partition_for_split`,
//! `prepare_multi_split_finish`. M3.5.b.1+ will fold these in as
//! they're needed by passing tests.
//!
//! ## Determinism caveat (still unfixed)
//!
//! Write methods build row structs (`Table::new`, `Chunk::new`, etc.)
//! that internally stamp `Utc::now()`. Today the wrapper proposes
//! the bare args and the apply path on each replica builds its own
//! struct with its own wall-clock — they will diverge.
//!
//! The audit (M3.4 audit, parked here): the only `Utc::now()` /
//! `SystemTime::now()` sites in `metastore/mod.rs` are in **read**
//! methods (filtering by elapsed time) — those are safe. The real
//! gap is in row constructors (`Table::new` line ~230,
//! `Chunk::new` line ~29 + ~30, `Job::new` line ~82 + ~107,
//! `ReplayHandle::new` line ~119 + ~144, `Chunk::set_deactivated_at`
//! line ~93). M3.4 lands the leader-stamp pattern that closes
//! these. **Do not flip `CUBESTORE_HA_MODE=raft` (M3.5.c) until
//! M3.4 is in.**

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
    ///
    /// Multi-node boot lands in M4.
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

    /// The local `RocksMetaStore`. Tests and `M3.5.c` config wiring
    /// use this. Read methods on the trait delegate here; write
    /// methods go through Raft.
    pub fn local_store(&self) -> &Arc<RocksMetaStore> {
        &self.store
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
    // M3.5.* TODO: prepare_multi_partition_for_split is read+write hybrid;
    // requires careful design to split the read piece from the write
    // piece before HA mode can use it. Falls through locally for now.
    async fn prepare_multi_partition_for_split(
        &self,
        multi_partition_id: u64,
    ) -> Result<(IdRow<MultiIndex>, IdRow<MultiPartition>, Vec<PartitionData>), CubeError> {
        self.store
            .prepare_multi_partition_for_split(multi_partition_id)
            .await
    }
    // M3.5.* TODO: commit_multi_partition_split — Cat E with nested
    // tuple Vecs. Needs its own MetaCommand variant. Falls through.
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
        self.store
            .commit_multi_partition_split(
                multi_partition_id,
                new_multi_partitions,
                new_multi_partition_rows,
                old_partitions,
                new_partitions,
                new_partition_rows,
                initial_split,
            )
            .await
    }
    async fn find_unsplit_partitions(
        &self,
        multi_partition_id: u64,
    ) -> Result<Vec<u64>, CubeError> {
        self.store.find_unsplit_partitions(multi_partition_id).await
    }
    // M3.5.* TODO: prepare_multi_split_finish — read+write hybrid.
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
    // M3.5.* TODO: insert_chunks returns Vec<IdRow<Chunk>> — requires
    // a new MetaCommandResult::IdRowList variant. Falls through.
    async fn insert_chunks(&self, chunks: Vec<Chunk>) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        self.store.insert_chunks(chunks).await
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
    // M3.5.* TODO: chunk_update_last_inserted — needs a MetaCommand
    // variant carrying Option<DateTime<Utc>> as Option<i64>.
    async fn chunk_update_last_inserted(
        &self,
        chunk_ids: Vec<u64>,
        last_inserted_at: Option<DateTime<Utc>>,
    ) -> Result<(), CubeError> {
        self.store
            .chunk_update_last_inserted(chunk_ids, last_inserted_at)
            .await
    }
    // M3.5.* TODO: deactivate_chunk — Cat A but no variant yet.
    async fn deactivate_chunk(&self, chunk_id: u64) -> Result<(), CubeError> {
        self.store.deactivate_chunk(chunk_id).await
    }
    // M3.5.* TODO: deactivate_chunks — Cat E small.
    async fn deactivate_chunks(&self, chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        self.store.deactivate_chunks(chunk_ids).await
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
        self.raft
            .propose(MetaCommand::StartProcessingJob {
                server_name,
                long_term,
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
        self.raft
            .propose(MetaCommand::UpdateStatus {
                job_id,
                status_blob,
            })
            .await?
            .into_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("update_status", e))
    }
    async fn update_heart_beat(&self, job_id: u64) -> Result<IdRow<Job>, CubeError> {
        self.raft
            .propose(MetaCommand::UpdateHeartBeat { job_id })
            .await?
            .into_id_row(IdRowKind::Job)
            .map_err(|e| Self::mismatch("update_heart_beat", e))
    }
    // M3.5.* TODO: delete_all_jobs returns Vec<IdRow<Job>> — needs
    // a new MetaCommandResult::IdRowList variant.
    async fn delete_all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        self.store.delete_all_jobs().await
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
        self.raft
            .propose(MetaCommand::CreateReplayHandle {
                table_id,
                location_index: location_index as u64,
                seq_pointer_blob,
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
        self.raft
            .propose(MetaCommand::CreateReplayHandleFromSeqPointers {
                table_id,
                seq_pointers_blob,
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
}
