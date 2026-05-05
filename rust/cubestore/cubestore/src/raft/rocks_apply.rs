//! `RocksMetaStoreApply` — the production `Apply` impl that dispatches
//! each `MetaCommand` to the matching `MetaStore` write method on the
//! local `RocksMetaStore`.
//!
//! ## How dispatch works
//!
//! `Apply::apply` is async (M3.3). Each match arm:
//! 1. Decodes the variant's fields back into the trait method's
//!    arguments (most are direct; `CreatePartition` flexbuffer-decodes
//!    `partition_blob` into a `Partition`; `SetCurrentSnapshot` joins
//!    its split u128).
//! 2. Calls the trait method on `self.store`. The method goes through
//!    `RocksStore::write_operation` which serializes against the
//!    `rw_loop` and atomically commits a single RocksDB `WriteBatch`.
//! 3. Wraps the trait's return into the matching `MetaCommandResult`
//!    variant via the helper constructors in `command.rs`.
//!
//! ## Determinism (M3.4 — closed)
//!
//! Every persistent write that carries a `DateTime<Utc>` field is
//! leader-stamped via `assigned_now_millis` on the propose side and
//! decoded on the apply side via `decode_required_millis` /
//! `decode_optional_millis`. The local `_with_now` helpers
//! (`create_table_with_now`, `update_heart_beat_with_now`, …) thread
//! the leader's `now` straight into the row constructor so all
//! replicas produce a byte-identical RocksDB write.
//!
//! See `raft_meta_store.rs` for the per-method propose path,
//! `docs/ha/M3.4-AUDIT.md` for the per-`Utc::now()` classification,
//! and `tests::pure_replay_is_byte_deterministic_across_replicas`
//! below for the regression test.
//!
//! ## Sub-milestones
//!
//! M3.3.a (this commit) — dispatch for the M3.1/M3.2 variants
//! (~28 of 86 trait writes). Variants whose trait method takes a
//! complex multi-arg shape (`CreateTable`, `SwapActivePartitions`)
//! still ship as opaque `payload_version + payload`; their dispatch
//! is a `not yet implemented` error so a misconfigured caller fails
//! loud rather than silently dropping data.
//!
//! M3.3.b — fill the remaining ~58 variants (Cat C/D/E/F).
//! M3.3.c — single-`write_operation` `Batch` so multi-statement DDL
//! is atomic across the dispatched commands. Until then, `Batch`
//! returns an error.

use crate::metastore::job::{Job, JobStatus};
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::replay_handle::SeqPointer;
use crate::metastore::source::SourceCredentials;
use crate::metastore::table::StreamOffset;
use crate::metastore::{
    Chunk, Column, IdRow, ImportFormat, IndexDef, MetaStore, Partition, RocksMetaStore,
};
use chrono::{DateTime, TimeZone, Utc};
use crate::raft::command::{IdRowKind, MetaCommand, MetaCommandResult};
use crate::raft::state_machine::Apply;
// Row is imported inside the SwapActivePartitions arm where it's used.
use crate::CubeError;
use async_trait::async_trait;
use flexbuffers::Reader;
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// Production `Apply` impl. Holds `Arc<RocksMetaStore>` so the dispatch
/// can call any of the trait methods.
pub struct RocksMetaStoreApply {
    store: Arc<RocksMetaStore>,
}

impl RocksMetaStoreApply {
    pub fn new(store: Arc<RocksMetaStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Apply for RocksMetaStoreApply {
    async fn apply(&self, cmd: MetaCommand) -> Result<MetaCommandResult, CubeError> {
        match cmd {
            // ---- Schema ----------------------------------------------------
            MetaCommand::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                let row = self.store.create_schema(schema_name, if_not_exists).await?;
                wrap_id_row(IdRowKind::Schema, &row)
            }
            MetaCommand::RenameSchema {
                old_schema_name,
                new_schema_name,
            } => {
                let row = self
                    .store
                    .rename_schema(old_schema_name, new_schema_name)
                    .await?;
                wrap_id_row(IdRowKind::Schema, &row)
            }
            MetaCommand::RenameSchemaById {
                schema_id,
                new_schema_name,
            } => {
                let row = self
                    .store
                    .rename_schema_by_id(schema_id, new_schema_name)
                    .await?;
                wrap_id_row(IdRowKind::Schema, &row)
            }
            MetaCommand::DeleteSchema { schema_name } => {
                self.store.delete_schema(schema_name).await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::DeleteSchemaById { schema_id } => {
                self.store.delete_schema_by_id(schema_id).await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- Tables ----------------------------------------------------
            MetaCommand::DropTable { table_id } => {
                let row = self.store.drop_table(table_id).await?;
                wrap_id_row(IdRowKind::Table, &row)
            }
            MetaCommand::SealTable { table_id } => {
                let row = self.store.seal_table(table_id).await?;
                wrap_id_row(IdRowKind::Table, &row)
            }
            MetaCommand::UpdateLocationDownloadSize {
                table_id,
                location,
                download_size,
            } => {
                let row = self
                    .store
                    .update_location_download_size(table_id, location, download_size)
                    .await?;
                wrap_id_row(IdRowKind::Table, &row)
            }

            // ---- Partitions ------------------------------------------------
            MetaCommand::CreatePartition { partition_blob } => {
                let partition: Partition = decode_typed_blob(&partition_blob, "partition_blob")?;
                let row = self.store.create_partition(partition).await?;
                wrap_id_row(IdRowKind::Partition, &row)
            }
            MetaCommand::DeletePartition { partition_id } => {
                let row = self.store.delete_partition(partition_id).await?;
                wrap_id_row(IdRowKind::Partition, &row)
            }
            MetaCommand::MarkPartitionWarmedUp { partition_id } => {
                self.store.mark_partition_warmed_up(partition_id).await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::DeleteMiddleManPartition { partition_id } => {
                let row = self.store.delete_middle_man_partition(partition_id).await?;
                wrap_id_row(IdRowKind::Partition, &row)
            }

            // ---- Cat C: create-with-struct (M3.3.b.1, refined M3.4.a) -----
            // M3.4.a: CreateChunk ships a fully-built `Chunk` (blob)
            // rather than raw args. The leader resolved
            // `created_at`/`oldest_insert_at` (Utc::now) and `suffix`
            // (random) before propose, so each replica inserts a
            // byte-identical row. See `Chunk::new_pure`.
            MetaCommand::CreateChunk { chunk_blob } => {
                let chunk: Chunk = decode_typed_blob(&chunk_blob, "chunk_blob")?;
                let row = self.store.insert_chunk_pre_built(chunk).await?;
                wrap_id_row(IdRowKind::Chunk, &row)
            }
            MetaCommand::CreateWal {
                table_id,
                row_count,
            } => {
                let row_count_usize = usize::try_from(row_count).map_err(|_| {
                    CubeError::internal(format!(
                        "CreateWal row_count {} exceeds usize::MAX on this platform",
                        row_count
                    ))
                })?;
                let row = self.store.create_wal(table_id, row_count_usize).await?;
                wrap_id_row(IdRowKind::Wal, &row)
            }
            MetaCommand::CreateIndex {
                schema_name,
                table_name,
                index_def_blob,
            } => {
                let index_def: IndexDef = decode_typed_blob(&index_def_blob, "index_def_blob")?;
                let row = self
                    .store
                    .create_index(schema_name, table_name, index_def)
                    .await?;
                wrap_id_row(IdRowKind::Index, &row)
            }
            MetaCommand::CreatePartitionedIndex {
                schema,
                name,
                columns_blob,
                if_not_exists,
            } => {
                let columns: Vec<Column> = decode_typed_blob(&columns_blob, "columns_blob")?;
                let row = self
                    .store
                    .create_partitioned_index(schema, name, columns, if_not_exists)
                    .await?;
                wrap_id_row(IdRowKind::MultiIndex, &row)
            }
            MetaCommand::CreateMultiPartition {
                multi_partition_blob,
            } => {
                let mp: MultiPartition =
                    decode_typed_blob(&multi_partition_blob, "multi_partition_blob")?;
                let row = self.store.create_multi_partition(mp).await?;
                wrap_id_row(IdRowKind::MultiPartition, &row)
            }
            MetaCommand::CreateOrUpdateSource {
                name,
                credentials_blob,
            } => {
                let creds: SourceCredentials =
                    decode_typed_blob(&credentials_blob, "credentials_blob")?;
                let row = self.store.create_or_update_source(name, creds).await?;
                wrap_id_row(IdRowKind::Source, &row)
            }
            MetaCommand::CreateReplayHandle {
                table_id,
                location_index,
                seq_pointer_blob,
                assigned_now_millis,
            } => {
                let seq: SeqPointer = decode_typed_blob(&seq_pointer_blob, "seq_pointer_blob")?;
                let location_usize = usize::try_from(location_index).map_err(|_| {
                    CubeError::internal(format!(
                        "CreateReplayHandle location_index {} exceeds usize::MAX",
                        location_index
                    ))
                })?;
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let row = self
                    .store
                    .create_replay_handle_with_now(table_id, location_usize, seq, now)
                    .await?;
                wrap_id_row(IdRowKind::ReplayHandle, &row)
            }
            MetaCommand::CreateReplayHandleFromSeqPointers {
                table_id,
                seq_pointers_blob,
                assigned_now_millis,
            } => {
                let seq_pointers: Option<Vec<Option<SeqPointer>>> =
                    decode_typed_blob(&seq_pointers_blob, "seq_pointers_blob")?;
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let row = self
                    .store
                    .create_replay_handle_from_seq_pointers_with_now(
                        table_id,
                        seq_pointers,
                        now,
                    )
                    .await?;
                wrap_id_row(IdRowKind::ReplayHandle, &row)
            }

            // ---- Chunks ----------------------------------------------------
            MetaCommand::SwapCompactedChunks {
                partition_id,
                old_chunk_ids,
                new_chunk,
                new_chunk_file_size,
            } => {
                let did_swap = self
                    .store
                    .swap_compacted_chunks(
                        partition_id,
                        old_chunk_ids,
                        new_chunk,
                        new_chunk_file_size,
                    )
                    .await?;
                Ok(MetaCommandResult::Bool(did_swap))
            }
            MetaCommand::DeleteChunk { chunk_id } => {
                let row = self.store.delete_chunk(chunk_id).await?;
                wrap_id_row(IdRowKind::Chunk, &row)
            }
            MetaCommand::DeleteChunksWithoutChecks { chunk_ids } => {
                self.store.delete_chunks_without_checks(chunk_ids).await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- WAL -------------------------------------------------------
            MetaCommand::DeleteWal { wal_id } => {
                self.store.delete_wal(wal_id).await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::WalUploaded { wal_id } => {
                let row = self.store.wal_uploaded(wal_id).await?;
                wrap_id_row(IdRowKind::Wal, &row)
            }

            // ---- Chunk uploads / activates / swaps (M3.3.b.2 Cat E) -------
            MetaCommand::ChunkUploaded { chunk_id } => {
                let row = self.store.chunk_uploaded(chunk_id).await?;
                wrap_id_row(IdRowKind::Chunk, &row)
            }
            MetaCommand::SwapChunks {
                deactivate_ids,
                uploaded_ids_and_sizes,
                new_replay_handle_id,
            } => {
                self.store
                    .swap_chunks(deactivate_ids, uploaded_ids_and_sizes, new_replay_handle_id)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::SwapChunksWithoutCheck {
                deactivate_ids,
                uploaded_ids_and_sizes,
                new_replay_handle_id,
            } => {
                self.store
                    .swap_chunks_without_check(
                        deactivate_ids,
                        uploaded_ids_and_sizes,
                        new_replay_handle_id,
                    )
                    .await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::DeactivateChunksWithoutCheck { deactivate_ids } => {
                self.store
                    .deactivate_chunks_without_check(deactivate_ids)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::DeactivateChunk {
                chunk_id,
                assigned_now_millis,
            } => {
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                self.store.deactivate_chunk_with_now(chunk_id, now).await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::DeactivateChunks {
                chunk_ids,
                assigned_now_millis,
            } => {
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                self.store
                    .deactivate_chunks_with_now(chunk_ids, now)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::ActivateChunks {
                table_id,
                uploaded_chunk_ids,
                replay_handle_id,
            } => {
                self.store
                    .activate_chunks(table_id, uploaded_chunk_ids, replay_handle_id)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- M3.7: insert_chunks + chunk_update_last_inserted ---------
            MetaCommand::InsertChunks { chunks_blob } => {
                let chunks: Vec<Chunk> = decode_typed_blob(&chunks_blob, "chunks_blob")?;
                let rows = self.store.insert_chunks(chunks).await?;
                wrap_id_row_list(IdRowKind::Chunk, &rows)
            }
            MetaCommand::ChunkUpdateLastInserted {
                chunk_ids,
                last_inserted_at_millis,
            } => {
                let last_inserted_at = decode_optional_millis(
                    last_inserted_at_millis,
                    "last_inserted_at_millis",
                )?;
                self.store
                    .chunk_update_last_inserted(chunk_ids, last_inserted_at)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- Tables: ready (M3.3.b.2) ---------------------------------
            MetaCommand::TableReady { table_id, is_ready } => {
                let row = self.store.table_ready(table_id, is_ready).await?;
                wrap_id_row(IdRowKind::Table, &row)
            }

            // ---- Indexes: drop_partitioned_index (M3.3.b.2) ---------------
            MetaCommand::DropPartitionedIndex { schema, name } => {
                self.store.drop_partitioned_index(schema, name).await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- Jobs ------------------------------------------------------
            MetaCommand::DeleteJob { job_id } => {
                let row = self.store.delete_job(job_id).await?;
                wrap_id_row(IdRowKind::Job, &row)
            }
            MetaCommand::UpdateHeartBeat {
                job_id,
                assigned_now_millis,
            } => {
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let row = self.store.update_heart_beat_with_now(job_id, now).await?;
                wrap_id_row(IdRowKind::Job, &row)
            }
            // M3.7
            MetaCommand::DeleteAllJobs => {
                let rows = self.store.delete_all_jobs().await?;
                wrap_id_row_list(IdRowKind::Job, &rows)
            }
            MetaCommand::AddJob { job_blob } => {
                let job: Job = decode_typed_blob(&job_blob, "job_blob")?;
                let opt = self.store.add_job(job).await?;
                MetaCommandResult::optional_id_row(IdRowKind::Job, opt.as_ref()).map_err(|e| {
                    CubeError::internal(format!("encode AddJob result: {}", e))
                })
            }
            MetaCommand::StartProcessingJob {
                server_name,
                long_term,
                assigned_now_millis,
            } => {
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let opt = self
                    .store
                    .start_processing_job_with_now(server_name, long_term, now)
                    .await?;
                MetaCommandResult::optional_id_row(IdRowKind::Job, opt.as_ref()).map_err(|e| {
                    CubeError::internal(format!("encode StartProcessingJob result: {}", e))
                })
            }
            MetaCommand::UpdateStatus {
                job_id,
                status_blob,
                assigned_now_millis,
            } => {
                let status: JobStatus = decode_typed_blob(&status_blob, "status_blob")?;
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let row = self
                    .store
                    .update_status_with_now(job_id, status, now)
                    .await?;
                wrap_id_row(IdRowKind::Job, &row)
            }

            // ---- Sources ---------------------------------------------------
            MetaCommand::DeleteSource { id } => {
                let row = self.store.delete_source(id).await?;
                wrap_id_row(IdRowKind::Source, &row)
            }

            // ---- Replay handle ops (M3.3.b.2) -----------------------------
            MetaCommand::UpdateReplayHandleFailedIfExists { id, failed } => {
                self.store
                    .update_replay_handle_failed_if_exists(id, failed)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }
            MetaCommand::ReplaceReplayHandles {
                old_ids,
                new_seq_pointer_blob,
            } => {
                let new_seq: Option<Vec<Option<SeqPointer>>> =
                    decode_typed_blob(&new_seq_pointer_blob, "new_seq_pointer_blob")?;
                let opt = self.store.replace_replay_handles(old_ids, new_seq).await?;
                MetaCommandResult::optional_id_row(IdRowKind::ReplayHandle, opt.as_ref()).map_err(
                    |e| {
                        CubeError::internal(format!(
                            "encode ReplaceReplayHandles result: {}",
                            e
                        ))
                    },
                )
            }

            // ---- Snapshot --------------------------------------------------
            MetaCommand::SetCurrentSnapshot {
                snapshot_id_low,
                snapshot_id_high,
            } => {
                // Re-join the split u128. flexbuffers doesn't have native
                // u128 so M3.1 split it; here we re-form before calling
                // the trait method.
                let snapshot_id =
                    (snapshot_id_high as u128) << 64 | snapshot_id_low as u128;
                self.store.set_current_snapshot(snapshot_id).await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- Cat D: CreateTable structured form (M3.3.b.3, refined M3.4.b.2)
            // M3.4.b.2: read the leader-stamped `assigned_now_millis`
            // and call `create_table_with_now` so the resulting Table's
            // `created_at` is identical on every replica.
            MetaCommand::CreateTable {
                schema_name,
                table_name,
                columns_blob,
                locations,
                import_format_blob,
                indexes_blob,
                is_ready,
                build_range_end_millis,
                seal_at_millis,
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
            } => {
                let columns: Vec<Column> = decode_typed_blob(&columns_blob, "columns_blob")?;
                let import_format: Option<ImportFormat> = decode_optional_blob(
                    import_format_blob.as_deref(),
                    "import_format_blob",
                )?;
                let indexes: Vec<IndexDef> = decode_typed_blob(&indexes_blob, "indexes_blob")?;
                let source_columns: Option<Vec<Column>> = decode_optional_blob(
                    source_columns_blob.as_deref(),
                    "source_columns_blob",
                )?;
                let stream_offset: Option<StreamOffset> = decode_optional_blob(
                    stream_offset_blob.as_deref(),
                    "stream_offset_blob",
                )?;
                let build_range_end = decode_optional_millis(
                    build_range_end_millis,
                    "build_range_end_millis",
                )?;
                let seal_at = decode_optional_millis(seal_at_millis, "seal_at_millis")?;
                let now = decode_required_millis(assigned_now_millis, "assigned_now_millis")?;
                let row = self
                    .store
                    .create_table_with_now(
                        schema_name,
                        table_name,
                        columns,
                        locations,
                        import_format,
                        indexes,
                        is_ready,
                        build_range_end,
                        seal_at,
                        select_statement,
                        source_columns,
                        stream_offset,
                        unique_key_column_names,
                        aggregates,
                        partition_split_threshold,
                        trace_obj,
                        drop_if_exists,
                        extension,
                        now,
                    )
                    .await?;
                wrap_id_row(IdRowKind::Table, &row)
            }

            // ---- SwapActivePartitions structured form (M3.3.b.4) ----------
            MetaCommand::SwapActivePartitions {
                current_active_blob,
                new_active_blob,
                new_active_min_max_blob,
            } => {
                use crate::metastore::{Chunk, IdRow};
                use crate::table::Row;
                let current_active: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)> =
                    decode_typed_blob(&current_active_blob, "current_active_blob")?;
                let new_active: Vec<(IdRow<Partition>, u64)> =
                    decode_typed_blob(&new_active_blob, "new_active_blob")?;
                let new_active_min_max: Vec<(
                    u64,
                    (Option<Row>, Option<Row>),
                    (Option<Row>, Option<Row>),
                )> = decode_typed_blob(&new_active_min_max_blob, "new_active_min_max_blob")?;
                self.store
                    .swap_active_partitions(current_active, new_active, new_active_min_max)
                    .await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- M3.7: commit_multi_partition_split ----------------------
            MetaCommand::CommitMultiPartitionSplit {
                multi_partition_id,
                new_multi_partitions,
                new_multi_partition_rows,
                old_partitions_blob,
                new_partitions_blob,
                new_partition_rows,
                initial_split,
            } => {
                let old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)> =
                    decode_typed_blob(&old_partitions_blob, "old_partitions_blob")?;
                let new_partitions: Vec<(IdRow<Partition>, u64)> =
                    decode_typed_blob(&new_partitions_blob, "new_partitions_blob")?;
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
                    .await?;
                Ok(MetaCommandResult::Unit)
            }

            // ---- Still deferred -------------------------------------------
            MetaCommand::AcquirePartitionedLock { .. } => Err(not_yet_implemented(
                "AcquirePartitionedLock",
                "cluster-side lock op, no MetaStore equivalent yet",
            )),
            MetaCommand::ReleasePartitionedLock { .. } => Err(not_yet_implemented(
                "ReleasePartitionedLock",
                "cluster-side lock op, no MetaStore equivalent yet",
            )),

            // ---- Batch (M3.3.c) --------------------------------------------
            // Apply each sub-command in order. Determinism is preserved
            // because every replica sees the same Raft entry and walks
            // the same loop in the same order.
            //
            // **Atomicity caveat (M3.3.c)**: each sub-command runs as
            // its own `write_operation`, so an N-statement batch is
            // N independent RocksDB WriteBatches, not one atomic one.
            // If a mid-batch sub-command errors, the prefix has
            // already committed. M3.3.c.atomic is the follow-up that
            // routes the whole batch through a single
            // `write_operation` with a shared `BatchPipe` — that
            // requires sync `_in_batch_pipe` helpers per variant
            // (mirroring the existing `drop_table_impl`).
            //
            // Nested Batch is rejected — flatten on the wrapper if
            // you need a flat sequence.
            MetaCommand::Batch { commands } => {
                for cmd in commands {
                    if let MetaCommand::Batch { .. } = cmd {
                        return Err(CubeError::internal(
                            "MetaCommand::Batch nested inside Batch is rejected — \
                             flatten on the wrapper before propose"
                                .into(),
                        ));
                    }
                    Box::pin(self.apply(cmd)).await?;
                }
                Ok(MetaCommandResult::Unit)
            }

            // ---- Generic escape hatch -------------------------------------
            // M1→M3 transition tool; once every method has a typed variant
            // (M3.3.b), Generic should never be applied. If it lands at
            // apply time, it indicates a stale peer in a rolling upgrade
            // proposing methods we don't know how to handle here.
            MetaCommand::Generic { method, .. } => Err(CubeError::internal(format!(
                "Generic command for `{}` cannot be dispatched on this replica — \
                 this peer is older than the leader and is missing the typed variant. \
                 Upgrade the binary or roll back the leader.",
                method
            ))),
        }
    }
}

fn wrap_id_row<T: serde::Serialize>(
    kind: IdRowKind,
    row: &T,
) -> Result<MetaCommandResult, CubeError> {
    MetaCommandResult::id_row(kind, row).map_err(|e| {
        CubeError::internal(format!(
            "encode IdRow<{:?}> for apply result: {}",
            kind, e
        ))
    })
}

fn wrap_id_row_list<T: serde::Serialize>(
    kind: IdRowKind,
    rows: &[T],
) -> Result<MetaCommandResult, CubeError> {
    MetaCommandResult::id_row_list(kind, rows).map_err(|e| {
        CubeError::internal(format!(
            "encode IdRowList<{:?}> for apply result: {}",
            kind, e
        ))
    })
}

fn decode_flex<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let r = Reader::get_root(bytes).map_err(|e| e.to_string())?;
    T::deserialize(r).map_err(|e| e.to_string())
}

/// Decode a blob field that's always present. Returns a `CubeError`
/// already wrapped in the format `Apply` callers expect.
fn decode_typed_blob<T: DeserializeOwned>(bytes: &[u8], field: &str) -> Result<T, CubeError> {
    decode_flex(bytes).map_err(|e| {
        CubeError::internal(format!(
            "MetaCommand apply: {} decode failed: {}",
            field, e
        ))
    })
}

/// Decode an `Option<...>` blob: `None` skips, `Some(bytes)` decodes.
fn decode_optional_blob<T: DeserializeOwned>(
    bytes: Option<&[u8]>,
    field: &str,
) -> Result<Option<T>, CubeError> {
    match bytes {
        None => Ok(None),
        Some(b) => decode_typed_blob(b, field).map(Some),
    }
}

/// Decode `Option<i64>` ms-since-epoch into `Option<DateTime<Utc>>`.
/// Out-of-range timestamps (chrono can't represent them) error rather
/// than silently wrap — that surfaces a leader/follower wire bug.
fn decode_optional_millis(
    ms: Option<i64>,
    field: &str,
) -> Result<Option<DateTime<Utc>>, CubeError> {
    match ms {
        None => Ok(None),
        Some(v) => match Utc.timestamp_millis_opt(v).single() {
            Some(dt) => Ok(Some(dt)),
            None => Err(CubeError::internal(format!(
                "MetaCommand apply: {} = {} ms-since-epoch is out of \
                 representable DateTime<Utc> range",
                field, v
            ))),
        },
    }
}

/// Decode a required `i64` ms-since-epoch into `DateTime<Utc>`. Used
/// by M3.4.b.1 variants that carry a leader-stamped `now`.
fn decode_required_millis(ms: i64, field: &str) -> Result<DateTime<Utc>, CubeError> {
    Utc.timestamp_millis_opt(ms).single().ok_or_else(|| {
        CubeError::internal(format!(
            "MetaCommand apply: {} = {} ms-since-epoch is out of \
             representable DateTime<Utc> range",
            field, ms
        ))
    })
}

fn not_yet_implemented(variant: &str, reason: &str) -> CubeError {
    CubeError::internal(format!(
        "MetaCommand::{} dispatch not yet implemented ({}); see docs/ha/M3-NOTES.md",
        variant, reason
    ))
}

// =============================================================================
// Tests — dispatch correctness against a real local `RocksMetaStore`.
// =============================================================================
//
// These tests stand up a temp RocksMetaStore (no Raft) and exercise the
// apply dispatch directly. The point is to verify each match arm
// produces the right side effect and the right `MetaCommandResult`
// shape. The end-to-end Raft path is covered by a separate test in
// `state_machine.rs` (boots a real single-node Raft on top of this
// `Apply` impl and proves propose→apply→trait-return).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::{BaseRocksStoreFs, MetaStore, RocksMetaStore, Schema};
    use crate::remotefs::LocalDirRemoteFs;
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    /// Spin up a temp RocksMetaStore inside `<cwd>/<test_name>-{local,remote}`.
    /// This is the same shape used across `metastore/mod.rs::tests`.
    fn setup_store(test_name: &str) -> (Arc<RocksMetaStore>, PathBuf, PathBuf) {
        let config = Config::test(test_name);
        let store_path = env::current_dir()
            .unwrap()
            .join(format!("{}-local", test_name));
        let remote_store_path = env::current_dir()
            .unwrap()
            .join(format!("{}-remote", test_name));
        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_store_path);
        let remote_fs =
            LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let store = RocksMetaStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )
        .expect("RocksMetaStore::new");
        (store, store_path, remote_store_path)
    }

    fn cleanup(store_path: &PathBuf, remote_store_path: &PathBuf) {
        let _ = fs::remove_dir_all(store_path);
        let _ = fs::remove_dir_all(remote_store_path);
    }

    #[tokio::test]
    async fn create_schema_then_rename_dispatches_correctly() {
        let test_name = "raft_apply_create_rename_schema";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // CreateSchema → IdRow<Schema>
        let r = apply
            .apply(MetaCommand::CreateSchema {
                schema_name: "public".into(),
                if_not_exists: false,
            })
            .await
            .expect("apply CreateSchema");
        let row: crate::metastore::IdRow<Schema> = r
            .into_id_row(IdRowKind::Schema)
            .expect("decode IdRow<Schema>");
        assert_eq!(row.get_row().get_name(), "public");

        // The store's view must agree.
        let listed = store.get_schemas().await.expect("get_schemas");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].get_row().get_name(), "public");

        // RenameSchema → new IdRow with the same id, different name.
        let r = apply
            .apply(MetaCommand::RenameSchema {
                old_schema_name: "public".into(),
                new_schema_name: "renamed".into(),
            })
            .await
            .expect("apply RenameSchema");
        let row: crate::metastore::IdRow<Schema> = r
            .into_id_row(IdRowKind::Schema)
            .expect("decode IdRow<Schema>");
        assert_eq!(row.get_row().get_name(), "renamed");

        cleanup(&sp, &rp);
    }

    #[tokio::test]
    async fn delete_schema_returns_unit_and_clears_store() {
        let test_name = "raft_apply_delete_schema";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // Create then delete by name.
        apply
            .apply(MetaCommand::CreateSchema {
                schema_name: "to_drop".into(),
                if_not_exists: false,
            })
            .await
            .unwrap();

        let r = apply
            .apply(MetaCommand::DeleteSchema {
                schema_name: "to_drop".into(),
            })
            .await
            .expect("apply DeleteSchema");
        r.into_unit().expect("must be Unit");

        let listed = store.get_schemas().await.expect("get_schemas");
        assert!(listed.is_empty(), "schema must be gone after DeleteSchema");

        cleanup(&sp, &rp);
    }

    #[tokio::test]
    async fn deferred_variants_error_loudly() {
        let test_name = "raft_apply_deferred_variants";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // AcquirePartitionedLock has no trait equivalent yet — still
        // deferred, must error rather than silently succeed.
        let err = apply
            .apply(MetaCommand::AcquirePartitionedLock {
                payload_version: 1,
                payload: vec![1, 2, 3],
            })
            .await
            .expect_err("AcquirePartitionedLock must still error");
        assert!(
            err.message.contains("AcquirePartitionedLock"),
            "error must name the variant: {}",
            err.message
        );

        // Nested Batch is rejected — M3.3.c flattens before propose.
        let err = apply
            .apply(MetaCommand::Batch {
                commands: vec![MetaCommand::Batch { commands: vec![] }],
            })
            .await
            .expect_err("nested Batch must error");
        assert!(
            err.message.contains("nested inside Batch"),
            "error must explain nested-batch rejection: {}",
            err.message
        );

        // Generic always errors at apply (it's a transition escape hatch).
        let err = apply
            .apply(MetaCommand::Generic {
                method: "totally_made_up".into(),
                body: vec![],
            })
            .await
            .expect_err("Generic must error");
        assert!(err.message.contains("totally_made_up"));

        cleanup(&sp, &rp);
    }

    #[tokio::test]
    async fn batch_applies_each_command_in_order() {
        let test_name = "raft_apply_batch";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // Apply a Batch that creates two schemas in order.
        let r = apply
            .apply(MetaCommand::Batch {
                commands: vec![
                    MetaCommand::CreateSchema {
                        schema_name: "first".into(),
                        if_not_exists: false,
                    },
                    MetaCommand::CreateSchema {
                        schema_name: "second".into(),
                        if_not_exists: false,
                    },
                ],
            })
            .await
            .expect("apply Batch");
        r.into_unit().expect("Batch result is Unit");

        // Both schemas must be present in insertion order.
        let listed = store.get_schemas().await.expect("get_schemas");
        let names: Vec<String> = listed
            .into_iter()
            .map(|r| r.get_row().get_name().clone())
            .collect();
        assert!(
            names.contains(&"first".to_string()) && names.contains(&"second".to_string()),
            "Batch must apply both creates: {:?}",
            names
        );

        cleanup(&sp, &rp);
    }

    #[tokio::test]
    async fn snapshot_id_round_trips_through_split_u128() {
        let test_name = "raft_apply_snapshot_id";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // Pick a u128 with bits set in both halves so we can prove
        // the (low, high) split + rejoin doesn't mangle anything.
        // `set_current_snapshot` validates the id against existing
        // snapshots, so it will error — but the error message
        // contains the rejoined id, so we use that as the wire
        // round-trip assertion.
        let original: u128 =
            ((0x1234_5678_9ABC_DEF0u128) << 64) | 0xDEAD_BEEF_CAFE_BABEu128;
        let snapshot_id_low = original as u64;
        let snapshot_id_high = (original >> 64) as u64;

        let err = apply
            .apply(MetaCommand::SetCurrentSnapshot {
                snapshot_id_low,
                snapshot_id_high,
            })
            .await
            .expect_err("snapshot id never existed — error expected");
        let original_str = original.to_string();
        assert!(
            err.message.contains(&original_str),
            "error must reference the rejoined u128 to prove the \
             leader's id reaches the apply path intact: got {:?}",
            err.message
        );

        cleanup(&sp, &rp);
    }

    // =====================================================================
    // M3.4 determinism — pure-replay test
    // =====================================================================
    //
    // Wire two independent RocksMetaStore replicas (A and B) and feed
    // them the *same* MetaCommand sequence with the *same*
    // `assigned_now_millis`. Then read the resulting rows back and
    // assert their flexbuffer-encoded bytes are identical.
    //
    // The hypothesis under test: leader-stamped `now` survives the
    // Raft wire format end-to-end, so two replicas applying the same
    // log produce byte-identical persisted rows. Covers M3.4.a (job
    // blob), M3.4.b.2 (CreateTable), M3.4.c (UpdateHeartBeat).
    //
    // If a future refactor reintroduces a per-replica `Utc::now()`
    // call into any of these write paths, this test fires.
    use crate::metastore::job::JobType;
    use crate::metastore::ColumnType;
    use crate::metastore::RowKey;
    use crate::metastore::TableId;
    use flexbuffers::FlexbufferSerializer;
    use serde::Serialize;
    // Note: chrono::{TimeZone, Utc} are already in scope via the
    // module's `use chrono::{DateTime, TimeZone, Utc};` (rocks_apply.rs).
    // `Column`, `IdRow`, `Job`, `JobStatus`, `IndexDef` come in via
    // `use super::*;` at the top of `mod tests`.

    /// Encode `value` as flexbuffer bytes — same shape `RaftMetaStore`
    /// uses on the propose side.
    fn flex_encode<T: Serialize>(value: &T) -> Vec<u8> {
        let mut s = FlexbufferSerializer::new();
        value.serialize(&mut s).expect("flexbuffer encode");
        s.take_buffer()
    }

    /// Compare two `IdRow<T>` rows by their flexbuffer-encoded bytes.
    /// Stronger than `==` because it catches divergence in fields
    /// `PartialEq` skips (none today, but defensive against future
    /// `#[serde(skip)]` regressions).
    fn assert_rows_byte_equal<T: Serialize + Clone>(
        label: &str,
        a: &IdRow<T>,
        b: &IdRow<T>,
    ) {
        let ba = flex_encode(a);
        let bb = flex_encode(b);
        assert_eq!(
            ba, bb,
            "{}: replicas A and B diverge on persisted row bytes",
            label
        );
    }

    #[tokio::test]
    async fn pure_replay_is_byte_deterministic_across_replicas() {
        let (store_a, sa_p, ra_p) = setup_store("m3_4_determinism_a");
        let (store_b, sb_p, rb_p) = setup_store("m3_4_determinism_b");
        let apply_a = RocksMetaStoreApply::new(store_a.clone());
        let apply_b = RocksMetaStoreApply::new(store_b.clone());

        // Leader-stamped "now" — single value shipped to both replicas.
        // Picked to be far from any real wall clock so a stray
        // `Utc::now()` regression would jump out by orders of magnitude.
        let assigned_now_millis: i64 = 1_600_000_000_000;
        let leader_now = Utc.timestamp_millis_opt(assigned_now_millis).single().unwrap();

        // ---- 1. Schema ------------------------------------------------
        // (no time field — but every table needs one)
        for apply in [&apply_a, &apply_b] {
            apply
                .apply(MetaCommand::CreateSchema {
                    schema_name: "det".into(),
                    if_not_exists: false,
                })
                .await
                .expect("CreateSchema");
        }

        // ---- 2. CreateTable (M3.4.b.2: Table::created_at) -------------
        let columns = vec![Column::new("c1".into(), ColumnType::Int, 0)];
        let columns_blob = flex_encode(&columns);
        let indexes_blob = flex_encode(&Vec::<IndexDef>::new());

        for apply in [&apply_a, &apply_b] {
            apply
                .apply(MetaCommand::CreateTable {
                    schema_name: "det".into(),
                    table_name: "t".into(),
                    columns_blob: columns_blob.clone(),
                    locations: None,
                    import_format_blob: None,
                    indexes_blob: indexes_blob.clone(),
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
                    assigned_now_millis,
                })
                .await
                .expect("CreateTable");
        }

        let table_a = store_a
            .get_table("det".into(), "t".into())
            .await
            .expect("get_table A");
        let table_b = store_b
            .get_table("det".into(), "t".into())
            .await
            .expect("get_table B");
        assert_eq!(
            table_a.get_row().created_at(),
            &Some(leader_now),
            "Table::created_at must equal the leader-stamped now",
        );
        assert_rows_byte_equal("Table after CreateTable", &table_a, &table_b);

        // ---- 3. AddJob (M3.4.a: job_blob ships full row) --------------
        // Caller (scheduler) builds the Job once with its own now; we
        // simulate that by constructing the Job locally and re-encoding.
        // Both replicas receive identical bytes.
        let mut job = Job::new(
            RowKey::Table(TableId::Tables, table_a.get_id()),
            JobType::TableImport,
            "shard-0".into(),
        );
        // Stamp the leader-clock manually so the test doesn't depend on
        // the host wall clock's nanosecond precision.
        job = job.update_status_pure(
            JobStatus::Scheduled("shard-0".into()),
            leader_now,
        );
        let job_blob = flex_encode(&job);

        for apply in [&apply_a, &apply_b] {
            apply
                .apply(MetaCommand::AddJob {
                    job_blob: job_blob.clone(),
                })
                .await
                .expect("AddJob");
        }

        let jobs_a = store_a.all_jobs().await.expect("all_jobs A");
        let jobs_b = store_b.all_jobs().await.expect("all_jobs B");
        assert_eq!(jobs_a.len(), 1);
        assert_eq!(jobs_b.len(), 1);
        assert_eq!(
            jobs_a[0].get_row().last_heart_beat(),
            &leader_now,
            "Job::last_heart_beat must equal the leader-stamped now",
        );
        assert_rows_byte_equal("Job after AddJob", &jobs_a[0], &jobs_b[0]);

        // ---- 4. UpdateHeartBeat (M3.4.c: Job::last_heart_beat) --------
        let later_millis: i64 = assigned_now_millis + 5_000;
        let later_now = Utc.timestamp_millis_opt(later_millis).single().unwrap();
        let job_id = jobs_a[0].get_id();

        for apply in [&apply_a, &apply_b] {
            apply
                .apply(MetaCommand::UpdateHeartBeat {
                    job_id,
                    assigned_now_millis: later_millis,
                })
                .await
                .expect("UpdateHeartBeat");
        }

        let job_a = store_a.get_job(job_id).await.expect("get_job A");
        let job_b = store_b.get_job(job_id).await.expect("get_job B");
        assert_eq!(
            job_a.get_row().last_heart_beat(),
            &later_now,
            "UpdateHeartBeat must use the leader-stamped later_now",
        );
        assert_rows_byte_equal("Job after UpdateHeartBeat", &job_a, &job_b);

        // ---- 5. UpdateStatus (M3.4.c: Job::last_heart_beat) -----------
        let final_millis: i64 = later_millis + 7_000;
        let final_now = Utc.timestamp_millis_opt(final_millis).single().unwrap();
        let status_blob = flex_encode(&JobStatus::Completed);

        for apply in [&apply_a, &apply_b] {
            apply
                .apply(MetaCommand::UpdateStatus {
                    job_id,
                    status_blob: status_blob.clone(),
                    assigned_now_millis: final_millis,
                })
                .await
                .expect("UpdateStatus");
        }

        let job_a = store_a.get_job(job_id).await.expect("get_job A");
        let job_b = store_b.get_job(job_id).await.expect("get_job B");
        assert_eq!(
            job_a.get_row().last_heart_beat(),
            &final_now,
            "UpdateStatus must use the leader-stamped final_now",
        );
        assert_rows_byte_equal("Job after UpdateStatus", &job_a, &job_b);

        // Touch unused vars so the compiler doesn't strip them and so
        // future readers see the leader_now intent.
        let _ = (table_a, table_b);

        cleanup(&sa_p, &ra_p);
        cleanup(&sb_p, &rb_p);
    }
}
