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
//! ## Determinism (read this before flipping `CUBESTORE_HA_MODE=raft`)
//!
//! Several existing `MetaStore` write methods read non-deterministic
//! state (`Utc::now`, `next_id` derived from RocksDB merge counters).
//! When this dispatch runs on multiple replicas, the replicas will
//! diverge unless those reads happen on the leader and ship as part
//! of the `MetaCommand` payload. M3.4 is the gating commit that adds
//! `assigned_id: Option<u64>` / `assigned_now: Option<i64>` to the
//! relevant variants and changes the trait methods to consume them.
//! **Do not flip the HA boot path on (M3.5) until M3.4 is in.**
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
    Chunk, Column, ImportFormat, IndexDef, MetaStore, Partition, RocksMetaStore,
};
use chrono::{DateTime, TimeZone, Utc};
use crate::raft::command::{IdRowKind, MetaCommand, MetaCommandResult};
use crate::raft::state_machine::Apply;
use crate::table::Row;
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

            // ---- Still deferred -------------------------------------------
            MetaCommand::AcquirePartitionedLock { .. } => Err(not_yet_implemented(
                "AcquirePartitionedLock",
                "cluster-side lock op, no MetaStore equivalent yet",
            )),
            MetaCommand::ReleasePartitionedLock { .. } => Err(not_yet_implemented(
                "ReleasePartitionedLock",
                "cluster-side lock op, no MetaStore equivalent yet",
            )),

            // ---- Batch -----------------------------------------------------
            // M3.3.c will route this through one `write_operation` so the
            // sequence applies as a single RocksDB WriteBatch. Until then,
            // accept-and-degrade-to-non-atomic would be a determinism bug,
            // so reject.
            MetaCommand::Batch { .. } => Err(not_yet_implemented(
                "Batch",
                "M3.3.c — needs single write_operation with sub-dispatch",
            )),

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

        // Batch is M3.3.c — must error.
        let err = apply
            .apply(MetaCommand::Batch {
                commands: vec![MetaCommand::DeleteWal { wal_id: 1 }],
            })
            .await
            .expect_err("Batch must error in M3.3.a");
        assert!(err.message.contains("Batch"));

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
    async fn snapshot_id_round_trips_through_split_u128() {
        let test_name = "raft_apply_snapshot_id";
        let (store, sp, rp) = setup_store(test_name);
        let apply = RocksMetaStoreApply::new(store.clone());

        // Pick a u128 with bits set in both halves so we can prove the
        // (low, high) split + rejoin doesn't mangle anything.
        let original: u128 =
            ((0x1234_5678_9ABC_DEF0u128) << 64) | 0xDEAD_BEEF_CAFE_BABEu128;
        let snapshot_id_low = original as u64;
        let snapshot_id_high = (original >> 64) as u64;

        let r = apply
            .apply(MetaCommand::SetCurrentSnapshot {
                snapshot_id_low,
                snapshot_id_high,
            })
            .await
            .expect("apply SetCurrentSnapshot");
        r.into_unit().expect("must be Unit");

        cleanup(&sp, &rp);
    }
}
