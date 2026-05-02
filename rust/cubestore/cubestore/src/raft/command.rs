//! `MetaCommand` — the wire-format for every replicated MetaStore write.
//!
//! When `CUBESTORE_HA_MODE=raft`, every mutating call on the `MetaStore`
//! trait is encoded as a `MetaCommand`, proposed to the Raft group, and
//! applied to the local `RocksMetaStore` only after it commits. Reads stay
//! local on the leader.
//!
//! ## Why a closed enum (instead of `Vec<u8>` of arbitrary serialized args)
//!
//! Determinism is the gating property for replicated state machines. By
//! making every replicated operation an explicit variant we get:
//!
//! - exhaustive `match` in the apply path (compile-time guarantee that
//!   every write is routed through Raft, no shadow paths);
//! - a stable wire schema that can be versioned across rolling upgrades;
//! - room for Raft-leader-assigned metadata (e.g. monotonic IDs computed
//!   on the leader and applied verbatim on followers, see plan risk #1).
//!
//! ## Codec
//!
//! `flexbuffers` matches what the rest of the cubestore codebase already
//! uses for ad-hoc binary serialization (see e.g. `import` and `streaming`
//! modules). It is `serde`-compatible, schema-less, and length-prefixed,
//! which is what we want for log entries that may need partial reads.
//!
//! ## Status
//!
//! This is M1 — the **shape** of the codec, not the exhaustive enumeration
//! of all 86 write methods. Variants here are representative across
//! parameter shapes (primitives, structs, multi-vec atomics, complex
//! options). M3 will mechanically extend the enum to cover every write.
//! A `Generic { method: String, body: Vec<u8> }` escape hatch lets out-of-
//! enum operations replicate during the M1→M3 transition without breaking
//! the wire schema.

use flexbuffers::{DeserializationError, FlexbufferSerializer, Reader, ReaderError};
use serde::de::DeserializeOwned;
// `Serialize` from `serde` is the trait (used in helper bounds via fully
// qualified `serde::Serialize`); the names below are the derive macros.
use serde_derive::{Deserialize, Serialize};
use std::fmt;

/// Stable wire-version of the `MetaCommand` schema. Bump when adding/
/// renaming variants. Mismatch on apply is a fatal error.
pub const META_COMMAND_VERSION: u16 = 1;

/// Errors that can happen while encoding or decoding a `MetaCommand`.
#[derive(Debug)]
pub enum MetaCommandCodecError {
    Serialize(flexbuffers::SerializationError),
    Deserialize(DeserializationError),
    Reader(ReaderError),
    UnsupportedVersion { found: u16, expected: u16 },
    EmptyPayload,
}

impl fmt::Display for MetaCommandCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize(e) => write!(f, "MetaCommand serialize: {}", e),
            Self::Deserialize(e) => write!(f, "MetaCommand deserialize: {}", e),
            Self::Reader(e) => write!(f, "MetaCommand reader: {}", e),
            Self::UnsupportedVersion { found, expected } => write!(
                f,
                "MetaCommand version mismatch: found={} expected={}",
                found, expected
            ),
            Self::EmptyPayload => write!(f, "MetaCommand decode: empty payload"),
        }
    }
}

impl std::error::Error for MetaCommandCodecError {}

impl From<flexbuffers::SerializationError> for MetaCommandCodecError {
    fn from(e: flexbuffers::SerializationError) -> Self {
        Self::Serialize(e)
    }
}

impl From<DeserializationError> for MetaCommandCodecError {
    fn from(e: DeserializationError) -> Self {
        Self::Deserialize(e)
    }
}

impl From<ReaderError> for MetaCommandCodecError {
    fn from(e: ReaderError) -> Self {
        Self::Reader(e)
    }
}

/// A single replicable mutation against `MetaStore`. Each variant maps
/// 1-to-1 to a write method on the trait; the apply path on every router
/// replica must be deterministic for the same input.
///
/// Variants here are the M1 representative set. M3 will extend the enum
/// to cover all 86 write methods.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetaCommand {
    // ---- Schema ----------------------------------------------------------
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    RenameSchema {
        old_schema_name: String,
        new_schema_name: String,
    },
    DeleteSchema {
        schema_name: String,
    },

    // ---- Tables ----------------------------------------------------------
    /// `MetaStore::create_table` — the single most parameter-heavy method
    /// in the trait (16 args). M1 shipped it as opaque bytes; M3.3.b.3
    /// replaces that with a fully structured payload that mirrors the
    /// trait arguments. Complex types from the metastore (`Vec<Column>`,
    /// `Vec<IndexDef>`, `ImportFormat`, `StreamOffset`) ship as flex
    /// blobs because the raft module deliberately doesn't import
    /// metastore types. Times encode as `Option<i64>` ms-since-epoch.
    CreateTable {
        schema_name: String,
        table_name: String,
        /// flex-encoded `Vec<Column>`.
        columns_blob: Vec<u8>,
        locations: Option<Vec<String>>,
        /// flex-encoded `Option<ImportFormat>`. Stored as a blob even
        /// though `ImportFormat` is a small enum, to avoid leaking the
        /// metastore type into the raft wire schema.
        import_format_blob: Option<Vec<u8>>,
        /// flex-encoded `Vec<IndexDef>`.
        indexes_blob: Vec<u8>,
        is_ready: bool,
        /// `Option<DateTime<Utc>>` as ms-since-epoch.
        build_range_end_millis: Option<i64>,
        seal_at_millis: Option<i64>,
        select_statement: Option<String>,
        /// flex-encoded `Option<Vec<Column>>`.
        source_columns_blob: Option<Vec<u8>>,
        /// flex-encoded `Option<StreamOffset>`.
        stream_offset_blob: Option<Vec<u8>>,
        unique_key_column_names: Option<Vec<String>>,
        aggregates: Option<Vec<(String, String)>>,
        partition_split_threshold: Option<u64>,
        trace_obj: Option<String>,
        drop_if_exists: bool,
        extension: Option<String>,
    },
    DropTable {
        table_id: u64,
    },
    SealTable {
        table_id: u64,
    },
    UpdateLocationDownloadSize {
        table_id: u64,
        location: String,
        download_size: u64,
    },

    // ---- Partitions ------------------------------------------------------
    /// Raw partition bytes — `Partition` carries Row data which is best
    /// shipped through the existing `flexbuffers` codec rather than re-
    /// derived here.
    CreatePartition {
        partition_blob: Vec<u8>,
    },
    DeletePartition {
        partition_id: u64,
    },
    /// The compaction swap — atomically replace N old chunks with one new
    /// chunk inside a partition. M3 will validate that this maps to
    /// `MetaStore::swap_compacted_chunks` byte-for-byte.
    SwapCompactedChunks {
        partition_id: u64,
        old_chunk_ids: Vec<u64>,
        new_chunk: u64,
        new_chunk_file_size: u64,
    },
    /// The repartition swap — most complex single replicated operation.
    /// M3 will replace `payload` with structured arguments.
    SwapActivePartitions {
        payload_version: u16,
        payload: Vec<u8>,
    },

    // ---- Cat A: single-id deletes (M3.1) --------------------------------
    DeleteSchemaById {
        schema_id: u64,
    },
    MarkPartitionWarmedUp {
        partition_id: u64,
    },
    DeleteMiddleManPartition {
        partition_id: u64,
    },
    DeleteChunk {
        chunk_id: u64,
    },
    DeleteChunksWithoutChecks {
        chunk_ids: Vec<u64>,
    },
    DeleteWal {
        wal_id: u64,
    },
    DeleteJob {
        job_id: u64,
    },
    DeleteSource {
        id: u64,
    },

    // ---- Cat B: two-arg primitives (M3.1) -------------------------------
    RenameSchemaById {
        schema_id: u64,
        new_schema_name: String,
    },
    UpdateHeartBeat {
        job_id: u64,
    },
    SetCurrentSnapshot {
        // u128 — flexbuffers doesn't have native u128, so encode as
        // [u64; 2] (low, high). Keeps round-trip exact.
        snapshot_id_low: u64,
        snapshot_id_high: u64,
    },
    AcquirePartitionedLock {
        // arguments TBD when we type-fully this in M3.3
        payload_version: u16,
        payload: Vec<u8>,
    },
    ReleasePartitionedLock {
        payload_version: u16,
        payload: Vec<u8>,
    },

    // ---- Cat C: create-with-struct (M3.3.b.1) ---------------------------
    // Each of these takes a metastore row struct (Chunk, MultiPartition,
    // SourceCredentials, …) plus context. The raft module deliberately
    // doesn't import metastore types — would create a cycle — so the
    // struct is shipped as a `flexbuffers`-encoded blob in `*_blob`. The
    // dispatch impl in `rocks_apply.rs` decodes it back into the typed
    // shape before calling the trait method. Field documents the
    // expected Rust type so the dispatch arm can be checked at review.
    CreateChunk {
        partition_id: u64,
        /// Use `u64` not `usize` — flexbuffers + cross-platform stable.
        row_count: u64,
        in_memory: bool,
        /// flex-encoded `Option<Row>` (`Row` from `crate::table`).
        min_blob: Option<Vec<u8>>,
        /// flex-encoded `Option<Row>`.
        max_blob: Option<Vec<u8>>,
    },
    CreateWal {
        table_id: u64,
        /// `usize` in the trait — converted to `u64` here.
        row_count: u64,
    },
    CreateIndex {
        schema_name: String,
        table_name: String,
        /// flex-encoded `IndexDef` (from `crate::metastore`).
        index_def_blob: Vec<u8>,
    },
    CreatePartitionedIndex {
        schema: String,
        name: String,
        /// flex-encoded `Vec<Column>` (from `crate::metastore`).
        columns_blob: Vec<u8>,
        if_not_exists: bool,
    },
    CreateMultiPartition {
        /// flex-encoded `MultiPartition`.
        multi_partition_blob: Vec<u8>,
    },
    CreateOrUpdateSource {
        name: String,
        /// flex-encoded `SourceCredentials`.
        credentials_blob: Vec<u8>,
    },
    CreateReplayHandle {
        table_id: u64,
        /// `usize` in trait — `u64` here.
        location_index: u64,
        /// flex-encoded `SeqPointer`.
        seq_pointer_blob: Vec<u8>,
    },
    CreateReplayHandleFromSeqPointers {
        table_id: u64,
        /// flex-encoded `Option<Vec<Option<SeqPointer>>>`.
        seq_pointers_blob: Vec<u8>,
    },

    // ---- Cat A: simple uploads/seals (M3.3.b.2) -------------------------
    /// `chunk_uploaded(chunk_id) -> IdRow<Chunk>` — flips the upload flag.
    ChunkUploaded {
        chunk_id: u64,
    },
    /// `wal_uploaded(wal_id) -> IdRow<WAL>` — same shape for WALs.
    WalUploaded {
        wal_id: u64,
    },

    // ---- Cat B: simple two-arg setters (M3.3.b.2) -----------------------
    /// `table_ready(id, is_ready) -> IdRow<Table>`.
    TableReady {
        table_id: u64,
        is_ready: bool,
    },
    /// `drop_partitioned_index(schema, name) -> ()`.
    DropPartitionedIndex {
        schema: String,
        name: String,
    },
    /// `update_replay_handle_failed_if_exists(id, failed) -> ()`.
    UpdateReplayHandleFailedIfExists {
        id: u64,
        failed: bool,
    },

    // ---- Cat F: job lifecycle (M3.3.b.2) --------------------------------
    /// `add_job(Job) -> Option<IdRow<Job>>` — returns `None` if a job
    /// for the same row reference + type is already queued.
    AddJob {
        /// flex-encoded `Job` (from `crate::metastore::job::Job`).
        /// Includes `last_heart_beat: DateTime<Utc>` which is stamped on
        /// the leader before encode and replicated verbatim — that's
        /// the determinism boundary for this variant.
        job_blob: Vec<u8>,
    },
    StartProcessingJob {
        server_name: String,
        long_term: bool,
    },
    UpdateStatus {
        job_id: u64,
        /// flex-encoded `JobStatus` (enum).
        status_blob: Vec<u8>,
    },

    // ---- Cat E: atomic chunk swap ops (M3.3.b.2) ------------------------
    // All return `()` — they're fire-and-forget atomic ops on the
    // underlying RocksDB WriteBatch.
    SwapChunks {
        deactivate_ids: Vec<u64>,
        /// `Vec<(chunk_id, Option<file_size>)>` — file_size is `None`
        /// for in-memory chunks where it's not yet known.
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    },
    SwapChunksWithoutCheck {
        deactivate_ids: Vec<u64>,
        uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        new_replay_handle_id: Option<u64>,
    },
    DeactivateChunksWithoutCheck {
        deactivate_ids: Vec<u64>,
    },
    ActivateChunks {
        table_id: u64,
        uploaded_chunk_ids: Vec<(u64, Option<u64>)>,
        replay_handle_id: Option<u64>,
    },

    // ---- Cat F: replay handle merge (M3.3.b.2) --------------------------
    /// `replace_replay_handles(old_ids, new_seq_pointer) -> Option<IdRow<ReplayHandle>>`.
    ReplaceReplayHandles {
        old_ids: Vec<u64>,
        /// flex-encoded `Option<Vec<Option<SeqPointer>>>` —
        /// kept as a blob even though `Option<...>` could nest in the
        /// variant directly, because `SeqPointer` lives in the metastore
        /// and the raft module doesn't import metastore types.
        new_seq_pointer_blob: Vec<u8>,
    },

    // ---- Atomic batch ----------------------------------------------------
    /// Multi-statement DDL — `BatchPipe`. Applied as a single RocksDB
    /// `WriteBatch` on the apply path so the whole sequence either
    /// commits or rolls back together. See plan risk #2.
    Batch {
        commands: Vec<MetaCommand>,
    },

    // ---- Escape hatch ----------------------------------------------------
    /// An unmodelled write — used during M1→M3 to keep the wire stable
    /// while the enum is being filled in. `method` is the trait method
    /// name; `body` is its `flexbuffers`-serialized argument tuple.
    /// Apply path returns an error for any `Generic` whose method has
    /// since been promoted to a typed variant — this catches stale peers
    /// during a rolling upgrade.
    Generic {
        method: String,
        body: Vec<u8>,
    },
}

/// A versioned envelope around a `MetaCommand`. This is what actually
/// goes on the wire (and into the Raft log).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetaCommandEnvelope {
    pub version: u16,
    pub command: MetaCommand,
}

/// Tag for the row payload kind carried by `MetaCommandResult::IdRow` /
/// `MetaCommandResult::OptionalIdRow`. The raft module deliberately does
/// not import metastore row types directly (would create a cycle —
/// metastore depends on the raft module for replication, and the raft
/// module would then transitively depend on metastore). Instead we ship
/// the row as a flexbuffer-encoded blob plus this tag; the wrapper-style
/// `RaftMetaStore: MetaStore` impl decodes it into the right `IdRow<T>`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IdRowKind {
    Schema,
    Table,
    Partition,
    Chunk,
    Wal,
    Job,
    Source,
    ReplayHandle,
    MultiPartition,
    MultiIndex,
    Index,
}

/// Typed return shape of an applied `MetaCommand`. Mirrors the
/// `Result<...>` shapes used across the `MetaStore` trait.
///
/// The matrix of trait return types is small and well-defined:
///
/// - `()` → `Unit`
/// - `bool` → `Bool` (only `swap_compacted_chunks` returns this)
/// - `IdRow<T>` → `IdRow { kind, payload }`
/// - `Option<IdRow<T>>` → `OptionalIdRow { kind, payload }`
///
/// The wrapper impl on `RaftMetaStore` reads the matching variant
/// after `propose(...).await?` and decodes the payload via flexbuffers.
/// Variant mismatch is a **bug** — it indicates a write method is
/// returning a result shape that doesn't match its declared trait
/// return — and is reported as a `CubeError::internal`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetaCommandResult {
    Unit,
    Bool(bool),
    IdRow {
        kind: IdRowKind,
        /// flexbuffer-encoded `IdRow<T>` matching `kind`.
        payload: Vec<u8>,
    },
    OptionalIdRow {
        kind: IdRowKind,
        /// `None` is encoded as `None` here directly so callers can
        /// distinguish "the operation succeeded but produced no row"
        /// (e.g. `add_job` returning `None` when a duplicate exists)
        /// from "the call produced an unrelated variant".
        payload: Option<Vec<u8>>,
    },
}

impl MetaCommandResult {
    /// Build an `IdRow` result from any serializable row.
    pub fn id_row<T: serde::Serialize>(
        kind: IdRowKind,
        row: &T,
    ) -> Result<Self, MetaCommandCodecError> {
        Ok(Self::IdRow {
            kind,
            payload: encode_flex(row)?,
        })
    }

    /// Build an `OptionalIdRow` result.
    pub fn optional_id_row<T: serde::Serialize>(
        kind: IdRowKind,
        row: Option<&T>,
    ) -> Result<Self, MetaCommandCodecError> {
        Ok(Self::OptionalIdRow {
            kind,
            payload: match row {
                Some(r) => Some(encode_flex(r)?),
                None => None,
            },
        })
    }

    /// Decode the payload as a typed `IdRow<T>`. Errors if the result
    /// is not an `IdRow` of the expected `kind`.
    pub fn into_id_row<T: DeserializeOwned>(
        self,
        expected: IdRowKind,
    ) -> Result<T, MetaCommandResultMismatch> {
        match self {
            Self::IdRow { kind, payload } if kind == expected => {
                decode_flex::<T>(&payload).map_err(MetaCommandResultMismatch::Codec)
            }
            other => Err(MetaCommandResultMismatch::Variant {
                expected: format!("IdRow({:?})", expected),
                got: other.variant_name().to_string(),
            }),
        }
    }

    /// Decode the payload as a typed `Option<IdRow<T>>`. Errors if the
    /// result is not an `OptionalIdRow` of the expected `kind`.
    pub fn into_optional_id_row<T: DeserializeOwned>(
        self,
        expected: IdRowKind,
    ) -> Result<Option<T>, MetaCommandResultMismatch> {
        match self {
            Self::OptionalIdRow { kind, payload } if kind == expected => match payload {
                Some(bytes) => decode_flex::<T>(&bytes)
                    .map(Some)
                    .map_err(MetaCommandResultMismatch::Codec),
                None => Ok(None),
            },
            other => Err(MetaCommandResultMismatch::Variant {
                expected: format!("OptionalIdRow({:?})", expected),
                got: other.variant_name().to_string(),
            }),
        }
    }

    /// Assert the result is `Unit`. Errors otherwise.
    pub fn into_unit(self) -> Result<(), MetaCommandResultMismatch> {
        match self {
            Self::Unit => Ok(()),
            other => Err(MetaCommandResultMismatch::Variant {
                expected: "Unit".into(),
                got: other.variant_name().to_string(),
            }),
        }
    }

    /// Assert the result is `Bool` and unwrap it. Errors otherwise.
    pub fn into_bool(self) -> Result<bool, MetaCommandResultMismatch> {
        match self {
            Self::Bool(b) => Ok(b),
            other => Err(MetaCommandResultMismatch::Variant {
                expected: "Bool".into(),
                got: other.variant_name().to_string(),
            }),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Unit => "Unit",
            Self::Bool(_) => "Bool",
            Self::IdRow { .. } => "IdRow",
            Self::OptionalIdRow { .. } => "OptionalIdRow",
        }
    }
}

/// Returned when a caller asks `MetaCommandResult` for a shape that
/// doesn't match what the apply path produced. In practice this only
/// fires when a write method's apply impl returns the wrong variant —
/// i.e. it is a bug in `RocksMetaStoreApply`, not a runtime data
/// condition.
#[derive(Debug)]
pub enum MetaCommandResultMismatch {
    Variant { expected: String, got: String },
    Codec(MetaCommandCodecError),
}

impl fmt::Display for MetaCommandResultMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variant { expected, got } => write!(
                f,
                "MetaCommandResult variant mismatch: expected={} got={}",
                expected, got
            ),
            Self::Codec(e) => write!(f, "MetaCommandResult payload decode: {}", e),
        }
    }
}

impl std::error::Error for MetaCommandResultMismatch {}

impl MetaCommand {
    pub fn encode(&self) -> Result<Vec<u8>, MetaCommandCodecError> {
        let envelope = MetaCommandEnvelope {
            version: META_COMMAND_VERSION,
            command: self.clone(),
        };
        encode_flex(&envelope)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaCommandCodecError> {
        if bytes.is_empty() {
            return Err(MetaCommandCodecError::EmptyPayload);
        }
        let envelope: MetaCommandEnvelope = decode_flex(bytes)?;
        if envelope.version != META_COMMAND_VERSION {
            return Err(MetaCommandCodecError::UnsupportedVersion {
                found: envelope.version,
                expected: META_COMMAND_VERSION,
            });
        }
        Ok(envelope.command)
    }
}

fn encode_flex<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, MetaCommandCodecError> {
    let mut s = FlexbufferSerializer::new();
    value.serialize(&mut s)?;
    Ok(s.take_buffer())
}

fn decode_flex<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, MetaCommandCodecError> {
    let r = Reader::get_root(bytes)?;
    Ok(T::deserialize(r)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(cmd: MetaCommand) {
        let bytes = cmd.encode().expect("encode");
        let decoded = MetaCommand::decode(&bytes).expect("decode");
        assert_eq!(cmd, decoded, "round-trip mismatch");
    }

    #[test]
    fn create_schema_round_trip() {
        round_trip(MetaCommand::CreateSchema {
            schema_name: "public".into(),
            if_not_exists: true,
        });
        round_trip(MetaCommand::CreateSchema {
            schema_name: String::new(),
            if_not_exists: false,
        });
    }

    #[test]
    fn rename_schema_round_trip() {
        round_trip(MetaCommand::RenameSchema {
            old_schema_name: "old".into(),
            new_schema_name: "new".into(),
        });
    }

    #[test]
    fn delete_schema_round_trip() {
        round_trip(MetaCommand::DeleteSchema {
            schema_name: "to_drop".into(),
        });
    }

    #[test]
    fn create_table_round_trip() {
        // Maximum-arg form: every Option set to Some, every Vec
        // populated. Catches any field mis-ordering during encode.
        round_trip(MetaCommand::CreateTable {
            schema_name: "public".into(),
            table_name: "orders".into(),
            columns_blob: vec![0xC0; 64],
            locations: Some(vec!["s3://a/x.csv".into(), "s3://a/y.csv".into()]),
            import_format_blob: Some(vec![0xF1; 8]),
            indexes_blob: vec![0xF2; 32],
            is_ready: true,
            build_range_end_millis: Some(1_700_000_000_000),
            seal_at_millis: Some(1_800_000_000_000),
            select_statement: Some("SELECT * FROM raw".into()),
            source_columns_blob: Some(vec![0xF3; 16]),
            stream_offset_blob: Some(vec![0xF4; 4]),
            unique_key_column_names: Some(vec!["id".into(), "ts".into()]),
            aggregates: Some(vec![("count".into(), "*".into()), ("sum".into(), "amt".into())]),
            partition_split_threshold: Some(1 << 30),
            trace_obj: Some("trace-blob".into()),
            drop_if_exists: false,
            extension: Some("parquet".into()),
        });
        // Minimum-arg form: every Option None, every Vec empty.
        round_trip(MetaCommand::CreateTable {
            schema_name: String::new(),
            table_name: String::new(),
            columns_blob: vec![],
            locations: None,
            import_format_blob: None,
            indexes_blob: vec![],
            is_ready: false,
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
        });
    }

    #[test]
    fn drop_and_seal_table_round_trip() {
        round_trip(MetaCommand::DropTable { table_id: 42 });
        round_trip(MetaCommand::SealTable { table_id: u64::MAX });
    }

    #[test]
    fn update_location_download_size_round_trip() {
        round_trip(MetaCommand::UpdateLocationDownloadSize {
            table_id: 1,
            location: "s3://bucket/key".into(),
            download_size: 12345,
        });
    }

    #[test]
    fn swap_compacted_chunks_round_trip() {
        round_trip(MetaCommand::SwapCompactedChunks {
            partition_id: 7,
            old_chunk_ids: vec![1, 2, 3, 99],
            new_chunk: 100,
            new_chunk_file_size: 1 << 30,
        });
        // empty old_chunk_ids must round-trip too (compaction edge case)
        round_trip(MetaCommand::SwapCompactedChunks {
            partition_id: 0,
            old_chunk_ids: vec![],
            new_chunk: 0,
            new_chunk_file_size: 0,
        });
    }

    #[test]
    fn create_partition_round_trip() {
        round_trip(MetaCommand::CreatePartition {
            partition_blob: vec![0xAA; 256],
        });
    }

    #[test]
    fn swap_active_partitions_round_trip() {
        round_trip(MetaCommand::SwapActivePartitions {
            payload_version: 1,
            payload: vec![0xBB; 1024],
        });
    }

    #[test]
    fn batch_round_trip_atomicity() {
        // The whole point of Batch — multi-statement DDL ships as one entry.
        round_trip(MetaCommand::Batch {
            commands: vec![
                MetaCommand::CreateSchema {
                    schema_name: "s1".into(),
                    if_not_exists: false,
                },
                MetaCommand::CreateTable {
                    schema_name: "s1".into(),
                    table_name: "t1".into(),
                    columns_blob: vec![9, 9, 9],
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
                },
                MetaCommand::DropTable { table_id: 1 },
            ],
        });
        // Empty batch must encode (no-op) and round-trip — M3 will reject
        // it at apply time, but the codec is permissive.
        round_trip(MetaCommand::Batch { commands: vec![] });
    }

    #[test]
    fn nested_batch_round_trip() {
        // Defensive: even if a Batch contains a Batch (it shouldn't in
        // practice; apply path will reject), the codec round-trips it.
        round_trip(MetaCommand::Batch {
            commands: vec![MetaCommand::Batch {
                commands: vec![MetaCommand::DropTable { table_id: 1 }],
            }],
        });
    }

    #[test]
    fn cat_a_single_id_deletes_round_trip() {
        round_trip(MetaCommand::DeleteSchemaById { schema_id: 1 });
        round_trip(MetaCommand::MarkPartitionWarmedUp { partition_id: 0 });
        round_trip(MetaCommand::DeleteMiddleManPartition {
            partition_id: u64::MAX,
        });
        round_trip(MetaCommand::DeleteChunk { chunk_id: 7 });
        round_trip(MetaCommand::DeleteChunksWithoutChecks {
            chunk_ids: vec![1, 2, 3, 4, 5],
        });
        round_trip(MetaCommand::DeleteChunksWithoutChecks { chunk_ids: vec![] });
        round_trip(MetaCommand::DeleteWal { wal_id: 42 });
        round_trip(MetaCommand::DeleteJob { job_id: 100 });
        round_trip(MetaCommand::DeleteSource { id: 5 });
    }

    #[test]
    fn cat_b_two_arg_primitives_round_trip() {
        round_trip(MetaCommand::RenameSchemaById {
            schema_id: 1,
            new_schema_name: "renamed".into(),
        });
        round_trip(MetaCommand::UpdateHeartBeat { job_id: 7 });
        round_trip(MetaCommand::SetCurrentSnapshot {
            snapshot_id_low: 0xDEAD_BEEF_CAFE_BABE,
            snapshot_id_high: 0x1234_5678_9ABC_DEF0,
        });
        round_trip(MetaCommand::AcquirePartitionedLock {
            payload_version: 1,
            payload: vec![0xAA; 32],
        });
        round_trip(MetaCommand::ReleasePartitionedLock {
            payload_version: 1,
            payload: vec![],
        });
    }

    #[test]
    fn cat_a_uploads_round_trip() {
        round_trip(MetaCommand::ChunkUploaded { chunk_id: 1 });
        round_trip(MetaCommand::ChunkUploaded { chunk_id: u64::MAX });
        round_trip(MetaCommand::WalUploaded { wal_id: 0 });
    }

    #[test]
    fn cat_b_setters_round_trip() {
        round_trip(MetaCommand::TableReady {
            table_id: 1,
            is_ready: true,
        });
        round_trip(MetaCommand::TableReady {
            table_id: 0,
            is_ready: false,
        });
        round_trip(MetaCommand::DropPartitionedIndex {
            schema: "public".into(),
            name: "by_country".into(),
        });
        round_trip(MetaCommand::DropPartitionedIndex {
            schema: String::new(),
            name: String::new(),
        });
        round_trip(MetaCommand::UpdateReplayHandleFailedIfExists {
            id: 7,
            failed: true,
        });
    }

    #[test]
    fn cat_f_jobs_round_trip() {
        round_trip(MetaCommand::AddJob {
            job_blob: vec![0xAA; 128],
        });
        round_trip(MetaCommand::StartProcessingJob {
            server_name: "node1".into(),
            long_term: true,
        });
        round_trip(MetaCommand::StartProcessingJob {
            server_name: String::new(),
            long_term: false,
        });
        round_trip(MetaCommand::UpdateStatus {
            job_id: 42,
            status_blob: vec![0xBB; 64],
        });
    }

    #[test]
    fn cat_e_chunk_swaps_round_trip() {
        round_trip(MetaCommand::SwapChunks {
            deactivate_ids: vec![1, 2, 3],
            uploaded_ids_and_sizes: vec![(10, Some(1024)), (11, None), (12, Some(0))],
            new_replay_handle_id: Some(99),
        });
        round_trip(MetaCommand::SwapChunks {
            deactivate_ids: vec![],
            uploaded_ids_and_sizes: vec![],
            new_replay_handle_id: None,
        });

        round_trip(MetaCommand::SwapChunksWithoutCheck {
            deactivate_ids: vec![5, 6],
            uploaded_ids_and_sizes: vec![(7, Some(2048))],
            new_replay_handle_id: None,
        });

        round_trip(MetaCommand::DeactivateChunksWithoutCheck {
            deactivate_ids: vec![1, 2, 3, 4, 5],
        });
        round_trip(MetaCommand::DeactivateChunksWithoutCheck {
            deactivate_ids: vec![],
        });

        round_trip(MetaCommand::ActivateChunks {
            table_id: 1,
            uploaded_chunk_ids: vec![(100, Some(512)), (101, None)],
            replay_handle_id: Some(7),
        });
        round_trip(MetaCommand::ActivateChunks {
            table_id: u64::MAX,
            uploaded_chunk_ids: vec![],
            replay_handle_id: None,
        });
    }

    #[test]
    fn cat_f_replace_replay_handles_round_trip() {
        round_trip(MetaCommand::ReplaceReplayHandles {
            old_ids: vec![1, 2, 3],
            new_seq_pointer_blob: vec![0xCC; 96],
        });
        // empty old_ids + empty blob — degenerate but legal.
        round_trip(MetaCommand::ReplaceReplayHandles {
            old_ids: vec![],
            new_seq_pointer_blob: vec![],
        });
    }

    #[test]
    fn cat_c_create_with_struct_round_trip() {
        // Cat C variants — each carries a struct as a flex blob plus
        // primitive context. The codec doesn't decode the blob; we
        // just verify the byte-for-byte round-trip.
        round_trip(MetaCommand::CreateChunk {
            partition_id: 1,
            row_count: 12345,
            in_memory: true,
            min_blob: Some(vec![0x01; 16]),
            max_blob: None,
        });
        round_trip(MetaCommand::CreateChunk {
            partition_id: u64::MAX,
            row_count: 0,
            in_memory: false,
            min_blob: None,
            max_blob: Some(vec![]),
        });

        round_trip(MetaCommand::CreateWal {
            table_id: 7,
            row_count: 1024,
        });

        round_trip(MetaCommand::CreateIndex {
            schema_name: "public".into(),
            table_name: "orders".into(),
            index_def_blob: vec![0xCA; 64],
        });

        round_trip(MetaCommand::CreatePartitionedIndex {
            schema: "public".into(),
            name: "by_country".into(),
            columns_blob: vec![0xFE; 32],
            if_not_exists: true,
        });
        round_trip(MetaCommand::CreatePartitionedIndex {
            schema: String::new(),
            name: String::new(),
            columns_blob: vec![],
            if_not_exists: false,
        });

        round_trip(MetaCommand::CreateMultiPartition {
            multi_partition_blob: vec![0xAA; 256],
        });

        round_trip(MetaCommand::CreateOrUpdateSource {
            name: "kafka1".into(),
            credentials_blob: vec![0xCC; 128],
        });

        round_trip(MetaCommand::CreateReplayHandle {
            table_id: 42,
            location_index: 0,
            seq_pointer_blob: vec![0xBB; 32],
        });
        round_trip(MetaCommand::CreateReplayHandle {
            table_id: u64::MAX,
            location_index: u64::MAX,
            seq_pointer_blob: vec![],
        });

        round_trip(MetaCommand::CreateReplayHandleFromSeqPointers {
            table_id: 99,
            seq_pointers_blob: vec![0xDD; 192],
        });
    }

    #[test]
    fn generic_round_trip() {
        round_trip(MetaCommand::Generic {
            method: "create_partitioned_index".into(),
            body: b"opaque flexbuffer payload here".to_vec(),
        });
    }

    #[test]
    fn empty_payload_decode_errors() {
        let err = MetaCommand::decode(&[]).unwrap_err();
        matches!(err, MetaCommandCodecError::EmptyPayload);
    }

    #[test]
    fn version_mismatch_decode_errors() {
        // Hand-craft an envelope with a wrong version, encode, then try to
        // decode — must fail with UnsupportedVersion.
        let envelope = MetaCommandEnvelope {
            version: META_COMMAND_VERSION + 99,
            command: MetaCommand::DropTable { table_id: 1 },
        };
        let bytes = encode_flex(&envelope).unwrap();
        let err = MetaCommand::decode(&bytes).unwrap_err();
        match err {
            MetaCommandCodecError::UnsupportedVersion { found, expected } => {
                assert_eq!(found, META_COMMAND_VERSION + 99);
                assert_eq!(expected, META_COMMAND_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {:?}", other),
        }
    }

    #[test]
    fn corrupt_bytes_decode_errors() {
        let err = MetaCommand::decode(&[0xFF, 0xFE, 0xFD]).unwrap_err();
        // Either Reader or Deserialize — both are acceptable.
        assert!(matches!(
            err,
            MetaCommandCodecError::Reader(_) | MetaCommandCodecError::Deserialize(_)
        ));
    }

    // -------------------------------------------------------------------
    // M3.2 — MetaCommandResult round-trip + helper coverage
    // -------------------------------------------------------------------

    fn result_round_trip(r: MetaCommandResult) {
        // Encoded-as-a-flexbuffer round-trip — same codec the apply path
        // uses to send the result back through the oneshot in M3.3+.
        let bytes = encode_flex(&r).expect("encode result");
        let decoded: MetaCommandResult = decode_flex(&bytes).expect("decode result");
        assert_eq!(r, decoded, "MetaCommandResult round-trip mismatch");
    }

    #[test]
    fn meta_command_result_unit_round_trip() {
        result_round_trip(MetaCommandResult::Unit);
    }

    #[test]
    fn meta_command_result_bool_round_trip() {
        result_round_trip(MetaCommandResult::Bool(true));
        result_round_trip(MetaCommandResult::Bool(false));
    }

    #[test]
    fn meta_command_result_id_row_round_trip() {
        // Use a tuple proxy: the wire layer is type-agnostic — it just
        // ships flexbuffer bytes — so any serializable shape exercises
        // the round-trip.
        let row = (42u64, "schema_name".to_string(), true);
        let r = MetaCommandResult::id_row(IdRowKind::Schema, &row).expect("build");
        result_round_trip(r.clone());

        let decoded: (u64, String, bool) = r.into_id_row(IdRowKind::Schema).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn meta_command_result_optional_id_row_round_trip() {
        // Some(...) and None both must round-trip.
        let row = (7u64, "row".to_string());
        let some = MetaCommandResult::optional_id_row(IdRowKind::Job, Some(&row)).expect("build");
        result_round_trip(some.clone());

        let none = MetaCommandResult::optional_id_row::<(u64, String)>(IdRowKind::Job, None)
            .expect("build none");
        result_round_trip(none.clone());

        let decoded_some: Option<(u64, String)> =
            some.into_optional_id_row(IdRowKind::Job).expect("decode");
        assert_eq!(decoded_some, Some(row));

        let decoded_none: Option<(u64, String)> =
            none.into_optional_id_row(IdRowKind::Job).expect("decode");
        assert_eq!(decoded_none, None);
    }

    #[test]
    fn meta_command_result_kind_mismatch_errors() {
        // Build an IdRow tagged Schema, ask for it as Table — must error.
        let row = (1u64, "name".to_string());
        let r = MetaCommandResult::id_row(IdRowKind::Schema, &row).expect("build");
        let err = r.into_id_row::<(u64, String)>(IdRowKind::Table).unwrap_err();
        match err {
            MetaCommandResultMismatch::Variant { expected, got } => {
                assert!(expected.contains("Table"), "expected msg: {}", expected);
                assert!(got.contains("IdRow"), "got msg: {}", got);
            }
            other => panic!("expected Variant mismatch, got {:?}", other),
        }
    }

    #[test]
    fn meta_command_result_variant_mismatch_errors() {
        // `into_unit` on a `Bool` must error.
        let r = MetaCommandResult::Bool(true);
        let err = r.into_unit().unwrap_err();
        match err {
            MetaCommandResultMismatch::Variant { expected, got } => {
                assert_eq!(expected, "Unit");
                assert_eq!(got, "Bool");
            }
            other => panic!("expected Variant mismatch, got {:?}", other),
        }

        // `into_bool` on a `Unit` must error.
        let r = MetaCommandResult::Unit;
        let err = r.into_bool().unwrap_err();
        match err {
            MetaCommandResultMismatch::Variant { expected, got } => {
                assert_eq!(expected, "Bool");
                assert_eq!(got, "Unit");
            }
            other => panic!("expected Variant mismatch, got {:?}", other),
        }
    }

    #[test]
    fn id_row_kind_round_trip_all_variants() {
        // Sanity: every IdRowKind variant must encode + decode exactly.
        // If a future variant is added but not exercised here, the test
        // is a forcing function to keep coverage current.
        let all = [
            IdRowKind::Schema,
            IdRowKind::Table,
            IdRowKind::Partition,
            IdRowKind::Chunk,
            IdRowKind::Wal,
            IdRowKind::Job,
            IdRowKind::Source,
            IdRowKind::ReplayHandle,
            IdRowKind::MultiPartition,
            IdRowKind::MultiIndex,
            IdRowKind::Index,
        ];
        for kind in all {
            let bytes = encode_flex(&kind).unwrap();
            let decoded: IdRowKind = decode_flex(&bytes).unwrap();
            assert_eq!(decoded, kind);
        }
    }

    #[test]
    fn encoded_size_is_bounded() {
        // Sanity: a 1KB payload must encode to <= 4KB (flexbuffers overhead
        // bound). If this breaks, we have a codec regression to investigate
        // before sending log entries over the wire.
        let cmd = MetaCommand::CreateTable {
            schema_name: "s".into(),
            table_name: "t".into(),
            columns_blob: vec![0x42; 1024],
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
        };
        let bytes = cmd.encode().unwrap();
        assert!(
            bytes.len() < 4096,
            "envelope grew unexpectedly: {} bytes",
            bytes.len()
        );
    }
}
