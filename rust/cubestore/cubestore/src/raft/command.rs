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
    /// in the trait. We treat the whole call as opaque-bytes here in M1;
    /// M3 will replace this with a structured payload mirroring the trait
    /// arguments exactly. The wire stays stable because we tag the body
    /// with `payload_version`.
    CreateTable {
        schema_name: String,
        table_name: String,
        payload_version: u16,
        payload: Vec<u8>,
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
        round_trip(MetaCommand::CreateTable {
            schema_name: "public".into(),
            table_name: "orders".into(),
            payload_version: 1,
            payload: vec![1, 2, 3, 4, 5, 6, 7, 8],
        });
        // empty payload is legal (M3 might use it for default values)
        round_trip(MetaCommand::CreateTable {
            schema_name: "public".into(),
            table_name: "empty".into(),
            payload_version: 1,
            payload: vec![],
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
                    payload_version: 1,
                    payload: vec![9, 9, 9],
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

    #[test]
    fn encoded_size_is_bounded() {
        // Sanity: a 1KB payload must encode to <= 4KB (flexbuffers overhead
        // bound). If this breaks, we have a codec regression to investigate
        // before sending log entries over the wire.
        let cmd = MetaCommand::CreateTable {
            schema_name: "s".into(),
            table_name: "t".into(),
            payload_version: 1,
            payload: vec![0x42; 1024],
        };
        let bytes = cmd.encode().unwrap();
        assert!(
            bytes.len() < 4096,
            "envelope grew unexpectedly: {} bytes",
            bytes.len()
        );
    }
}
