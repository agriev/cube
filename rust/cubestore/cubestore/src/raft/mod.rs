//! HA fork — Raft-based metadata replication for Cube Store.
//!
//! See `docs/ha/README.md` (or the fork-level `HA.md`) for the full design.
//! This module is *off* unless `CUBESTORE_HA_MODE=raft` and is structured so
//! the rest of the codebase keeps calling the [`crate::metastore::MetaStore`]
//! trait without knowing replication exists.
//!
//! Layout:
//! - [`command`] — `MetaCommand` enum: every replicable write of the
//!   `MetaStore` trait, serializable via the chart's existing
//!   `flexbuffers` / `serde_bytes` codec.
//! - [`state_machine`] — `RaftMetaStore`, the trait impl that wraps
//!   `RocksMetaStore` and routes writes through Raft.
//! - [`storage`] — `RaftStorage`, the `raft::Storage` impl backed by a
//!   dedicated RocksDB instance under `<data_dir>/raft-log/`.
//! - [`transport`] — wire-format and `cuberpc`-based message shipping
//!   between Raft peers.
//!
//! Milestone status (M1: scaffolding only):
//! - [x] `MetaCommand` enum with representative variants and round-trip tests
//! - [ ] All 86 `MetaStore` write methods enumerated (M3)
//! - [ ] Single-node Raft state machine (M2)
//! - [ ] 3-node clustering / leader election (M4)

pub mod command;
pub mod state_machine;
pub mod storage;
pub mod transport;

pub use command::{MetaCommand, MetaCommandCodecError};
