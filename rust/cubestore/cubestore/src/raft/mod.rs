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
//! - [`rocks_apply`] — `RocksMetaStoreApply`, the `Apply` impl that
//!   dispatches each `MetaCommand` to the matching `RocksMetaStore`
//!   write method. This is the production wiring; tests use lighter
//!   in-memory `Apply` impls for round-trip coverage.
//! - [`transport`] — wire-format and `cuberpc`-based message shipping
//!   between Raft peers.
//!
//! Milestone status:
//! - [x] M1 — `MetaCommand` enum with representative variants
//! - [x] M2 — single-node Raft state machine + persistent storage
//! - [x] M3.1 — Cat A/B variants (~28 of 86 write methods)
//! - [x] M3.2 — `MetaCommandResult` typed returns
//! - [ ] M3.3 — `RocksMetaStoreApply` dispatch (in progress)
//! - [ ] M3.4 — Determinism fix: leader-assigned IDs / timestamps
//! - [ ] M3.5 — Config wiring: `CUBESTORE_HA_MODE` boot path
//! - [ ] M3.6 — `cubestore-sql-tests` passing under HA mode
//! - [ ] M4 — Multi-node clustering + leader election

pub mod command;
pub mod rocks_apply;
pub mod state_machine;
pub mod storage;
pub mod transport;

pub use command::{MetaCommand, MetaCommandCodecError};
