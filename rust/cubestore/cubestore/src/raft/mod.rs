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
//! - [`state_machine`] — `RaftNode`, the Raft consensus engine + apply
//!   task. Wraps `raft-rs`'s `RawNode` in an mpsc/oneshot propose API.
//! - [`raft_meta_store`] — `RaftMetaStore`, the production
//!   `MetaStore` wrapper that uses `RaftNode` to replicate writes.
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
//! - [x] M3 — full `RaftMetaStore: MetaStore` impl + DI binding
//!         + determinism fix + sql-tests passing under HA mode
//! - [x] M4.1 — `Transport` trait + `LocalLoopback` test transport
//! - [x] M4.2 — `RaftNode::start_multi_node` — outbound + inbound
//!         message paths, persisted-messages quirk fixed
//! - [x] M4.3 — 3-node cluster test: election + replication + apply
//! - [x] M4.4 — Leader failover test via partition injection
//! - [x] M4.5.1 — `HaPeer` parser + `CUBESTORE_RAFT_PEERS` config
//! - [x] M4.5.2 — `TcpTransport` + `spawn_listener` + 3-node-over-TCP
//! - [x] M4.5.3 — `configure_meta_store` boot wiring through TCP
//! - [x] M4 chaos — 20 partition cycles converge under 5s each
//! - [x] M9.1 — operator metrics (`cs.raft.*` term, leader_id,
//!         commit/applied index, leader_changes, proposals,
//!         apply_duration_ms)
//! - [ ] M5 — Snapshot + log compaction over `RemoteFs`

pub mod command;
pub mod raft_meta_store;
pub mod rocks_apply;
pub mod snapshot_builder;
pub mod snapshot_payload;
pub mod state_machine;
pub mod storage;
pub mod transport;

pub use command::{MetaCommand, MetaCommandCodecError};
pub use raft_meta_store::RaftMetaStore;
pub use transport::{spawn_listener, Inbound, LocalLoopback, TcpTransport, Transport};
