//! `RaftStorage` — the `raft::Storage` impl backed by a dedicated
//! RocksDB instance under `<data_dir>/raft-log/`.
//!
//! ## Status
//!
//! M1 stub. Concrete impl in M2.
//!
//! ## Design notes (intent)
//!
//! - Separate RocksDB DB from the metastore one. Reasons:
//!   1. Log compaction / truncation semantics differ from state-machine
//!      column-family compaction; mixing them complicates both.
//!   2. The Raft log is high-write, low-read; the metastore is the
//!      opposite. Different RocksDB tunings fit.
//!   3. Easier to back up / restore independently.
//! - Three column families: `entries`, `hard_state`, `conf_state`.
//!   `entries` is keyed by big-endian `u64` log index for ordered scans.
//! - Snapshot trigger: `apply_index - first_log_index >= snapshot_interval`.
//!   Snapshot writes a state-machine RocksDB checkpoint into S3 via the
//!   existing `RemoteFs` trait, then truncates the log at that index.
//! - Crash safety: every write on the log path goes through RocksDB
//!   `WriteOptions::set_sync(true)` to guarantee fsync before ack — Raft
//!   correctness requires this.
//!
//! ## Reference impl
//!
//! TiKV's `raft-engine` crate. Don't roll our own — copy the trait
//! impl and adapt to our column-family layout. See plan risk #3.

#[allow(dead_code)]
pub struct RaftStorage {
    _placeholder: (),
}
