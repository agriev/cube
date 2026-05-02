//! `RaftStorage` — `raft::Storage` impl backed by RocksDB. M2.2.
//!
//! Replaces the in-memory `MemStorage` from M2.1 so the Raft log
//! survives router restarts. This is the durability foundation for
//! every later milestone — without it, a leader pod restart would
//! lose recent committed entries before the apply path got to them.
//!
//! # Layout
//!
//! Two column families inside a single RocksDB instance under
//! `<data_dir>/raft-log/`:
//!
//! - `entries`: keyed by 8-byte big-endian `u64` log index → protobuf
//!   `Entry` bytes. Big-endian so RocksDB's lexicographic ordering
//!   matches numeric index order; range scans over the log are O(N).
//! - `meta`: small fixed keys for `hard_state`, `conf_state`, and
//!   `applied_index`. All protobuf-encoded.
//!
//! # Why a separate RocksDB instance from the metastore
//!
//! 1. Log compaction / truncation semantics are different from
//!    state-machine compaction; mixing them complicates both.
//! 2. Raft log is high-write, low-read; metastore is the opposite.
//!    Different RocksDB tunings fit.
//! 3. Independent backup / restore.
//!
//! # Crash safety
//!
//! Every write goes through a `WriteOptions` with `set_sync(true)` —
//! Raft correctness requires fsync-before-ack. Reads are unsynced.
//! See plan risk #3.

use protobuf::Message as ProtobufMessage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
// cubestore doesn't depend on rocksdb directly — it re-exports through cuberockstore.
use cuberockstore::rocksdb::{
    self, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use std::path::Path;
use std::sync::{Arc, RwLock};

const CF_ENTRIES: &str = "entries";
const CF_META: &str = "meta";

const META_KEY_HARD_STATE: &[u8] = b"hard_state";
const META_KEY_CONF_STATE: &[u8] = b"conf_state";
const META_KEY_APPLIED_INDEX: &[u8] = b"applied_index";

/// Errors specific to the storage layer.
#[derive(Debug)]
pub enum RaftStorageError {
    Rocksdb(rocksdb::Error),
    Protobuf(protobuf::ProtobufError),
    Inconsistent(String),
}

impl std::fmt::Display for RaftStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rocksdb(e) => write!(f, "rocksdb: {}", e),
            Self::Protobuf(e) => write!(f, "protobuf: {}", e),
            Self::Inconsistent(msg) => write!(f, "inconsistent raft storage: {}", msg),
        }
    }
}

impl std::error::Error for RaftStorageError {}

impl From<rocksdb::Error> for RaftStorageError {
    fn from(e: rocksdb::Error) -> Self {
        Self::Rocksdb(e)
    }
}

impl From<protobuf::ProtobufError> for RaftStorageError {
    fn from(e: protobuf::ProtobufError) -> Self {
        Self::Protobuf(e)
    }
}

/// raft-rs's `Storage` trait surfaces a `raft::Error` for unrecoverable
/// failures. Convert ours to that.
fn storage_unavailable<T: std::fmt::Display>(e: T) -> raft::Error {
    raft::Error::Store(StorageError::Other(format!("{}", e).into()))
}

/// Persistent backing for a single Raft replica's log + meta state.
///
/// Cheap to clone (DB handle is internally `Arc`'d in rocksdb).
pub struct RaftStorage {
    db: DB,
    /// Cached HardState — consulted on every Storage::initial_state(),
    /// updated atomically with disk write on `set_hard_state`.
    hard_state: RwLock<HardState>,
    /// Cached ConfState — same caching pattern as HardState.
    conf_state: RwLock<ConfState>,
}

impl RaftStorage {
    /// Open an existing RaftStorage or create one if `<dir>` is empty.
    /// First boot seeds an empty HardState (term=0, vote=0, commit=0)
    /// and the supplied `voters` ConfState (e.g. `[node_id]` for a
    /// single-node cluster).
    pub fn open<P: AsRef<Path>>(
        dir: P,
        bootstrap_voters: Vec<u64>,
    ) -> Result<Self, RaftStorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_ENTRIES, Options::default()),
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, dir, cfs)?;

        // Bootstrap meta on first boot, or load existing.
        let hard_state = match get_meta::<HardState>(&db, META_KEY_HARD_STATE)? {
            Some(hs) => hs,
            None => {
                let hs = HardState::default();
                put_meta(&db, META_KEY_HARD_STATE, &hs)?;
                hs
            }
        };
        let conf_state = match get_meta::<ConfState>(&db, META_KEY_CONF_STATE)? {
            Some(cs) => cs,
            None => {
                let mut cs = ConfState::default();
                cs.set_voters(bootstrap_voters);
                put_meta(&db, META_KEY_CONF_STATE, &cs)?;
                cs
            }
        };

        // Seed an empty entry at index 0 if log is empty — raft-rs's
        // contract requires `first_index() == last_index() + 1` when
        // the log is "empty after a snapshot at index N", and N must
        // be at least 0. We materialize a zeroth Entry so subsequent
        // first_index/last_index/term calls have something to point at.
        let cf_e = db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;
        let last = db.iterator_cf(cf_e, IteratorMode::End).next();
        if last.is_none() {
            let zero_entry = Entry::default();
            let mut wb = WriteBatch::default();
            wb.put_cf(cf_e, &index_key(0), zero_entry.write_to_bytes()?);
            db.write_opt(wb, &sync_write())?;
        }

        Ok(Self {
            db,
            hard_state: RwLock::new(hard_state),
            conf_state: RwLock::new(conf_state),
        })
    }

    /// Append entries to the log. raft-rs's contract: entries[0].index
    /// must equal `last_index() + 1` OR overwrite the existing tail
    /// (this happens after a leader change with conflicting suffixes).
    pub fn append(&self, entries: &[Entry]) -> Result<(), RaftStorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;

        let first_new = entries[0].index;
        let last_existing = self.last_index_internal()?;

        let mut wb = WriteBatch::default();

        // If the new prefix overlaps the existing log, truncate the
        // tail past the new prefix's start. Standard raft-rs append
        // semantics — followers do this when reconciling against the
        // leader's authoritative log.
        if first_new <= last_existing {
            for stale in first_new..=last_existing {
                wb.delete_cf(cf_e, &index_key(stale));
            }
        }

        for e in entries {
            wb.put_cf(cf_e, &index_key(e.index), e.write_to_bytes()?);
        }
        self.db.write_opt(wb, &sync_write())?;
        Ok(())
    }

    pub fn set_hard_state(&self, hs: HardState) -> Result<(), RaftStorageError> {
        put_meta(&self.db, META_KEY_HARD_STATE, &hs)?;
        *self.hard_state.write().unwrap() = hs;
        Ok(())
    }

    pub fn set_conf_state(&self, cs: ConfState) -> Result<(), RaftStorageError> {
        put_meta(&self.db, META_KEY_CONF_STATE, &cs)?;
        *self.conf_state.write().unwrap() = cs;
        Ok(())
    }

    pub fn set_applied_index(&self, idx: u64) -> Result<(), RaftStorageError> {
        let bytes = idx.to_be_bytes();
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| RaftStorageError::Inconsistent("meta CF missing".into()))?;
        self.db.put_cf_opt(cf, META_KEY_APPLIED_INDEX, bytes, &sync_write())?;
        Ok(())
    }

    pub fn applied_index(&self) -> Result<Option<u64>, RaftStorageError> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| RaftStorageError::Inconsistent("meta CF missing".into()))?;
        Ok(self
            .db
            .get_cf(cf, META_KEY_APPLIED_INDEX)?
            .and_then(|b| {
                let mut buf = [0u8; 8];
                if b.len() == 8 {
                    buf.copy_from_slice(&b);
                    Some(u64::from_be_bytes(buf))
                } else {
                    None
                }
            }))
    }

    /// Compact log up to (but not including) `compact_to`. After a
    /// snapshot at index N, all entries with index < N can be dropped.
    pub fn compact(&self, compact_to: u64) -> Result<(), RaftStorageError> {
        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;
        let mut wb = WriteBatch::default();
        // Range delete: [0, compact_to)
        wb.delete_range_cf(cf_e, &index_key(0), &index_key(compact_to));
        self.db.write_opt(wb, &sync_write())?;
        Ok(())
    }

    fn first_index_internal(&self) -> Result<u64, RaftStorageError> {
        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;
        let mut it = self.db.iterator_cf(cf_e, IteratorMode::Start);
        match it.next() {
            Some(Ok((k, _))) => Ok(decode_index_key(&k)?),
            Some(Err(e)) => Err(RaftStorageError::Rocksdb(e)),
            None => Ok(0),
        }
    }

    fn last_index_internal(&self) -> Result<u64, RaftStorageError> {
        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;
        let mut it = self.db.iterator_cf(cf_e, IteratorMode::End);
        match it.next() {
            Some(Ok((k, _))) => Ok(decode_index_key(&k)?),
            Some(Err(e)) => Err(RaftStorageError::Rocksdb(e)),
            None => Ok(0),
        }
    }

    fn entry_at(&self, idx: u64) -> Result<Option<Entry>, RaftStorageError> {
        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| RaftStorageError::Inconsistent("entries CF missing".into()))?;
        match self.db.get_cf(cf_e, index_key(idx))? {
            Some(bytes) => Ok(Some(Entry::parse_from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }
}

// =============================================================================
// raft::Storage impl
// =============================================================================
impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hs = self.hard_state.read().unwrap().clone();
        let cs = self.conf_state.read().unwrap().clone();
        Ok(RaftState {
            hard_state: hs,
            conf_state: cs,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let max = max_size.into();
        let cf_e = self
            .db
            .cf_handle(CF_ENTRIES)
            .ok_or_else(|| storage_unavailable("entries CF missing"))?;
        let mut out = Vec::new();
        let mut total: u64 = 0;
        for idx in low..high {
            let bytes = self
                .db
                .get_cf(cf_e, index_key(idx))
                .map_err(storage_unavailable)?
                .ok_or(raft::Error::Store(StorageError::Unavailable))?;
            let entry = Entry::parse_from_bytes(&bytes).map_err(storage_unavailable)?;
            let entry_size = bytes.len() as u64;
            if let Some(cap) = max {
                if !out.is_empty() && total + entry_size > cap {
                    break;
                }
            }
            total += entry_size;
            out.push(entry);
        }
        Ok(out)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        match self.entry_at(idx).map_err(storage_unavailable)? {
            Some(e) => Ok(e.term),
            None => Err(raft::Error::Store(StorageError::Unavailable)),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        // raft-rs convention: first_index is the first ENTRY index, i.e.
        // 1 past the last applied snapshot. With the seed entry at 0,
        // first_index() returns 1 once any real entry has been appended,
        // and 0 otherwise (matches MemStorage behavior).
        let first = self.first_index_internal().map_err(storage_unavailable)?;
        Ok(first.max(1))
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.last_index_internal().map_err(storage_unavailable)
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        // Snapshots are M5. For M2/M3/M4 we return SnapshotTemporarilyUnavailable
        // which raft-rs handles by retrying later.
        Err(raft::Error::Store(StorageError::SnapshotTemporarilyUnavailable))
    }
}

// =============================================================================
// SharedRaftStorage — Cloneable Arc-wrapped RaftStorage so that raft-rs's
// RawNode<S: Storage> can hold one handle while the apply loop holds another.
//
// raft-rs's MemStorage is itself Cloneable via internal Arc<RwLock<...>>.
// We don't want to bake Arc into RaftStorage's struct (it complicates the
// open/close lifecycle), so we wrap it externally.
// =============================================================================

#[derive(Clone)]
pub struct SharedRaftStorage(Arc<RaftStorage>);

impl SharedRaftStorage {
    pub fn new(inner: Arc<RaftStorage>) -> Self {
        Self(inner)
    }

    pub fn append(&self, entries: &[Entry]) -> Result<(), RaftStorageError> {
        self.0.append(entries)
    }

    pub fn set_hard_state(&self, hs: HardState) -> Result<(), RaftStorageError> {
        self.0.set_hard_state(hs)
    }

    pub fn set_conf_state(&self, cs: ConfState) -> Result<(), RaftStorageError> {
        self.0.set_conf_state(cs)
    }

    pub fn set_applied_index(&self, idx: u64) -> Result<(), RaftStorageError> {
        self.0.set_applied_index(idx)
    }

    pub fn applied_index(&self) -> Result<Option<u64>, RaftStorageError> {
        self.0.applied_index()
    }

    /// Convenience: same as `applied_index().unwrap_or(0)`. Used by
    /// the state machine boot path to seed `Config::applied`.
    pub fn applied_index_or_zero(&self) -> u64 {
        self.0.applied_index().ok().flatten().unwrap_or(0)
    }

    pub fn compact(&self, compact_to: u64) -> Result<(), RaftStorageError> {
        self.0.compact(compact_to)
    }
}

impl Storage for SharedRaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.0.initial_state()
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.0.entries(low, high, max_size, ctx)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.0.term(idx)
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.0.first_index()
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.0.last_index()
    }
    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        self.0.snapshot(request_index, to)
    }
}

impl RaftStorage {
    /// Test helper used by state_machine::tests to verify log
    /// persistence across restart without going through the public
    /// raft::Storage trait.
    #[cfg(test)]
    pub fn last_index_internal_for_test(&self) -> u64 {
        self.last_index_internal().unwrap_or(0)
    }
}

// =============================================================================
// helpers
// =============================================================================

fn index_key(idx: u64) -> [u8; 8] {
    idx.to_be_bytes()
}

fn decode_index_key(b: &[u8]) -> Result<u64, RaftStorageError> {
    if b.len() != 8 {
        return Err(RaftStorageError::Inconsistent(format!(
            "index key wrong length: {}",
            b.len()
        )));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(b);
    Ok(u64::from_be_bytes(buf))
}

fn sync_write() -> WriteOptions {
    let mut wo = WriteOptions::default();
    // Raft correctness depends on fsync-before-ack — without this, a
    // leader could lose committed entries on a kernel crash and a
    // follower elected next term wouldn't know about them.
    wo.set_sync(true);
    wo
}

fn put_meta<T: ProtobufMessage>(
    db: &DB,
    key: &[u8],
    value: &T,
) -> Result<(), RaftStorageError> {
    let cf = db
        .cf_handle(CF_META)
        .ok_or_else(|| RaftStorageError::Inconsistent("meta CF missing".into()))?;
    let bytes = value.write_to_bytes()?;
    db.put_cf_opt(cf, key, bytes, &sync_write())?;
    Ok(())
}

fn get_meta<T: ProtobufMessage>(
    db: &DB,
    key: &[u8],
) -> Result<Option<T>, RaftStorageError> {
    let cf = db
        .cf_handle(CF_META)
        .ok_or_else(|| RaftStorageError::Inconsistent("meta CF missing".into()))?;
    Ok(match db.get_cf(cf, key)? {
        Some(bytes) => Some(T::parse_from_bytes(&bytes)?),
        None => None,
    })
}

// =============================================================================
// Tests — pure storage layer, no Raft state machine
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_entry(idx: u64, term: u64, data: &[u8]) -> Entry {
        let mut e = Entry::default();
        e.index = idx;
        e.term = term;
        // raft-rs's protobuf-codec uses `bytes::Bytes` for the data field;
        // Vec<u8> coerces via Into.
        e.data = data.to_vec().into();
        e
    }

    #[test]
    fn fresh_storage_initial_state_has_default_hardstate_and_voters() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        let st = s.initial_state().unwrap();
        assert_eq!(st.hard_state.term, 0);
        assert_eq!(st.hard_state.vote, 0);
        assert_eq!(st.hard_state.commit, 0);
        assert_eq!(st.conf_state.voters, vec![1]);
        // Empty log past the seed entry: first_index() returns 1.
        assert_eq!(s.first_index().unwrap(), 1);
        assert_eq!(s.last_index().unwrap(), 0);
    }

    #[test]
    fn append_and_read_back_entries() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        let entries = vec![
            make_entry(1, 1, b"a"),
            make_entry(2, 1, b"bb"),
            make_entry(3, 1, b"ccc"),
        ];
        s.append(&entries).unwrap();
        assert_eq!(s.last_index().unwrap(), 3);
        assert_eq!(s.first_index().unwrap(), 1);

        let read = s
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(read.len(), 3);
        for (a, b) in entries.iter().zip(read.iter()) {
            assert_eq!(a.index, b.index);
            assert_eq!(a.term, b.term);
            assert_eq!(a.data, b.data);
        }
    }

    #[test]
    fn append_overwrites_conflicting_suffix() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        s.append(&[
            make_entry(1, 1, b"old1"),
            make_entry(2, 1, b"old2"),
            make_entry(3, 1, b"old3"),
        ])
        .unwrap();
        // Leader change: new leader's term-2 entries overwrite term-1
        // suffix from index 2.
        s.append(&[
            make_entry(2, 2, b"new2"),
            make_entry(3, 2, b"new3"),
            make_entry(4, 2, b"new4"),
        ])
        .unwrap();
        assert_eq!(s.last_index().unwrap(), 4);
        let read = s
            .entries(1, 5, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(read.len(), 4);
        assert_eq!(read[0].data.as_ref(), b"old1");
        assert_eq!(read[0].term, 1);
        assert_eq!(read[1].data.as_ref(), b"new2");
        assert_eq!(read[1].term, 2);
        assert_eq!(read[3].data.as_ref(), b"new4");
    }

    #[test]
    fn term_returns_entry_term() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        s.append(&[make_entry(1, 7, b"x"), make_entry(2, 7, b"y")])
            .unwrap();
        assert_eq!(s.term(1).unwrap(), 7);
        assert_eq!(s.term(2).unwrap(), 7);
        assert!(matches!(
            s.term(99),
            Err(raft::Error::Store(StorageError::Unavailable))
        ));
    }

    #[test]
    fn entries_respects_max_size_after_first() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        s.append(&[
            make_entry(1, 1, &vec![0u8; 100]),
            make_entry(2, 1, &vec![0u8; 100]),
            make_entry(3, 1, &vec![0u8; 100]),
        ])
        .unwrap();
        // max_size = 50: first entry is always returned (raft-rs
        // convention), but the loop stops there because adding more
        // would exceed the cap.
        let r = s
            .entries(1, 4, Some(50u64), GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(r.len(), 1, "first entry forced, second over cap");
        assert_eq!(r[0].index, 1);
    }

    #[test]
    fn hard_state_persists_across_open() {
        let dir = TempDir::new().unwrap();
        {
            let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
            let mut hs = HardState::default();
            hs.term = 42;
            hs.vote = 7;
            hs.commit = 100;
            s.set_hard_state(hs).unwrap();
        }
        // Reopen — hard state should round-trip.
        let s2 = RaftStorage::open(dir.path(), vec![1]).unwrap();
        let st = s2.initial_state().unwrap();
        assert_eq!(st.hard_state.term, 42);
        assert_eq!(st.hard_state.vote, 7);
        assert_eq!(st.hard_state.commit, 100);
    }

    #[test]
    fn entries_persist_across_open() {
        // The whole point of M2.2 — log survives restart.
        let dir = TempDir::new().unwrap();
        {
            let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
            s.append(&[
                make_entry(1, 1, b"survive"),
                make_entry(2, 1, b"this"),
                make_entry(3, 1, b"restart"),
            ])
            .unwrap();
        }
        let s2 = RaftStorage::open(dir.path(), vec![1]).unwrap();
        assert_eq!(s2.last_index().unwrap(), 3);
        let read = s2
            .entries(1, 4, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].data.as_ref(), b"survive");
        assert_eq!(read[1].data.as_ref(), b"this");
        assert_eq!(read[2].data.as_ref(), b"restart");
    }

    #[test]
    fn applied_index_round_trip() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        assert_eq!(s.applied_index().unwrap(), None);
        s.set_applied_index(42).unwrap();
        assert_eq!(s.applied_index().unwrap(), Some(42));
        // Persist across reopen
        drop(s);
        let s2 = RaftStorage::open(dir.path(), vec![1]).unwrap();
        assert_eq!(s2.applied_index().unwrap(), Some(42));
    }

    #[test]
    fn compact_drops_old_entries() {
        let dir = TempDir::new().unwrap();
        let s = RaftStorage::open(dir.path(), vec![1]).unwrap();
        s.append(&[
            make_entry(1, 1, b"1"),
            make_entry(2, 1, b"2"),
            make_entry(3, 1, b"3"),
            make_entry(4, 1, b"4"),
        ])
        .unwrap();
        s.compact(3).unwrap();
        // Entries 1, 2 dropped. Entry 3 retained.
        assert_eq!(s.last_index().unwrap(), 4);
        // After compaction first_index() reflects the surviving prefix.
        assert_eq!(s.first_index_internal().unwrap(), 3);
        assert!(matches!(
            s.term(1),
            Err(raft::Error::Store(StorageError::Unavailable))
        ));
        assert_eq!(s.term(3).unwrap(), 1);
    }
}
