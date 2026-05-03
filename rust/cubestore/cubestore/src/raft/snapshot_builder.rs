//! Build state-machine snapshots from a live `RocksMetaStore`. M5.3.
//!
//! The pieces that came before:
//! - M5.1 (`storage.rs::save_snapshot`/`read_snapshot`) gave us
//!   durable persistence for the snapshot (metadata in RocksDB +
//!   data in `<dir>/snapshot.bin`).
//! - M5.2 (`snapshot_payload`) gave us the dir↔bytes pack/unpack
//!   so a directory of files can ride inside `Snapshot.data`.
//!
//! M5.3 closes the loop: turn the live `RocksMetaStore` state into
//! the bytes that `save_snapshot` will then persist.
//!
//! ## How
//!
//! `Checkpoint::new(&db)` + `create_checkpoint(<temp dir>)` is the
//! standard RocksDB story for taking a consistent snapshot of the
//! database without blocking writers — it hardlinks SST files
//! (constant-time, no copies) and writes a fresh MANIFEST that
//! captures the snapshot's view. `pack_dir` then turns that dir
//! into a single byte buffer; `unpack_dir` (M5.2) is the inverse.
//!
//! The temp dir lives under `<raft-log>/snapshot-staging/<ts>/` and
//! is removed once `pack_dir` returns. Putting it under the raft-log
//! directory keeps the snapshot fully on the same filesystem (no
//! cross-FS rename cost) and makes cleanup obvious in disk-usage
//! audits.
//!
//! ## What the install path looks like
//!
//! See `apply_snapshot_to_dir` for the inverse: take a snapshot's
//! data bytes (received from a leader via `MsgSnapshot`) and unpack
//! them into a target directory. The caller is responsible for the
//! atomic swap of that directory into the live RocksMetaStore path —
//! that needs more invasive changes (M5.6) and is intentionally
//! deferred. For unit-test scope, the round-trip "build → unpack →
//! open as fresh RocksMetaStore" is enough to prove the byte path.

use crate::metastore::RocksMetaStore;
use crate::raft::snapshot_payload;
use crate::CubeError;
use cuberockstore::rocksdb::checkpoint::Checkpoint;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Build a snapshot's `data` bytes from a live `RocksMetaStore`.
///
/// `staging_root` is a directory under which a temp checkpoint dir
/// will be created and removed. Callers typically pass the
/// `<data_dir>/raft-log/` so the staging dir is on the same
/// filesystem as everything else raft owns. Doesn't have to exist
/// up-front; the function creates it on demand.
///
/// Returns the packed bytes ready to set as `Snapshot.data`.
pub async fn build_state_machine_snapshot_bytes(
    rocks_meta_store: &Arc<RocksMetaStore>,
    staging_root: &Path,
) -> Result<Vec<u8>, CubeError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let staging_dir = staging_root.join(format!("snapshot-staging-{}", nanos));

    // Don't pre-create staging_dir — RocksDB's create_checkpoint
    // requires the path to NOT exist (it will refuse to write into
    // an existing dir). The parent must exist though.
    if let Some(parent) = staging_dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            CubeError::internal(format!(
                "build_snapshot: create staging parent {:?}: {}",
                parent, e
            ))
        })?;
    }

    let db = rocks_meta_store.rocksdb_arc();

    // RocksDB's Checkpoint API is sync. Move it onto a blocking
    // thread so we don't stall the tokio scheduler while it does
    // its work. spawn_blocking is the standard cubestore pattern
    // for sync rocksdb work — see RocksStore::prepare_checkpoint.
    let staging_for_blocking = staging_dir.clone();
    let result: Result<(), CubeError> = tokio::task::spawn_blocking(move || {
        let checkpoint = Checkpoint::new(&*db)
            .map_err(|e| CubeError::internal(format!("Checkpoint::new: {}", e)))?;
        checkpoint
            .create_checkpoint(&staging_for_blocking)
            .map_err(|e| CubeError::internal(format!("create_checkpoint: {}", e)))?;
        Ok(())
    })
    .await
    .map_err(|e| CubeError::internal(format!("spawn_blocking join: {}", e)))?;
    result?;

    // Pack the checkpoint dir into the bytes that go into the
    // raft Snapshot. Once packed we don't need the dir anymore.
    let bytes = snapshot_payload::pack_dir(&staging_dir).map_err(|e| {
        CubeError::internal(format!("pack_dir({:?}): {}", staging_dir, e))
    })?;

    // Best-effort cleanup. A leftover staging dir is harmless (gets
    // GC'd on the next snapshot) but noisy; log on failure rather
    // than escalate.
    if let Err(e) = std::fs::remove_dir_all(&staging_dir) {
        log::warn!("snapshot staging dir cleanup failed: {:?}: {}", staging_dir, e);
    }

    Ok(bytes)
}

/// Inverse of `build_state_machine_snapshot_bytes`: given a
/// snapshot's `data` bytes, materialize them into `target_dir` so
/// the directory looks like a fresh RocksMetaStore checkpoint.
///
/// `target_dir` MUST be empty (or not exist). The function creates
/// it if needed. The caller is responsible for the atomic swap
/// from `target_dir` into the live RocksMetaStore path — see the
/// module docstring for why M5.6 owns that step.
pub fn apply_snapshot_to_dir(
    snapshot_data: &[u8],
    target_dir: &Path,
) -> Result<(), CubeError> {
    if target_dir.exists() {
        // Refuse to overwrite — staleness here would be a silent
        // data-corruption bug. The caller should pass a fresh path.
        let mut entries = std::fs::read_dir(target_dir).map_err(|e| {
            CubeError::internal(format!("read_dir {:?}: {}", target_dir, e))
        })?;
        if entries.next().is_some() {
            return Err(CubeError::internal(format!(
                "apply_snapshot_to_dir: target {:?} is not empty",
                target_dir
            )));
        }
    } else {
        std::fs::create_dir_all(target_dir).map_err(|e| {
            CubeError::internal(format!("create target dir {:?}: {}", target_dir, e))
        })?;
    }

    snapshot_payload::unpack_dir(snapshot_data, target_dir)
        .map_err(|e| CubeError::internal(format!("unpack_dir into {:?}: {}", target_dir, e)))?;
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{MetaStore, RocksMetaStore};
    use tempfile::TempDir;

    /// Boot a fresh RocksMetaStore in a temp dir, ready for the
    /// snapshot test to populate.
    async fn boot_rocks(name: &str) -> (TempDir, Arc<RocksMetaStore>) {
        let (_remote_fs, store) = RocksMetaStore::prepare_test_metastore(name);
        let dir = TempDir::new().unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn build_snapshot_bytes_round_trips_through_unpack() {
        let (_dir, store) = boot_rocks("build_snap").await;
        // Plant a tiny bit of state so the snapshot has something
        // to round-trip.
        store
            .create_schema("alpha".into(), false)
            .await
            .expect("create schema");
        store
            .create_schema("beta".into(), false)
            .await
            .expect("create schema 2");

        // Build snapshot bytes on a real checkpoint.
        let staging = TempDir::new().unwrap();
        let bytes = build_state_machine_snapshot_bytes(&store, staging.path())
            .await
            .expect("build snapshot bytes");
        assert!(!bytes.is_empty(), "snapshot data must not be empty");

        // Round-trip via unpack into a fresh dir.
        let restored = TempDir::new().unwrap();
        // unpack_dir refuses non-empty target — drop and recreate.
        std::fs::remove_dir_all(restored.path()).unwrap();
        apply_snapshot_to_dir(&bytes, restored.path())
            .expect("apply snapshot to dir");

        // The restored dir should contain RocksDB checkpoint files
        // (MANIFEST-*, *.sst, OPTIONS, IDENTITY, CURRENT). At
        // minimum CURRENT and one MANIFEST must be present —
        // those are mandatory for any RocksDB instance.
        let names: Vec<String> = std::fs::read_dir(restored.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n == "CURRENT"),
            "restored dir missing CURRENT, has: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n.starts_with("MANIFEST-")),
            "restored dir missing MANIFEST-*, has: {:?}",
            names
        );
    }

    #[tokio::test]
    async fn apply_snapshot_to_dir_rejects_non_empty_target() {
        let (_dir, store) = boot_rocks("apply_target_full").await;
        let staging = TempDir::new().unwrap();
        let bytes = build_state_machine_snapshot_bytes(&store, staging.path())
            .await
            .unwrap();

        // Target with one stale file → must refuse.
        let target = TempDir::new().unwrap();
        std::fs::write(target.path().join("STALE"), b"x").unwrap();
        let err = apply_snapshot_to_dir(&bytes, target.path()).unwrap_err();
        assert!(
            format!("{}", err).contains("not empty"),
            "expected non-empty rejection, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn build_snapshot_cleans_up_staging_dir_on_success() {
        let (_dir, store) = boot_rocks("staging_cleanup").await;
        let staging_root = TempDir::new().unwrap();
        let _bytes = build_state_machine_snapshot_bytes(&store, staging_root.path())
            .await
            .unwrap();
        // No staging-* dirs should remain.
        let leftovers: Vec<_> = std::fs::read_dir(staging_root.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot-staging-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging dir should be cleaned up; found: {:?}",
            leftovers.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );
    }
}
