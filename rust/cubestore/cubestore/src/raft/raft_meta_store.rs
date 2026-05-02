//! `RaftMetaStore` — the production HA wrapper that will eventually
//! implement the `MetaStore` trait by routing every write through Raft.
//!
//! ## Architecture (M3.5)
//!
//! Each `RaftMetaStore` holds:
//! - a [`RaftNode`] for proposing commands;
//! - an `Arc<RocksMetaStore>` shared with the apply impl, used to
//!   serve **read** methods directly without going through Raft.
//!
//! Read methods (the trait's `get_*`, `all_*`, `not_ready_*`,
//! `tables_table()`, etc.) delegate straight to the local
//! `RocksMetaStore`. They are **leader-only** in v1: writes are acked
//! durable on the leader's local store before the propose returns,
//! so reads are linearizable with the writer's view. Followers are
//! receive-only in v1; M4 wires read-from-follower with a small
//! consistency window.
//!
//! Write methods encode their arguments into a `MetaCommand`, call
//! `RaftNode::propose(cmd).await`, and decode the typed return back
//! out of `MetaCommandResult` via the M3.2 helpers. Argument shapes
//! that include metastore types (Column, Partition, Job, …) get
//! flex-encoded into blob fields on the `MetaCommand` variant; the
//! dispatch in `RocksMetaStoreApply` decodes them back.
//!
//! ## Sub-milestones
//!
//! - **M3.5.a (this commit)** — wrapper skeleton with
//!   `start_single_node`, `propose`, and a handful of representative
//!   inherent methods (`create_schema`, `delete_schema`,
//!   `drop_table`, plus reads). e2e test proves propose-and-decode
//!   wires through.
//! - **M3.5.b** — full `impl MetaStore for RaftMetaStore { ... }` —
//!   ~121 methods, mostly mechanical. Read methods delegate to
//!   `self.store.*`; write methods route through `propose()`.
//! - **M3.5.c** — `CUBESTORE_HA_MODE` env var wired into the boot
//!   path; when `=raft`, swap the DI binding from `RocksMetaStore` to
//!   `RaftMetaStore`.
//!
//! ## Determinism caveat (still unfixed)
//!
//! Methods that internally call `next_id()` (every create) or
//! `Utc::now()` (`add_job`, a few `swap_*`) are non-deterministic
//! at the apply step. Today the wrapper proposes the bare args and
//! lets each replica generate its own ID/now — they will diverge.
//! M3.4 is the gating commit: it adds `assigned_id: Option<u64>`
//! and `assigned_now: Option<i64>` to the relevant `MetaCommand`
//! variants and changes the wrapper to pre-allocate them on the
//! leader before the propose. **Do not flip
//! `CUBESTORE_HA_MODE=raft` (M3.5.c) until M3.4 is in.**

use crate::metastore::table::Table;
use crate::metastore::{IdRow, MetaStore, RocksMetaStore, Schema};
use crate::raft::command::{IdRowKind, MetaCommand, MetaCommandResultMismatch};
use crate::raft::rocks_apply::RocksMetaStoreApply;
use crate::raft::state_machine::{RaftError, RaftNode};
use crate::CubeError;
use std::path::Path;
use std::sync::Arc;

/// HA-mode `MetaStore` impl substrate. See module docs.
pub struct RaftMetaStore {
    raft: RaftNode,
    pub(crate) store: Arc<RocksMetaStore>,
}

impl RaftMetaStore {
    /// Boot a single-node `RaftMetaStore` whose log lives at
    /// `raft_log_dir`. The caller has already constructed the local
    /// `RocksMetaStore`; we build a `RocksMetaStoreApply` from it and
    /// hand that to the Raft task.
    ///
    /// Multi-node boot lands in M4.
    pub fn start_single_node(
        raft_log_dir: impl AsRef<Path>,
        node_id: u64,
        store: Arc<RocksMetaStore>,
    ) -> Result<Arc<Self>, RaftError> {
        // Concrete-type Arc — `RaftNode::start_single_node` is generic
        // over `A: Apply` (implicitly `Sized`), so an `Arc<dyn Apply>`
        // would fail the size check. Concrete is also lighter — no
        // vtable per propose.
        let apply = Arc::new(RocksMetaStoreApply::new(store.clone()));
        let raft = RaftNode::start_single_node(raft_log_dir, node_id, apply)?;
        Ok(Arc::new(Self { raft, store }))
    }

    /// Lower-level handle to the inner `RaftNode`. Public so M3.5.b
    /// can route trait writes through it without needing to expose
    /// any extra plumbing.
    pub(crate) fn raft(&self) -> &RaftNode {
        &self.raft
    }

    /// The local `RocksMetaStore`. Read methods in M3.5.b delegate
    /// here; tests use this to assert post-apply state.
    pub fn local_store(&self) -> &Arc<RocksMetaStore> {
        &self.store
    }

    /// Map an apply-result-shape mismatch to a `CubeError` — see the
    /// note on `MetaCommandResultMismatch` in `command.rs`.
    pub(crate) fn mismatch(method: &'static str, e: MetaCommandResultMismatch) -> CubeError {
        CubeError::internal(format!(
            "RaftMetaStore::{} — apply produced a wrong result shape: {}",
            method, e
        ))
    }

    // -------------------------------------------------------------------
    // Representative inherent methods exercised by the e2e test below.
    // M3.5.b will replace these with a full `impl MetaStore` block
    // covering all 121 trait methods. Until then, we keep these here so
    // the wiring is testable.
    // -------------------------------------------------------------------

    /// Schema create — proposed via Raft.
    pub async fn create_schema(
        &self,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        let r = self
            .raft
            .propose(MetaCommand::CreateSchema {
                schema_name,
                if_not_exists,
            })
            .await?;
        r.into_id_row(IdRowKind::Schema)
            .map_err(|e| Self::mismatch("create_schema", e))
    }

    /// Schema rename — proposed via Raft.
    pub async fn rename_schema(
        &self,
        old_schema_name: String,
        new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        let r = self
            .raft
            .propose(MetaCommand::RenameSchema {
                old_schema_name,
                new_schema_name,
            })
            .await?;
        r.into_id_row(IdRowKind::Schema)
            .map_err(|e| Self::mismatch("rename_schema", e))
    }

    /// Schema delete by name — proposed via Raft.
    pub async fn delete_schema(&self, schema_name: String) -> Result<(), CubeError> {
        let r = self
            .raft
            .propose(MetaCommand::DeleteSchema { schema_name })
            .await?;
        r.into_unit().map_err(|e| Self::mismatch("delete_schema", e))
    }

    /// Drop a table by id — proposed via Raft.
    pub async fn drop_table(&self, table_id: u64) -> Result<IdRow<Table>, CubeError> {
        let r = self
            .raft
            .propose(MetaCommand::DropTable { table_id })
            .await?;
        r.into_id_row(IdRowKind::Table)
            .map_err(|e| Self::mismatch("drop_table", e))
    }

    /// Schema list — read served from local store (no Raft hop).
    pub async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError> {
        self.store.get_schemas().await
    }
}

// =============================================================================
// Tests — propose-and-read end-to-end through the production wrapper.
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::metastore::{BaseRocksStoreFs, RocksMetaStore};
    use crate::remotefs::LocalDirRemoteFs;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    /// Set up a temp `RocksMetaStore` and a `RaftMetaStore` wrapping
    /// it, with the Raft log in a separate temp dir. Returns paths
    /// that the caller is responsible for cleaning up.
    fn setup_wrapper(
        test_name: &str,
    ) -> (
        Arc<RaftMetaStore>,
        std::path::PathBuf,
        std::path::PathBuf,
        TempDir,
    ) {
        let config = Config::test(test_name);
        let cwd = env::current_dir().unwrap();
        let store_path = cwd.join(format!("{}-local", test_name));
        let remote_path = cwd.join(format!("{}-remote", test_name));
        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_path);

        let remote_fs = LocalDirRemoteFs::new(Some(remote_path.clone()), store_path.clone());
        let rocks = RocksMetaStore::new(
            store_path.join("metastore").as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )
        .expect("RocksMetaStore::new");

        let raft_dir = TempDir::new().expect("raft tempdir");
        let wrapper = RaftMetaStore::start_single_node(raft_dir.path(), 1, rocks)
            .expect("RaftMetaStore::start_single_node");

        (wrapper, store_path, remote_path, raft_dir)
    }

    fn cleanup(store_path: &std::path::Path, remote_path: &std::path::Path) {
        let _ = fs::remove_dir_all(store_path);
        let _ = fs::remove_dir_all(remote_path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn schema_lifecycle_routes_through_raft() {
        let (wrapper, sp, rp, _raft_dir) = setup_wrapper("raft_meta_store_lifecycle");

        // Settle initial campaign (single-node).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // CREATE — proposed through Raft, returns IdRow<Schema>.
        let created = wrapper
            .create_schema("public".into(), false)
            .await
            .expect("create_schema");
        assert_eq!(created.get_row().get_name(), "public");

        // GET — served from local store. The leader has applied the
        // entry by the time `create_schema` returned, so this is
        // linearizable with the write.
        let listed = wrapper.get_schemas().await.expect("get_schemas");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].get_id(), created.get_id());

        // RENAME — proposed.
        let renamed = wrapper
            .rename_schema("public".into(), "renamed".into())
            .await
            .expect("rename_schema");
        assert_eq!(renamed.get_row().get_name(), "renamed");
        assert_eq!(renamed.get_id(), created.get_id());

        // DELETE — proposed; returns Unit.
        wrapper
            .delete_schema("renamed".into())
            .await
            .expect("delete_schema");

        let after = wrapper.get_schemas().await.expect("get_schemas");
        assert!(after.is_empty(), "schema must be gone after delete");

        cleanup(&sp, &rp);
    }
}
