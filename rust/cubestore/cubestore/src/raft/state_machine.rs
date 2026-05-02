//! `RaftMetaStore` — the wrapper-style `MetaStore` impl that routes every
//! write through the Raft log before applying to local RocksDB.
//!
//! ## Status
//!
//! M1 stub. The full implementation lands in M2 (single-node Raft) and
//! M3 (every `MetaStore` write method wired to a `MetaCommand`).
//!
//! ## Design notes (intent)
//!
//! - Owns an `Arc<RocksMetaStore>` as the local state-machine backend.
//!   Apply path uses the same `RocksMetaStore::write_operation` /
//!   `BatchPipe` hooks as the existing direct-write path — the only
//!   difference is who calls them (apply task vs. RPC handler).
//! - Reads delegate straight to the inner `RocksMetaStore` on the
//!   leader; followers reject reads in M1 (return a "redirect to
//!   leader" error). Follower reads land in Phase 3.
//! - Writes: build the `MetaCommand`, call `Raft::propose`, await the
//!   commit notification on a oneshot channel that the apply loop
//!   resolves once it has finished applying that log index.
//! - ID generation: any `MetaStore` method that would otherwise call
//!   `next_id()` on the inner store now has the leader assign the ID,
//!   stamp it into the `MetaCommand`, and ship that. Followers apply
//!   verbatim — see plan risk #1.

use crate::raft::command::MetaCommand;
use crate::CubeError;

/// Marker / placeholder for the M2 implementation.
///
/// Compiles today as an empty struct so the module hierarchy is valid;
/// will be fleshed out in the next milestone.
#[allow(dead_code)]
pub struct RaftMetaStore {
    // Inner RocksMetaStore, raft-rs RawNode, apply-loop handle, ...
    _placeholder: (),
}

impl RaftMetaStore {
    /// M2 entry point: propose a `MetaCommand` to the Raft group and
    /// resolve the returned future once the entry has been committed
    /// and applied locally.
    #[allow(dead_code)]
    pub async fn propose(&self, _cmd: MetaCommand) -> Result<(), CubeError> {
        Err(CubeError::internal(
            "RaftMetaStore::propose is unimplemented (M2 milestone)".to_string(),
        ))
    }
}
