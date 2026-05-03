//! Raft transport — the seam between the Raft state machine and the
//! network. M4.1.
//!
//! ## Why a trait
//!
//! The `RaftNode` shouldn't know whether peer messages travel over
//! `cuberpc`, an in-memory mpsc, or a fault-injection harness. It just
//! ships outbound `raft::Message`s out and feeds inbound ones into
//! `RawNode::step`. We split that into:
//!
//! - [`Transport`] — what the Raft loop calls to send a message out.
//! - [`Inbound`] — a clonable handle the transport calls to deliver
//!   a message back into the Raft loop. Internally an mpsc tx; the
//!   raft task holds the rx and calls `raw.step(msg)` on each recv.
//!
//! Production wiring (cuberpc) lands later in M4 — same `Transport`
//! trait, just a different impl.
//!
//! ## In-memory `LocalLoopback`
//!
//! Used by every multi-node test in the raft module. Construct empty,
//! register each peer's [`Inbound`] keyed by node id, then hand the
//! same `Arc<LocalLoopback>` to every `RaftNode::start_multi_node`
//! call. `send` looks up the destination by id and pushes the message
//! into its `Inbound`.

use async_trait::async_trait;
use raft::eraftpb::Message;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::mpsc;

/// Outbound side: the Raft state machine ships messages here every
/// `Ready`. Implementations are responsible for delivering each
/// message to the peer named by `msg.to`. Best-effort — Raft retries
/// at the next heartbeat if a message is lost.
///
/// `send` is async to give RPC-backed impls a place to await without
/// blocking the raft tick loop. The raft loop spawns each send so
/// transport latency can't stall consensus.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, msg: Message);
}

/// Inbound side: a cheap-to-clone handle that lets a transport hand
/// a received `raft::Message` to the Raft loop. The loop owns the
/// matching receiver and calls `RawNode::step(msg)` for every recv.
///
/// `feed` is sync because the underlying mpsc::UnboundedSender is.
/// Backpressure is unbounded — losing inbound traffic to a full
/// queue would let consensus stall silently. The raft task drains
/// the queue every tick, so any real backlog implies a much bigger
/// problem (apply path stuck) that should surface elsewhere first.
#[derive(Clone)]
pub struct Inbound {
    tx: mpsc::UnboundedSender<Message>,
}

impl Inbound {
    pub fn feed(&self, msg: Message) {
        // Loss on a closed channel happens during shutdown — log only
        // if the recv side is still expected to be live.
        let _ = self.tx.send(msg);
    }

    /// Construct a paired `(Inbound, mpsc::UnboundedReceiver)`. The
    /// receiver is owned by the Raft task; the sender is given to
    /// the transport.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Message>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }
}

// =============================================================================
// LocalLoopback — in-memory transport for multi-node tests.
// =============================================================================

/// A `Transport` that routes messages between locally-running
/// `RaftNode` instances. Used by every multi-node test in the raft
/// module: no sockets, no serialization, no flake. Production uses
/// the cuberpc-backed transport instead.
pub struct LocalLoopback {
    peers: Mutex<HashMap<u64, Inbound>>,
}

impl LocalLoopback {
    pub fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }

    /// Wire up a peer. Call once per peer before any `RaftNode`
    /// starts campaigning, or the first heartbeat will be silently
    /// dropped.
    pub fn register(&self, node_id: u64, inbound: Inbound) {
        self.peers.lock().unwrap().insert(node_id, inbound);
    }

    /// Forget a peer — used by partition-injection tests to simulate
    /// a network split. Subsequent sends to this id are no-ops.
    pub fn unregister(&self, node_id: u64) {
        self.peers.lock().unwrap().remove(&node_id);
    }
}

impl Default for LocalLoopback {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for LocalLoopback {
    async fn send(&self, msg: Message) {
        // Snapshot the inbound under the lock, then drop the lock
        // before calling `feed` so a slow consumer can't stall other
        // senders (it can't here — feed is non-blocking — but keeps
        // the locking discipline honest).
        let target = {
            let peers = self.peers.lock().unwrap();
            peers.get(&msg.to).cloned()
        };
        if let Some(inbound) = target {
            inbound.feed(msg);
        }
        // Unknown peer → drop. Raft retries on the next heartbeat;
        // this is the same behavior as a real network drop.
    }
}

// =============================================================================
// Tests
// =============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::Message;

    #[tokio::test]
    async fn loopback_routes_by_target_id() {
        let loop_ = LocalLoopback::new();
        let (in1, mut rx1) = Inbound::new();
        let (in2, mut rx2) = Inbound::new();
        loop_.register(1, in1);
        loop_.register(2, in2);

        let mut m = Message::default();
        m.to = 2;
        loop_.send(m.clone()).await;

        assert!(rx1.try_recv().is_err(), "node 1 should not have received");
        let recv = rx2.try_recv().expect("node 2 should have received");
        assert_eq!(recv.to, 2);
    }

    #[tokio::test]
    async fn loopback_drops_messages_for_unregistered_peer() {
        let loop_ = LocalLoopback::new();
        let (in1, mut rx1) = Inbound::new();
        loop_.register(1, in1);

        let mut m = Message::default();
        m.to = 99; // not registered
        loop_.send(m).await;

        // Registered peer 1 still gets nothing — drop is silent.
        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn unregister_simulates_partition() {
        let loop_ = LocalLoopback::new();
        let (in1, mut rx1) = Inbound::new();
        loop_.register(1, in1);

        let mut m = Message::default();
        m.to = 1;
        loop_.send(m.clone()).await;
        assert!(rx1.try_recv().is_ok());

        loop_.unregister(1);
        loop_.send(m).await;
        assert!(rx1.try_recv().is_err(), "after unregister no delivery");
    }
}
