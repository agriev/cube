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
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::Message;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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
///
/// Semantics:
/// - [`register`] places a peer in the routing table.
/// - [`unregister`] is a **bidirectional** kill: messages targeting
///   this peer are dropped (no recipient) AND messages originating
///   from this peer are dropped (the peer is "off the network").
///
/// The bidirectional cut models a `kubectl delete pod --force` from
/// the surviving peers' point of view. A one-way cut (drop to-X but
/// keep from-X) would let the dead peer's stale heartbeats reset
/// survivors' election timers, blocking failover; that's a real
/// scheduler-timing flake we hit on Linux CI in the M4 push.
pub struct LocalLoopback {
    peers: Mutex<HashMap<u64, Inbound>>,
    /// Ids that have been `unregister`ed since the last `register`.
    /// Outbound messages where `msg.from` is in this set are dropped
    /// — the partitioned peer can't talk to anyone, not just its
    /// recipients can't talk to it.
    silenced: Mutex<HashSet<u64>>,
}

impl LocalLoopback {
    pub fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
            silenced: Mutex::new(HashSet::new()),
        }
    }

    /// Wire up a peer. Also lifts any prior `unregister`-induced
    /// silencing so a healed peer can speak again. Call once per
    /// peer before any `RaftNode` starts campaigning, or the first
    /// heartbeat will be silently dropped.
    pub fn register(&self, node_id: u64, inbound: Inbound) {
        self.peers.lock().unwrap().insert(node_id, inbound);
        self.silenced.lock().unwrap().remove(&node_id);
    }

    /// Bidirectional partition: peer disappears from routing table
    /// AND its outbound is silenced. Subsequent sends to/from this
    /// id are no-ops until [`register`] reconnects it.
    pub fn unregister(&self, node_id: u64) {
        self.peers.lock().unwrap().remove(&node_id);
        self.silenced.lock().unwrap().insert(node_id);
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
        // Bidirectional kill check: if either the sender or the
        // recipient has been `unregister`ed, drop. Done before the
        // peers-map lookup so a partitioned-off leader's heartbeats
        // never reach surviving peers (otherwise their election
        // timers reset and failover is blocked indefinitely).
        {
            let silenced = self.silenced.lock().unwrap();
            if silenced.contains(&msg.from) || silenced.contains(&msg.to) {
                return;
            }
        }

        // Snapshot the inbound under the lock, then drop the lock
        // before calling `feed` so a slow consumer can't stall other
        // senders (it can't here — feed is non-blocking — but keeps
        // the locking discipline honest).
        let target = {
            let peers = self.peers.lock().unwrap();
            peers.get(&msg.to).cloned()
        };
        // Off by default; set RAFT_LOOPBACK_TRACE=1 in test env to log
        // every cross-peer message. Useful when debugging the snapshot
        // ship path; quiet for the rest of the test suite.
        if cfg!(test) && std::env::var("RAFT_LOOPBACK_TRACE").is_ok() {
            eprintln!(
                "loopback: {:?} from={} to={} index={} term={} commit={} delivered={}",
                msg.msg_type,
                msg.from,
                msg.to,
                msg.index,
                msg.term,
                msg.commit,
                target.is_some()
            );
        }
        if let Some(inbound) = target {
            inbound.feed(msg);
        }
        // Unknown peer → drop. Raft retries on the next heartbeat;
        // this is the same behavior as a real network drop.
    }
}

// =============================================================================
// TcpTransport — production transport over plain TCP. M4.5.2.
// =============================================================================
//
// Wire format (per message):
//
// ```
// +----------+--------+----------+
// | magic u32| ver u32| len u32  |
// +----------+--------+----------+
// |     payload (proto bytes)    |
// +------------------------------+
// ```
//
// All fields big-endian. `magic = 0xCBE_RAFT_1` (`0xC8E_AAF1`) lets us
// fail loud if a non-raft client (Cube SQL, HTTP probe) hits this port
// — same defensive pattern as `cluster/message.rs`. `ver` is a
// future-proofing knob; bumped any time the framing or message type
// set changes incompatibly. `len` capped at 16 MiB (raft snapshots
// would exceed that — we'll add a streaming snapshot path in M5).
//
// Connection model
// ----------------
//
// Per peer, the dialer holds at most ONE open TCP connection. On boot
// each peer is dialed lazily — the first `send` triggers the connect.
// On any I/O error the connection is dropped; the next `send` retries.
// Raft tolerates message loss (MsgHeartbeat retry every heartbeat tick,
// MsgAppend re-derived from the leader's progress map), so a transient
// network blip just means a few hundred ms of stale follower state.
//
// We don't persistent-loop reconnects on the dialer side because:
//   1. Heartbeats fire every 150ms — that's the natural retry cadence.
//   2. A reconnect storm against a dead peer would mask actual outages.
//
// The listener spawns one task per inbound connection. Each task reads
// frames in a loop and `feed`s the `Inbound` until the peer hangs up.

const FRAME_MAGIC: u32 = 0xC8EA_AF01; // "Cube HA / RAFT v1"
const FRAME_VERSION: u32 = 1;
const FRAME_MAX_LEN: u32 = 16 * 1024 * 1024;

/// Listen for inbound raft connections. One TcpListener; one async
/// task per accepted connection. Returns when `bind` succeeds — the
/// caller spawns the returned `Future` in a tokio task. Drop the
/// returned `JoinHandle` to stop accepting new connections (existing
/// connections drain).
pub async fn spawn_listener(
    bind_addr: String,
    inbound: Inbound,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = TcpListener::bind(&bind_addr).await?;
    log::info!("raft transport: listening on {}", bind_addr);
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, peer)) => {
                    log::debug!("raft transport: inbound conn from {}", peer);
                    let inbound_for_conn = inbound.clone();
                    tokio::spawn(async move {
                        if let Err(e) = run_recv_loop(sock, inbound_for_conn).await {
                            log::debug!("raft transport: recv loop ended ({}): {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    // Transient errors (FD exhaustion, etc.) — back off
                    // before retrying so we don't spin against the
                    // accept syscall.
                    log::warn!("raft transport: accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(handle)
}

async fn run_recv_loop(mut sock: TcpStream, inbound: Inbound) -> std::io::Result<()> {
    loop {
        // Drop the per-frame magic+version+len header. EOF here is the
        // peer cleanly closing — return Ok and let the spawn task end.
        let magic = match sock.read_u32().await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        if magic != FRAME_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bad raft frame magic 0x{:08X}", magic),
            ));
        }
        let ver = sock.read_u32().await?;
        if ver != FRAME_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "raft frame version mismatch: got {}, want {}",
                    ver, FRAME_VERSION
                ),
            ));
        }
        let len = sock.read_u32().await?;
        if len > FRAME_MAX_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("raft frame too large: {} bytes", len),
            ));
        }
        let mut buf = vec![0u8; len as usize];
        sock.read_exact(&mut buf).await?;
        let mut msg = Message::default();
        if let Err(e) = msg.merge_from_bytes(&buf) {
            log::warn!("raft transport: dropping malformed frame: {}", e);
            continue;
        }
        inbound.feed(msg);
    }
}

/// Per-peer dial state. Originally we cached a long-lived TCP
/// stream and reused it for every message; that broke under
/// repeated pod restarts on real k8s — the cached stream points
/// at the OLD pod's IP after the peer is recreated, and the
/// kernel doesn't notice the connection is dead until ~30 s of
/// retransmit timeouts. Raft's election cadence (10 ticks ≈ 500 ms)
/// fires election storms much faster than that detection.
///
/// Current model: open a fresh TCP connection for each message.
/// This is wasteful — ~6 connects/peer/sec at the heartbeat
/// cadence, ~18 SYN/sec/pod for a 3-replica cluster — but it
/// guarantees DNS is re-resolved every time and we never write
/// into a stale socket. The mutex still serializes per-peer to
/// preserve raft message ordering.
///
/// Future hardening: TCP_KEEPALIVE with aggressive intervals
/// would let us safely reuse cached streams; that needs `socket2`
/// added to the cubestore Cargo manifest, which is a bigger
/// change than this fix is worth. Tracked.
struct PeerConn {
    addr: String,
    write_lock: tokio::sync::Mutex<()>,
}

impl PeerConn {
    fn new(addr: String) -> Self {
        Self {
            addr,
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    async fn send(&self, payload: Vec<u8>) -> std::io::Result<()> {
        // Serialize per-peer so sends preserve raft ordering. The
        // mutex is cheap because each send is a one-shot connect+
        // write+drop; no long-lived state is held.
        let _guard = self.write_lock.lock().await;

        // Connect with a 3s timeout — k8s in-cluster RTT is sub-ms
        // but DNS / pod-startup transients can stretch this. After
        // 3 s we treat the peer as dead and let raft retry on the
        // next heartbeat.
        let mut stream =
            tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&self.addr))
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("connect timeout to {}", self.addr),
                    )
                })??;
        let _ = stream.set_nodelay(true);

        // Bound the write at 2 s. Heartbeats are <1 KiB and SST
        // shipping is gated by raft-rs's max_size_per_msg = 1 MiB,
        // both well under what 2 s of TCP can move on healthy LAN.
        match tokio::time::timeout(Duration::from_secs(2), write_frame(&mut stream, &payload)).await
        {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("write timeout to {}", self.addr),
            )),
        }
    }
}

async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    if payload.len() > FRAME_MAX_LEN as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("raft frame too large: {} bytes", payload.len()),
        ));
    }
    stream.write_u32(FRAME_MAGIC).await?;
    stream.write_u32(FRAME_VERSION).await?;
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Production [`Transport`] over TCP. Maintains one lazily-dialed
/// connection per peer. Send errors are logged and dropped — raft
/// retries via the heartbeat path, so loud re-raise here would just
/// add noise on every blip.
pub struct TcpTransport {
    /// `peer_id → (addr, conn)`. Shared via `Arc<Mutex<>>` so the
    /// transport can be cheaply cloned across the listener and the
    /// dialer paths. Lookups are O(N peers) which is fine for a
    /// 3-7 node deployment.
    peers: Mutex<HashMap<u64, Arc<PeerConn>>>,
}

impl TcpTransport {
    pub fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }

    /// Register or overwrite the address for a peer id. Existing
    /// connection is dropped — the next send dials the new address.
    /// Idempotent; safe to call from a config-reload handler.
    pub fn set_peer(&self, peer_id: u64, addr: String) {
        let mut peers = self.peers.lock().unwrap();
        peers.insert(peer_id, Arc::new(PeerConn::new(addr)));
    }

    /// Remove a peer (e.g. after a ConfChange RemoveNode). Existing
    /// connection is closed. Subsequent sends are dropped silently.
    pub fn remove_peer(&self, peer_id: u64) {
        let mut peers = self.peers.lock().unwrap();
        peers.remove(&peer_id);
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, msg: Message) {
        let target = msg.to;
        // Skip self-targeted messages — raft-rs delivers them via
        // step internally. They appear in outbound only on rare race
        // conditions; treating them as no-ops is correct.
        if target == 0 {
            return;
        }

        let conn = {
            let peers = self.peers.lock().unwrap();
            peers.get(&target).cloned()
        };
        let conn = match conn {
            Some(c) => c,
            None => {
                log::debug!("raft transport: no peer registered for id {}", target);
                return;
            }
        };

        let payload = match msg.write_to_bytes() {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "raft transport: failed to serialize msg to peer {}: {}",
                    target,
                    e
                );
                return;
            }
        };

        if let Err(e) = conn.send(payload).await {
            // Don't escalate — raft retries on its own cadence. Log
            // at debug because the very-first connect after a peer
            // restart is expected to error once.
            log::debug!("raft transport: send to peer {} failed: {}", target, e);
        }
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

    // =========================================================================
    // TcpTransport — round-trip a real raft::Message over localhost TCP.
    // =========================================================================

    use raft::eraftpb::MessageType;

    /// Pick a free port by binding to :0 and reading the assigned addr.
    /// Less brittle than hardcoding a port and racing on parallel test runs.
    async fn ephemeral_addr() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        (listener, addr)
    }

    fn sample_msg(from: u64, to: u64, term: u64) -> Message {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgAppend);
        m.from = from;
        m.to = to;
        m.term = term;
        m.commit = 7;
        m
    }

    #[tokio::test]
    async fn tcp_transport_round_trip_localhost() {
        // Spin up a listener+inbound for "peer 2" on an ephemeral port.
        let (_pre_listener, addr) = ephemeral_addr().await;
        // Drop the pre-bound listener so spawn_listener can rebind the
        // same port. We just used it to discover a free one.
        drop(_pre_listener);

        let (in2, mut rx2) = Inbound::new();
        let _h = spawn_listener(addr.clone(), in2).await.expect("listen");

        // Give the spawn task a tick to actually be listening.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Dialer: register peer 2 at the listener's addr and send.
        let transport = TcpTransport::new();
        transport.set_peer(2, addr);
        let msg = sample_msg(1, 2, 42);
        transport.send(msg.clone()).await;

        // Receiver should observe the same message bytes.
        let received = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        assert_eq!(received.from, 1);
        assert_eq!(received.to, 2);
        assert_eq!(received.term, 42);
        assert_eq!(received.commit, 7);
        assert_eq!(received.msg_type, MessageType::MsgAppend);
    }

    #[tokio::test]
    async fn tcp_transport_drops_unknown_peer_silently() {
        let transport = TcpTransport::new();
        // No peers registered — send is a no-op, no panic, no crash.
        transport.send(sample_msg(1, 99, 1)).await;
    }

    #[tokio::test]
    async fn tcp_transport_recovers_after_peer_restart() {
        // First listener.
        let (_pre, addr) = ephemeral_addr().await;
        drop(_pre);

        let (in2, mut rx2) = Inbound::new();
        let h1 = spawn_listener(addr.clone(), in2.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let transport = TcpTransport::new();
        transport.set_peer(2, addr.clone());

        // Round-trip 1
        transport.send(sample_msg(1, 2, 1)).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
            .await
            .expect("first recv timeout")
            .expect("first recv closed");

        // Simulate peer restart: drop the first listener, spin up a
        // new one on the same port. The first send after the restart
        // hits the broken stream — the transport should detect that,
        // drop the stream, reconnect on the next send, and deliver.
        h1.abort();
        // Wait for the OS to release the port.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _h2 = spawn_listener(addr.clone(), in2).await.expect("rebind");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // First post-restart send may either succeed (kernel still
        // has buffered bytes for the old conn) or silently drop. Try
        // a couple to cover the race deterministically.
        for _ in 0..3 {
            transport.send(sample_msg(1, 2, 2)).await;
            if let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(500), rx2.recv()).await
            {
                return; // recovered
            }
        }
        panic!("transport never recovered after peer restart");
    }

    #[tokio::test]
    async fn tcp_transport_rejects_bad_magic() {
        // Launch a real listener, but write a junk header into it
        // directly. The recv loop should reject and end without
        // poisoning the inbound channel.
        let (_pre, addr) = ephemeral_addr().await;
        drop(_pre);

        let (in_, mut rx) = Inbound::new();
        let _h = spawn_listener(addr.clone(), in_).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut sock = TcpStream::connect(&addr).await.unwrap();
        sock.write_u32(0xDEAD_BEEF).await.unwrap();
        sock.write_u32(1).await.unwrap();
        sock.write_u32(0).await.unwrap();
        // Server should drop this connection. Verify the inbound
        // channel saw nothing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            rx.try_recv().is_err(),
            "inbound must not receive on bad magic"
        );
    }
}
