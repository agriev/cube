//! Raft transport — peer-to-peer message shipping over `cuberpc`.
//!
//! ## Status
//!
//! M1 stub. Concrete impl in M4 (clustering / leader election).
//!
//! ## Design notes (intent)
//!
//! - We piggyback on the existing `cuberpc` infrastructure rather than
//!   adding a second RPC stack. New service trait `RaftTransport`
//!   exposes one method: `step(messages: Vec<raft::Message>)`.
//! - Messages are batched per peer per tick to coalesce log-replication
//!   traffic. Heartbeats stay separate (small, frequent).
//! - New port `CUBESTORE_RAFT_PORT` (default 9100). Distinct from the
//!   metadata port (9999) so the existing client connections don't
//!   compete with replication traffic.
//! - Peer discovery: bootstrapped from the `CUBESTORE_RAFT_PEERS` env
//!   var (`<id>@<host>:<port>` triples), runtime add/remove via a
//!   ConfChange RPC documented in the operator runbook.
//!
//! ## Wire format
//!
//! `raft::Message` already has `prost`-derived `prost::Message` impls
//! when the `prost-codec` feature is enabled. We use `protobuf-codec`
//! by default (see `Cargo.toml`) to avoid the prost 0.11 / 0.13 axis;
//! protobuf-rs serialization is just as fine for our throughput target.

#[allow(dead_code)]
pub struct RaftTransport {
    _placeholder: (),
}
