//! A small, readable implementation of the [Raft](https://raft.github.io/)
//! consensus algorithm - leader election and log replication - with a
//! deterministic simulator so an election, a partition, or a replication round
//! replays identically every run.
//!
//! A node never touches the clock or the network: it is a pure state machine
//! with two entry points, [`RaftNode::tick`] and [`RaftNode::handle`], each
//! returning the messages it wants sent. All time and I/O live in the
//! [`Cluster`] simulator. This mirrors the Python reference implementation
//! module for module.

pub mod cluster;
pub mod log;
pub mod messages;
pub mod node;
pub mod rng;

pub use cluster::{Cluster, ClusterOptions};
pub use log::{Log, LogEntry};
pub use messages::Message;
pub use node::{RaftNode, Role};
pub use rng::Rng;
