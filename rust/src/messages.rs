//! The RPC messages exchanged between nodes. These are plain data; the node
//! returns them from its handlers and the cluster delivers them.

use crate::log::LogEntry;

/// An RPC exchanged between nodes. Raft has four message types; they are modelled
/// as one enum so the cluster can route any message by its common header
/// ([`Message::src`], [`Message::dst`], [`Message::term`]) without knowing the
/// concrete variant.
#[derive(Clone, Debug)]
pub enum Message<C> {
    /// Sent by a candidate to gather votes for an election.
    RequestVote {
        /// Sender id.
        src: usize,
        /// Recipient id.
        dst: usize,
        /// Candidate's term.
        term: u64,
        /// Index of the candidate's last log entry.
        last_log_index: usize,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Answers a [`Message::RequestVote`], granting or denying the vote.
    RequestVoteReply {
        /// Sender id.
        src: usize,
        /// Recipient id.
        dst: usize,
        /// Voter's term.
        term: u64,
        /// Whether the vote was given.
        granted: bool,
    },
    /// Replicates log entries from the leader; a heartbeat when it carries none.
    AppendEntries {
        /// Sender (leader) id.
        src: usize,
        /// Recipient id.
        dst: usize,
        /// Leader's term.
        term: u64,
        /// Index immediately preceding `entries`.
        prev_log_index: usize,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: u64,
        /// New entries to store (empty for a heartbeat).
        entries: Vec<LogEntry<C>>,
        /// Leader's commit index.
        leader_commit: usize,
    },
    /// Answers an [`Message::AppendEntries`] request.
    AppendEntriesReply {
        /// Sender (follower) id.
        src: usize,
        /// Recipient id.
        dst: usize,
        /// Follower's term.
        term: u64,
        /// Whether the entries were accepted.
        success: bool,
        /// On success, the follower's last index, so the leader can advance its
        /// match index for that follower.
        match_index: usize,
    },
}

impl<C> Message<C> {
    /// The id of the node that sent the message.
    pub fn src(&self) -> usize {
        match self {
            Message::RequestVote { src, .. }
            | Message::RequestVoteReply { src, .. }
            | Message::AppendEntries { src, .. }
            | Message::AppendEntriesReply { src, .. } => *src,
        }
    }

    /// The id of the node the message is addressed to.
    pub fn dst(&self) -> usize {
        match self {
            Message::RequestVote { dst, .. }
            | Message::RequestVoteReply { dst, .. }
            | Message::AppendEntries { dst, .. }
            | Message::AppendEntriesReply { dst, .. } => *dst,
        }
    }

    /// The sender's term at the time the message was created.
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. } => *term,
        }
    }
}
