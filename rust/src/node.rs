//! A single Raft node.
//!
//! The node is a deterministic state machine. It never reads a clock or sends on
//! a socket itself - it only reacts: [`RaftNode::tick`] advances its internal
//! timers and may start an election or emit heartbeats, and
//! [`RaftNode::handle`] processes one inbound RPC. Both return the messages to
//! send. Keeping I/O out of the node is what lets the simulator replay elections
//! and log replication deterministically.

use std::collections::{HashMap, HashSet};

use crate::log::{Log, LogEntry};
use crate::messages::Message;

/// The current role of a node in the Raft protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passively replicates entries and grants votes.
    Follower,
    /// Standing for election.
    Candidate,
    /// Replicating log entries to the followers.
    Leader,
}

/// A single deterministic Raft state machine.
#[derive(Debug)]
pub struct RaftNode<C> {
    /// This node's identifier.
    pub id: usize,
    /// The other node ids in the cluster (this node's id excluded).
    pub peers: Vec<usize>,
    /// Ticks without contact that trigger an election.
    pub election_timeout: usize,
    /// How often a leader broadcasts heartbeats.
    pub heartbeat_interval: usize,

    /// The latest term the node has seen.
    pub current_term: u64,
    /// The candidate voted for in the current term, if any.
    pub voted_for: Option<usize>,
    /// The node's replicated log.
    pub log: Log<C>,

    /// The node's current role.
    pub role: Role,
    /// The highest index known to be committed.
    pub commit_index: usize,
    /// The highest index handed to the state machine.
    pub last_applied: usize,
    /// The ordered list of entries applied to the state machine.
    pub applied: Vec<LogEntry<C>>,

    /// Per-follower next index to send (leader state).
    pub next_index: HashMap<usize, usize>,
    /// Per-node highest replicated index (leader state).
    pub match_index: HashMap<usize, usize>,

    votes: HashSet<usize>,
    election_elapsed: usize,
    heartbeat_elapsed: usize,
}

impl<C: Clone> RaftNode<C> {
    /// Creates a follower with the given id, peer ids (which may include its own
    /// id, which is filtered out), and timers.
    pub fn new(
        id: usize,
        peers: &[usize],
        election_timeout: usize,
        heartbeat_interval: usize,
    ) -> Self {
        let peers = peers.iter().copied().filter(|&p| p != id).collect();
        RaftNode {
            id,
            peers,
            election_timeout,
            heartbeat_interval,
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            applied: Vec::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
        }
    }

    // ---- driving the node ----

    /// Advances the node's timers by one step. A leader may emit heartbeats; a
    /// follower or candidate may start an election. Returns any messages to send.
    pub fn tick(&mut self) -> Vec<Message<C>> {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                return self.broadcast_append();
            }
            return Vec::new();
        }

        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            return self.start_election();
        }
        Vec::new()
    }

    /// Processes one inbound RPC and returns the replies to send.
    pub fn handle(&mut self, msg: &Message<C>) -> Vec<Message<C>> {
        // Any message from a higher term makes us a follower of that term.
        if msg.term() > self.current_term {
            self.become_follower(msg.term());
        }

        match msg {
            Message::RequestVote {
                src,
                term,
                last_log_index,
                last_log_term,
                ..
            } => vec![self.on_request_vote(*src, *term, *last_log_index, *last_log_term)],
            Message::RequestVoteReply {
                src, term, granted, ..
            } => self.on_request_vote_reply(*src, *term, *granted),
            Message::AppendEntries {
                src,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                ..
            } => vec![self.on_append_entries(
                *src,
                *term,
                *prev_log_index,
                *prev_log_term,
                entries,
                *leader_commit,
            )],
            Message::AppendEntriesReply {
                src,
                term,
                success,
                match_index,
                ..
            } => self.on_append_entries_reply(*src, *term, *success, *match_index),
        }
    }

    /// Client entry point. Only a leader accepts a proposal; it appends to its
    /// own log and lets the next heartbeat replicate it. Returns whether the
    /// proposal was accepted.
    pub fn propose(&mut self, command: C) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        self.log.append(LogEntry::new(self.current_term, command));
        self.match_index.insert(self.id, self.log.last_index());
        true
    }

    // ---- role transitions ----

    fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.election_elapsed = 0;
        self.votes = HashSet::new();
    }

    fn become_candidate(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes = HashSet::from([self.id]);
        self.election_elapsed = 0;
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.heartbeat_elapsed = 0;
        let last = self.log.last_index();
        self.next_index = HashMap::new();
        self.match_index = HashMap::new();
        for &p in &self.peers {
            self.next_index.insert(p, last + 1);
            self.match_index.insert(p, 0);
        }
        self.match_index.insert(self.id, last);
    }

    // ---- elections ----

    fn start_election(&mut self) -> Vec<Message<C>> {
        self.become_candidate();
        if self.peers.is_empty() {
            // single-node cluster: instant win
            self.become_leader();
            return Vec::new();
        }
        self.peers
            .iter()
            .map(|&p| Message::RequestVote {
                src: self.id,
                dst: p,
                term: self.current_term,
                last_log_index: self.log.last_index(),
                last_log_term: self.log.last_term(),
            })
            .collect()
    }

    fn on_request_vote(
        &mut self,
        src: usize,
        term: u64,
        last_log_index: usize,
        last_log_term: u64,
    ) -> Message<C> {
        let mut granted = false;
        if term >= self.current_term {
            let free = self.voted_for.is_none() || self.voted_for == Some(src);
            let current = self.log.is_up_to_date(last_log_index, last_log_term);
            if free && current {
                granted = true;
                self.voted_for = Some(src);
                self.election_elapsed = 0;
            }
        }
        Message::RequestVoteReply {
            src: self.id,
            dst: src,
            term: self.current_term,
            granted,
        }
    }

    fn on_request_vote_reply(&mut self, src: usize, term: u64, granted: bool) -> Vec<Message<C>> {
        if self.role != Role::Candidate || term != self.current_term {
            return Vec::new();
        }
        if granted {
            self.votes.insert(src);
            if self.votes.len() >= self.majority() {
                self.become_leader();
                return self.broadcast_append();
            }
        }
        Vec::new()
    }

    // ---- log replication ----

    fn broadcast_append(&self) -> Vec<Message<C>> {
        self.peers.iter().map(|&p| self.append_for(p)).collect()
    }

    fn append_for(&self, peer: usize) -> Message<C> {
        let next_idx = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.log.last_index() + 1);
        let prev_index = next_idx - 1;
        Message::AppendEntries {
            src: self.id,
            dst: peer,
            term: self.current_term,
            prev_log_index: prev_index,
            prev_log_term: self.log.term_at(prev_index),
            entries: self.log.slice_from(next_idx),
            leader_commit: self.commit_index,
        }
    }

    fn on_append_entries(
        &mut self,
        src: usize,
        term: u64,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: &[LogEntry<C>],
        leader_commit: usize,
    ) -> Message<C> {
        if term < self.current_term {
            return Message::AppendEntriesReply {
                src: self.id,
                dst: src,
                term: self.current_term,
                success: false,
                match_index: 0,
            }; // stale leader
        }

        // Valid current-term leader: defer to it and reset our election timer.
        self.role = Role::Follower;
        self.election_elapsed = 0;

        if !self.log.matches(prev_log_index, prev_log_term) {
            return Message::AppendEntriesReply {
                src: self.id,
                dst: src,
                term: self.current_term,
                success: false,
                match_index: 0,
            }; // log gap/conflict at prev; leader will back off
        }

        self.log.truncate_and_append(prev_log_index, entries);
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.log.last_index());
            self.apply_committed();
        }

        Message::AppendEntriesReply {
            src: self.id,
            dst: src,
            term: self.current_term,
            success: true,
            match_index: prev_log_index + entries.len(),
        }
    }

    fn on_append_entries_reply(
        &mut self,
        src: usize,
        term: u64,
        success: bool,
        match_index: usize,
    ) -> Vec<Message<C>> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        if success {
            self.match_index.insert(src, match_index);
            self.next_index.insert(src, match_index + 1);
            self.advance_commit();
            return Vec::new();
        }
        // Rejected: step next_index back and retry.
        let cur = self.next_index.get(&src).copied().unwrap_or(1);
        self.next_index.insert(src, cur.saturating_sub(1).max(1));
        vec![self.append_for(src)]
    }

    /// Commits index N when a majority stores it AND it is from the current term
    /// (the paper's safety rule against committing stale entries).
    fn advance_commit(&mut self) {
        let mut idx = self.log.last_index();
        while idx > self.commit_index {
            if self.log.term_at(idx) == self.current_term {
                let replicas = 1 + self
                    .peers
                    .iter()
                    .filter(|&&p| self.match_index.get(&p).copied().unwrap_or(0) >= idx)
                    .count();
                if replicas >= self.majority() {
                    self.commit_index = idx;
                    self.apply_committed();
                    break;
                }
            }
            idx -= 1;
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = self.log.get(self.last_applied).clone();
            self.applied.push(entry);
        }
    }

    fn majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }
}
