//! A deterministic in-memory cluster that drives time and delivers messages.
//!
//! There are no real timers or sockets. [`Cluster::step`] advances every node one
//! step and routes whatever messages they emit. Message delivery order is stable,
//! and the network can be partitioned or healed, so an entire election or a
//! replication round replays identically every run - which is what makes the
//! tests reliable. With the fault knobs off, [`Cluster::new`] is fully
//! deterministic; a seed enables dropping, duplicating, and reordering messages.

use std::collections::HashSet;

use crate::messages::Message;
use crate::node::{RaftNode, Role};
use crate::rng::Rng;

/// Configuration for a [`Cluster`]. Use [`ClusterOptions::default`] for the
/// common case. A `seed` of `None` keeps the cluster fully deterministic; a
/// `Some` seed enables the fault knobs and randomizes election timeouts in
/// `[T, 2T-1]` like the paper recommends.
pub struct ClusterOptions {
    /// Base election timeout `T`.
    pub election_timeout: usize,
    /// Heartbeat interval.
    pub heartbeat_interval: usize,
    /// Seed for fault injection, or `None` for a deterministic cluster.
    pub seed: Option<u64>,
    /// Probability of dropping a message in flight.
    pub drop_prob: f64,
    /// Probability of delivering a message twice.
    pub duplicate_prob: f64,
    /// Whether to shuffle the pending queue each step.
    pub reorder: bool,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            election_timeout: 10,
            heartbeat_interval: 3,
            seed: None,
            drop_prob: 0.0,
            duplicate_prob: 0.0,
            reorder: false,
        }
    }
}

/// A deterministic in-memory cluster of Raft nodes.
pub struct Cluster<C> {
    nodes: Vec<RaftNode<C>>,
    /// The number of steps executed.
    pub time: usize,
    /// The number of messages the network threw away.
    pub dropped: usize,

    rng: Option<Rng>,
    drop_prob: f64,
    duplicate_prob: f64,
    reorder: bool,

    inbox: Vec<Message<C>>,
    partitioned: HashSet<(usize, usize)>,
}

impl<C: Clone> Cluster<C> {
    /// Builds a cluster of the given size with default timers and no faults,
    /// matching the reference implementation's plain `Cluster(n)`.
    pub fn new(size: usize) -> Self {
        Cluster::with_options(size, ClusterOptions::default())
    }

    /// Builds a cluster with explicit options.
    pub fn with_options(size: usize, opts: ClusterOptions) -> Self {
        let mut rng = opts.seed.map(Rng::new);
        let mut nodes = Vec::with_capacity(size);
        let ids: Vec<usize> = (0..size).collect();
        // Stagger election timeouts by node id so the cluster doesn't split the
        // vote forever; a deterministic per-node offset keeps the simulation
        // reproducible. With a seed we randomize timeouts in [T, 2T-1] instead.
        for i in 0..size {
            let timeout = match rng.as_mut() {
                Some(r) => opts.election_timeout + r.below(opts.election_timeout),
                None => opts.election_timeout + i * 2,
            };
            nodes.push(RaftNode::new(i, &ids, timeout, opts.heartbeat_interval));
        }
        Cluster {
            nodes,
            time: 0,
            dropped: 0,
            rng,
            drop_prob: opts.drop_prob,
            duplicate_prob: opts.duplicate_prob,
            reorder: opts.reorder,
            inbox: Vec::new(),
            partitioned: HashSet::new(),
        }
    }

    // ---- network control ----

    fn key(a: usize, b: usize) -> (usize, usize) {
        if a < b { (a, b) } else { (b, a) }
    }

    /// Cuts the link between `a` and `b` in both directions.
    pub fn partition(&mut self, a: usize, b: usize) {
        self.partitioned.insert(Self::key(a, b));
    }

    /// Restores the link between `a` and `b`.
    pub fn heal(&mut self, a: usize, b: usize) {
        self.partitioned.remove(&Self::key(a, b));
    }

    /// Restores every cut link.
    pub fn heal_all(&mut self) {
        self.partitioned.clear();
    }

    /// Cuts every link to the given node.
    pub fn isolate(&mut self, node: usize) {
        for other in 0..self.nodes.len() {
            if other != node {
                self.partition(node, other);
            }
        }
    }

    fn reachable(&self, src: usize, dst: usize) -> bool {
        !self.partitioned.contains(&Self::key(src, dst))
    }

    // ---- driving time ----

    /// Advances one tick: delivers queued messages (optionally dropping,
    /// duplicating, or reordering them under a seed) and then ticks every node.
    pub fn step(&mut self) {
        self.time += 1;
        let drop_prob = self.drop_prob;
        let duplicate_prob = self.duplicate_prob;

        let mut pending = std::mem::take(&mut self.inbox);
        if self.reorder
            && pending.len() > 1
            && let Some(rng) = self.rng.as_mut()
        {
            rng.shuffle(&mut pending);
        }

        for msg in &pending {
            let dst = msg.dst();
            if !(self.reachable(msg.src(), dst) && dst < self.nodes.len()) {
                continue;
            }
            let dropped = self
                .rng
                .as_mut()
                .is_some_and(|rng| drop_prob > 0.0 && rng.next_f64() < drop_prob);
            if dropped {
                self.dropped += 1;
                continue;
            }
            let duplicated = self
                .rng
                .as_mut()
                .is_some_and(|rng| duplicate_prob > 0.0 && rng.next_f64() < duplicate_prob);
            let deliveries = if duplicated { 2 } else { 1 };
            for _ in 0..deliveries {
                let replies = self.nodes[dst].handle(msg);
                self.enqueue(replies);
            }
        }

        let ticks: Vec<Vec<Message<C>>> = self.nodes.iter_mut().map(RaftNode::tick).collect();
        for replies in ticks {
            self.enqueue(replies);
        }
    }

    /// Advances the cluster by the given number of steps.
    pub fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// Steps until a unique leader emerges or the step budget is exhausted,
    /// returning the leader's id or `None`. The budget defaults to 200 steps
    /// when `max_steps` is `None`.
    pub fn run_until_leader(&mut self, max_steps: Option<usize>) -> Option<usize> {
        let steps = max_steps.unwrap_or(200);
        for _ in 0..steps {
            self.step();
            if let Some(leader) = self.leader_id() {
                return Some(leader);
            }
        }
        None
    }

    fn enqueue(&mut self, messages: Vec<Message<C>>) {
        for m in messages {
            if self.reachable(m.src(), m.dst()) {
                self.inbox.push(m);
            }
        }
    }

    // ---- inspection ----

    /// The nodes, indexed by id.
    pub fn nodes(&self) -> &[RaftNode<C>] {
        &self.nodes
    }

    /// The node with the given id.
    pub fn node(&self, id: usize) -> &RaftNode<C> {
        &self.nodes[id]
    }

    /// The node with the given id, mutably (for proposing commands).
    pub fn node_mut(&mut self, id: usize) -> &mut RaftNode<C> {
        &mut self.nodes[id]
    }

    /// The unique current-term leader's id, or `None` if there is a split or no
    /// leader.
    pub fn leader_id(&self) -> Option<usize> {
        let leaders: Vec<&RaftNode<C>> = self
            .nodes
            .iter()
            .filter(|n| n.role == Role::Leader)
            .collect();
        if leaders.is_empty() {
            return None;
        }
        let top_term = leaders.iter().map(|n| n.current_term).max().unwrap_or(0);
        let top: Vec<&RaftNode<C>> = leaders
            .into_iter()
            .filter(|n| n.current_term == top_term)
            .collect();
        if top.len() == 1 {
            Some(top[0].id)
        } else {
            None
        }
    }

    /// The ids of every node that currently believes it is the leader.
    pub fn leaders(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .filter(|n| n.role == Role::Leader)
            .map(|n| n.id)
            .collect()
    }

    /// The commands applied on the given node, in order.
    pub fn committed_commands(&self, node_id: usize) -> Vec<C> {
        self.nodes[node_id]
            .applied
            .iter()
            .map(|e| e.command.clone())
            .collect()
    }
}
