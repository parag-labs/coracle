// A single Raft node.
//
// The node is a deterministic state machine. It never reads a clock or sends on a
// socket itself - it only reacts:
//   - tick() advances its internal timers and may start an election or emit
//     heartbeats, returning the messages to send.
//   - handle(message) processes one inbound RPC and returns the replies to send.
//
// Keeping I/O out of the node is what lets the simulator replay elections and log
// replication deterministically, and lets tests assert exact behavior.

package com.miniraft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public final int id;
    public final List<Integer> peers;
    public final int electionTimeout;
    public final int heartbeatInterval;

    // Persistent state.
    private int currentTerm = 0;
    private Integer votedFor = null;
    private final Log log = new Log();

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private int commitIndex = 0;
    private int lastApplied = 0;
    private final List<Log.Entry> applied = new ArrayList<>(); // entries handed to the state machine

    // Leader state (reset on election).
    private final Map<Integer, Integer> nextIndex = new HashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();

    // Election bookkeeping.
    private final Set<Integer> votes = new HashSet<>();
    private int electionElapsed = 0;
    private int heartbeatElapsed = 0;

    public RaftNode(int nodeId, List<Integer> peerIds, int electionTimeout, int heartbeatInterval) {
        this.id = nodeId;
        this.peers = new ArrayList<>();
        for (int p : peerIds) if (p != nodeId) this.peers.add(p);
        this.electionTimeout = electionTimeout;
        this.heartbeatInterval = heartbeatInterval;
    }

    public RaftNode(int nodeId, List<Integer> peerIds) {
        this(nodeId, peerIds, 10, 3);
    }

    // ---- accessors used by the cluster / tests ----
    public int currentTerm() { return currentTerm; }
    public Role role() { return role; }
    public int commitIndex() { return commitIndex; }
    public Log log() { return log; }
    public List<Log.Entry> applied() { return applied; }

    // ---- driving the node ----

    public List<Message> tick() {
        if (role == Role.LEADER) {
            heartbeatElapsed++;
            if (heartbeatElapsed >= heartbeatInterval) {
                heartbeatElapsed = 0;
                return broadcastAppend();
            }
            return new ArrayList<>();
        }
        electionElapsed++;
        if (electionElapsed >= electionTimeout) return startElection();
        return new ArrayList<>();
    }

    public List<Message> handle(Message msg) {
        // Any message from a higher term makes us a follower of that term.
        if (msg.term() > currentTerm) becomeFollower(msg.term());

        List<Message> out = new ArrayList<>();
        if (msg instanceof Message.RequestVote rv) {
            out.add(onRequestVote(rv));
        } else if (msg instanceof Message.RequestVoteReply rvr) {
            out.addAll(onRequestVoteReply(rvr));
        } else if (msg instanceof Message.AppendEntries ae) {
            out.add(onAppendEntries(ae));
        } else if (msg instanceof Message.AppendEntriesReply aer) {
            out.addAll(onAppendEntriesReply(aer));
        }
        return out;
    }

    /** Client entry point. Only a leader accepts a proposal; it appends to its own
     * log and lets the next heartbeat replicate it. */
    public boolean propose(Object command) {
        if (role != Role.LEADER) return false;
        log.append(new Log.Entry(currentTerm, command));
        matchIndex.put(id, log.lastIndex());
        return true;
    }

    // ---- role transitions ----

    private void becomeFollower(int term) {
        currentTerm = term;
        votedFor = null;
        role = Role.FOLLOWER;
        electionElapsed = 0;
        votes.clear();
    }

    private void becomeCandidate() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = id;
        votes.clear();
        votes.add(id);
        electionElapsed = 0;
    }

    private void becomeLeader() {
        role = Role.LEADER;
        heartbeatElapsed = 0;
        int last = log.lastIndex();
        nextIndex.clear();
        matchIndex.clear();
        for (int p : peers) {
            nextIndex.put(p, last + 1);
            matchIndex.put(p, 0);
        }
        matchIndex.put(id, last);
    }

    // ---- elections ----

    private List<Message> startElection() {
        becomeCandidate();
        if (peers.isEmpty()) { // single-node cluster: instant win
            becomeLeader();
            return new ArrayList<>();
        }
        List<Message> out = new ArrayList<>();
        for (int p : peers) {
            out.add(new Message.RequestVote(id, p, currentTerm, log.lastIndex(), log.lastTerm()));
        }
        return out;
    }

    private Message.RequestVoteReply onRequestVote(Message.RequestVote msg) {
        boolean granted = false;
        if (msg.term() >= currentTerm) {
            boolean free = votedFor == null || votedFor == msg.src();
            boolean current = log.isUpToDate(msg.lastLogIndex(), msg.lastLogTerm());
            if (free && current) {
                granted = true;
                votedFor = msg.src();
                electionElapsed = 0;
            }
        }
        return new Message.RequestVoteReply(id, msg.src(), currentTerm, granted);
    }

    private List<Message> onRequestVoteReply(Message.RequestVoteReply msg) {
        if (role != Role.CANDIDATE || msg.term() != currentTerm) return new ArrayList<>();
        if (msg.granted()) {
            votes.add(msg.src());
            if (votes.size() >= majority()) {
                becomeLeader();
                return broadcastAppend();
            }
        }
        return new ArrayList<>();
    }

    // ---- log replication ----

    private List<Message> broadcastAppend() {
        List<Message> out = new ArrayList<>();
        for (int p : peers) out.add(appendFor(p));
        return out;
    }

    private Message.AppendEntries appendFor(int peer) {
        int nextIdx = nextIndex.getOrDefault(peer, log.lastIndex() + 1);
        int prevIndex = nextIdx - 1;
        return new Message.AppendEntries(
                id, peer, currentTerm, prevIndex, log.termAt(prevIndex),
                log.sliceFrom(nextIdx), commitIndex);
    }

    private Message.AppendEntriesReply onAppendEntries(Message.AppendEntries msg) {
        if (msg.term() < currentTerm) {
            return new Message.AppendEntriesReply(id, msg.src(), currentTerm, false, 0); // stale leader
        }

        // Valid current-term leader: defer to it and reset our election timer.
        role = Role.FOLLOWER;
        electionElapsed = 0;

        if (!log.matches(msg.prevLogIndex(), msg.prevLogTerm())) {
            return new Message.AppendEntriesReply(id, msg.src(), currentTerm, false, 0); // gap/conflict
        }

        log.truncateAndAppend(msg.prevLogIndex(), msg.entries());
        if (msg.leaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.leaderCommit(), log.lastIndex());
            applyCommitted();
        }

        int matched = msg.prevLogIndex() + msg.entries().size();
        return new Message.AppendEntriesReply(id, msg.src(), currentTerm, true, matched);
    }

    private List<Message> onAppendEntriesReply(Message.AppendEntriesReply msg) {
        if (role != Role.LEADER || msg.term() != currentTerm) return new ArrayList<>();
        if (msg.success()) {
            matchIndex.put(msg.src(), msg.matchIndex());
            nextIndex.put(msg.src(), msg.matchIndex() + 1);
            advanceCommit();
            return new ArrayList<>();
        }
        // Rejected: step next_index back and retry.
        int cur = nextIndex.getOrDefault(msg.src(), 1);
        nextIndex.put(msg.src(), Math.max(1, cur - 1));
        List<Message> out = new ArrayList<>();
        out.add(appendFor(msg.src()));
        return out;
    }

    /** A leader commits index N when a majority has it AND it's from the current
     * term (the paper's safety rule against committing stale entries). */
    private void advanceCommit() {
        for (int n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) continue;
            int replicas = 1;
            for (int p : peers) if (matchIndex.getOrDefault(p, 0) >= n) replicas++;
            if (replicas >= majority()) {
                commitIndex = n;
                applyCommitted();
                break;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            applied.add(log.get(lastApplied));
        }
    }

    private int majority() {
        return (peers.size() + 1) / 2 + 1;
    }
}
