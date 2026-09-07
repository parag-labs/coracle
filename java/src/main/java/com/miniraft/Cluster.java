// A deterministic in-memory cluster that drives time and delivers messages.
//
// There are no real timers or sockets. step() advances every node one step and
// routes whatever messages they emit. Message delivery order is stable, and the
// network can be partitioned or healed, so an entire election or a replication
// round replays identically every run - which is what makes the tests reliable.

package com.miniraft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class Cluster {

    private final Map<Integer, RaftNode> nodes = new LinkedHashMap<>();
    private int time = 0;
    private int dropped = 0;

    private final Random rng;
    private final double dropProb;
    private final double duplicateProb;
    private final boolean reorder;

    private final Deque<Message> inbox = new ArrayDeque<>();
    // A pair (a, b) in here means the link a<->b is cut in both directions.
    private final Set<Long> partitioned = new HashSet<>();

    public Cluster(int size, int electionTimeout, int heartbeatInterval,
                   Long seed, double dropProb, double duplicateProb, boolean reorder) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < size; i++) ids.add(i);
        this.rng = seed != null ? new Random(seed) : null;
        // Stagger election timeouts by node id. Real Raft randomizes these so the
        // cluster doesn't split the vote forever; a deterministic per-node offset
        // gives the same effect while keeping the simulation reproducible. When a
        // seed is given we randomize timeouts in [T, 2T) like the paper recommends.
        for (int i : ids) {
            int timeout = rng != null
                    ? electionTimeout + rng.nextInt(electionTimeout) // [T, 2T-1]
                    : electionTimeout + i * 2;
            nodes.put(i, new RaftNode(i, ids, timeout, heartbeatInterval));
        }
        this.dropProb = dropProb;
        this.duplicateProb = duplicateProb;
        this.reorder = reorder;
    }

    public Cluster(int size) {
        this(size, 10, 3, null, 0.0, 0.0, false);
    }

    public Cluster(int size, int electionTimeout, int heartbeatInterval) {
        this(size, electionTimeout, heartbeatInterval, null, 0.0, 0.0, false);
    }

    // ---- network control ----

    private static long key(int a, int b) {
        int lo = Math.min(a, b), hi = Math.max(a, b);
        return ((long) lo << 32) | (hi & 0xffffffffL);
    }

    public void partition(int a, int b) { partitioned.add(key(a, b)); }
    public void heal(int a, int b) { partitioned.remove(key(a, b)); }
    public void healAll() { partitioned.clear(); }

    public void isolate(int node) {
        for (int other : nodes.keySet()) if (other != node) partition(node, other);
    }

    private boolean reachable(int src, int dst) {
        return !partitioned.contains(key(src, dst));
    }

    // ---- driving time ----

    public void step() {
        time++;

        List<Message> pending = new ArrayList<>(inbox);
        inbox.clear();

        if (reorder && rng != null && pending.size() > 1) {
            Collections.shuffle(pending, rng);
        }

        for (Message msg : pending) {
            if (!(reachable(msg.src(), msg.dst()) && nodes.containsKey(msg.dst()))) continue;
            if (rng != null && dropProb > 0 && rng.nextDouble() < dropProb) {
                dropped++;
                continue;
            }
            int deliveries = 1;
            if (rng != null && duplicateProb > 0 && rng.nextDouble() < duplicateProb) {
                deliveries = 2; // deliver the same message twice
            }
            for (int d = 0; d < deliveries; d++) {
                enqueue(nodes.get(msg.dst()).handle(msg));
            }
        }

        for (RaftNode node : nodes.values()) {
            enqueue(node.tick());
        }
    }

    public void run(int steps) {
        for (int i = 0; i < steps; i++) step();
    }

    public RaftNode runUntilLeader(int maxSteps) {
        for (int i = 0; i < maxSteps; i++) {
            step();
            RaftNode leader = leader();
            if (leader != null) return leader;
        }
        return null;
    }

    public RaftNode runUntilLeader() {
        return runUntilLeader(200);
    }

    private void enqueue(List<Message> messages) {
        for (Message m : messages) if (reachable(m.src(), m.dst())) inbox.add(m);
    }

    // ---- inspection ----

    public Map<Integer, RaftNode> nodes() { return nodes; }
    public int time() { return time; }
    public int dropped() { return dropped; }

    /** The unique current-term leader, or null if there's a split/no leader. */
    public RaftNode leader() {
        List<RaftNode> leaders = leaders();
        if (leaders.isEmpty()) return null;
        int topTerm = leaders.stream().mapToInt(RaftNode::currentTerm).max().orElse(0);
        List<RaftNode> top = new ArrayList<>();
        for (RaftNode n : leaders) if (n.currentTerm() == topTerm) top.add(n);
        return top.size() == 1 ? top.get(0) : null;
    }

    public List<RaftNode> leaders() {
        List<RaftNode> out = new ArrayList<>();
        for (RaftNode n : nodes.values()) if (n.role() == RaftNode.Role.LEADER) out.add(n);
        return out;
    }

    public List<Object> committedCommands(int nodeId) {
        List<Object> out = new ArrayList<>();
        for (Log.Entry e : nodes.get(nodeId).applied()) out.add(e.command());
        return out;
    }
}
