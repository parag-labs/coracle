package com.miniraft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

// Log mechanics: matching, conflict truncation, and the election restriction.
class LogTest {

    @Test
    void appendAndIndicesAreOneBased() {
        Log log = new Log();
        assertEquals(0, log.lastIndex());
        log.append(new Log.Entry(1, "a"));
        log.append(new Log.Entry(1, "b"));
        assertEquals(2, log.lastIndex());
        assertEquals("a", log.get(1).command());
        assertEquals(1, log.termAt(2));
    }

    @Test
    void indexZeroAlwaysMatches() {
        assertTrue(new Log().matches(0, 0));
    }

    @Test
    void matchesRequiresSameTerm() {
        Log log = new Log();
        log.append(new Log.Entry(2, "x"));
        assertTrue(log.matches(1, 2));
        assertFalse(log.matches(1, 3));
    }

    @Test
    void truncateDropsConflictingSuffix() {
        Log log = new Log();
        log.append(new Log.Entry(1, "a"));
        log.append(new Log.Entry(1, "b"));
        log.append(new Log.Entry(1, "c"));
        log.truncateAndAppend(1, List.of(new Log.Entry(2, "B"), new Log.Entry(2, "C")));
        List<Object> cmds = new ArrayList<>();
        for (Log.Entry e : log.entries()) cmds.add(e.command());
        assertEquals(List.of("a", "B", "C"), cmds);
        assertEquals(2, log.termAt(2));
    }

    @Test
    void truncateKeepsMatchingPrefixIdempotent() {
        Log log = new Log();
        log.append(new Log.Entry(1, "a"));
        log.append(new Log.Entry(1, "b"));
        log.truncateAndAppend(0, List.of(new Log.Entry(1, "a"), new Log.Entry(1, "b")));
        List<Object> cmds = new ArrayList<>();
        for (Log.Entry e : log.entries()) cmds.add(e.command());
        assertEquals(List.of("a", "b"), cmds);
    }

    @Test
    void upToDatePrefersHigherTermThenLongerLog() {
        Log log = new Log();
        log.append(new Log.Entry(1, "a"));
        log.append(new Log.Entry(2, "b"));
        assertTrue(log.isUpToDate(1, 3));
        assertTrue(log.isUpToDate(2, 2));
        assertFalse(log.isUpToDate(1, 2));
    }
}

// Leader election.
class ElectionTest {

    @Test
    void singleNodeElectsItself() {
        Cluster c = new Cluster(1);
        RaftNode leader = c.runUntilLeader();
        assertNotNull(leader);
        assertEquals(0, leader.id);
    }

    @Test
    void clusterConvergesOnOneLeader() {
        Cluster c = new Cluster(3);
        RaftNode leader = c.runUntilLeader();
        assertNotNull(leader);
        assertEquals(1, c.leaders().size());
        for (RaftNode node : c.nodes().values()) {
            assertEquals(leader.currentTerm(), node.currentTerm());
        }
    }

    @Test
    void fiveNodeClusterAlsoConverges() {
        Cluster c = new Cluster(5);
        assertNotNull(c.runUntilLeader());
        assertEquals(1, c.leaders().size());
    }

    @Test
    void newLeaderEmergesAfterTheLeaderIsIsolated() {
        Cluster c = new Cluster(5);
        RaftNode first = c.runUntilLeader();
        assertNotNull(first);

        c.isolate(first.id);
        RaftNode newLeader = null;
        for (int i = 0; i < 300; i++) {
            c.step();
            for (RaftNode n : c.leaders()) {
                if (n.id != first.id) { newLeader = n; break; }
            }
            if (newLeader != null) break;
        }
        assertNotNull(newLeader);
        assertNotEquals(first.id, newLeader.id);
        assertTrue(newLeader.currentTerm() > first.currentTerm());
    }

    @Test
    void termIncrementsOnElectionTimeout() {
        Cluster c = new Cluster(3);
        c.runUntilLeader();
        assertTrue(c.leader().currentTerm() >= 1);
    }

    @Test
    void staleCandidateIsDenied() {
        RaftNode voter = new RaftNode(0, List.of(0, 1));
        voter.log().append(new Log.Entry(5, "committed"));

        Message.RequestVote stale = new Message.RequestVote(1, 0, 6, 0, 0);
        Message.RequestVoteReply reply = (Message.RequestVoteReply) voter.handle(stale).get(0);
        assertFalse(reply.granted());
        assertEquals(RaftNode.Role.FOLLOWER, voter.role());
    }
}

// Log replication and commit safety.
class ReplicationTest {

    private static RaftNode elect(Cluster c) {
        RaftNode leader = c.runUntilLeader();
        assertNotNull(leader);
        return leader;
    }

    @Test
    void proposalReplicatesAndAppliesInOrder() {
        Cluster c = new Cluster(3);
        RaftNode leader = elect(c);
        assertTrue(leader.propose("x=1"));
        assertTrue(leader.propose("x=2"));
        c.run(40);
        for (int nodeId : c.nodes().keySet()) {
            assertEquals(List.of("x=1", "x=2"), c.committedCommands(nodeId));
        }
    }

    @Test
    void followerRejectsClientProposals() {
        Cluster c = new Cluster(3);
        RaftNode leader = elect(c);
        RaftNode follower = null;
        for (RaftNode n : c.nodes().values()) if (n.id != leader.id) { follower = n; break; }
        assertNotNull(follower);
        assertFalse(follower.propose("nope"));
    }

    @Test
    void commitIndexReachesAMajority() {
        Cluster c = new Cluster(5);
        RaftNode leader = elect(c);
        leader.propose("a");
        c.run(50);
        int committedOn = 0;
        for (int nid : c.nodes().keySet()) {
            if (c.committedCommands(nid).equals(List.of("a"))) committedOn++;
        }
        assertTrue(committedOn >= 3);
    }

    @Test
    void laggingFollowerCatchesUpAfterHealing() {
        Cluster c = new Cluster(3, 10, 2);
        RaftNode leader = elect(c);
        RaftNode lagging = null;
        for (RaftNode n : c.nodes().values()) if (n.id != leader.id) { lagging = n; break; }
        assertNotNull(lagging);

        c.isolate(lagging.id);
        for (String cmd : List.of("c1", "c2", "c3")) leader.propose(cmd);
        c.run(60);
        assertTrue(c.committedCommands(lagging.id).isEmpty());

        c.healAll();
        c.run(80);
        assertEquals(List.of("c1", "c2", "c3"), c.committedCommands(lagging.id));
    }

    @Test
    void minorityPartitionCannotCommit() {
        Cluster c = new Cluster(5);
        RaftNode leader = elect(c);
        List<Integer> others = new ArrayList<>();
        for (RaftNode n : c.nodes().values()) if (n.id != leader.id) others.add(n.id);
        for (int i = 1; i < others.size(); i++) c.partition(leader.id, others.get(i));

        leader.propose("lonely");
        c.run(60);
        for (int nid : c.nodes().keySet()) {
            assertFalse(c.committedCommands(nid).contains("lonely"));
        }
    }
}

// Chaos suite: safety invariants under a lossy, out-of-order, partitioned network.
class ChaosTest {

    private static void checkElectionSafety(Cluster cluster) {
        Map<Integer, Integer> byTerm = new HashMap<>();
        for (RaftNode node : cluster.nodes().values()) {
            if (node.role() == RaftNode.Role.LEADER) {
                byTerm.merge(node.currentTerm(), 1, Integer::sum);
            }
        }
        for (Map.Entry<Integer, Integer> e : byTerm.entrySet()) {
            assertTrue(e.getValue() <= 1, "two leaders in term " + e.getKey());
        }
    }

    private static void checkLogsConsistent(Cluster cluster, Map<Integer, Object> committed) {
        for (RaftNode node : cluster.nodes().values()) {
            List<Log.Entry> applied = node.applied();
            for (int i = 0; i < applied.size(); i++) {
                int index = i + 1;
                Object command = applied.get(i).command();
                Object prior = committed.putIfAbsent(index, command);
                if (prior != null) {
                    assertEquals(prior, command, "index " + index + " diverged");
                }
            }
        }
    }

    private static void drainAndAssert(Cluster cluster, Map<Integer, Object> committed, int steps) {
        for (int i = 0; i < steps; i++) {
            cluster.step();
            checkElectionSafety(cluster);
            checkLogsConsistent(cluster, committed);
        }
    }

    @Test
    void safetyHoldsUnderMessageDrops() {
        Map<Integer, Object> committed = new HashMap<>();
        Cluster c = new Cluster(5, 10, 3, 1L, 0.15, 0.0, false);
        RaftNode leader = c.runUntilLeader(300);
        assertNotNull(leader);
        for (int i = 0; i < 10; i++) {
            RaftNode l = c.leader();
            (l != null ? l : leader).propose("cmd-" + i);
            drainAndAssert(c, committed, 8);
        }
        drainAndAssert(c, committed, 200);
    }

    @Test
    void safetyHoldsUnderDuplicationAndReorder() {
        Map<Integer, Object> committed = new HashMap<>();
        Cluster c = new Cluster(5, 10, 3, 7L, 0.0, 0.25, true);
        RaftNode leader = c.runUntilLeader(300);
        assertNotNull(leader);
        for (int i = 0; i < 15; i++) {
            RaftNode l = c.leader();
            (l != null ? l : leader).propose("x" + i);
            drainAndAssert(c, committed, 6);
        }
        drainAndAssert(c, committed, 150);
    }

    @Test
    void rollingPartitionsKeepSafetyAndEventuallyMakeProgress() {
        Map<Integer, Object> committed = new HashMap<>();
        Random rng = new Random(42);
        Cluster c = new Cluster(5, 10, 3, 42L, 0.05, 0.0, false);
        c.runUntilLeader(300);

        int proposed = 0;
        for (int i = 0; i < 40; i++) {
            int a = rng.nextInt(5), b = rng.nextInt(5);
            while (b == a) b = rng.nextInt(5);
            if (rng.nextDouble() < 0.5) c.partition(a, b); else c.heal(a, b);
            RaftNode leader = c.leader();
            if (leader != null) { leader.propose("v" + proposed); proposed++; }
            drainAndAssert(c, committed, 6);
        }

        c.healAll();
        drainAndAssert(c, committed, 400);

        RaftNode fin = c.runUntilLeader(200);
        assertNotNull(fin);
        List<Object> target = new ArrayList<>();
        for (Log.Entry e : fin.applied()) target.add(e.command());
        int agree = 0;
        for (RaftNode n : c.nodes().values()) {
            List<Object> got = new ArrayList<>();
            for (Log.Entry e : n.applied()) got.add(e.command());
            List<Object> prefix = got.size() >= target.size() ? got.subList(0, target.size()) : got;
            if (prefix.equals(target)) agree++;
        }
        assertTrue(agree >= 3, "only " + agree + "/5 nodes converged");
        assertTrue(target.size() > 0, "cluster made no progress at all");
    }

    @Test
    void soakManyRandomSchedules() {
        double[] drops = {0.0, 0.1, 0.2};
        double[] dups = {0.0, 0.15};
        for (int seed = 0; seed < 25; seed++) {
            Map<Integer, Object> committed = new HashMap<>();
            Random rng = new Random(seed);
            Cluster c = new Cluster(5, 10, 3, (long) seed,
                    drops[rng.nextInt(3)], dups[rng.nextInt(2)], rng.nextDouble() < 0.5);
            c.runUntilLeader(300);
            for (int i = 0; i < 30; i++) {
                RaftNode leader = c.leader();
                if (leader != null) leader.propose(rng.nextInt(1_000_000));
                drainAndAssert(c, committed, 3 + rng.nextInt(7));
            }
        }
    }
}
