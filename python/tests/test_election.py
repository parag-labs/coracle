"""Leader election: a cluster converges on exactly one leader, re-elects after a
leader is isolated, and won't elect a candidate with a stale log."""

from __future__ import annotations

from raft.cluster import Cluster
from raft.log import LogEntry
from raft.node import RaftNode, Role


def test_single_node_elects_itself():
    c = Cluster(1)
    leader = c.run_until_leader()
    assert leader is not None
    assert leader.id == 0


def test_cluster_converges_on_one_leader():
    c = Cluster(3)
    leader = c.run_until_leader()
    assert leader is not None
    # Exactly one leader at the highest term.
    assert len(c.leaders()) == 1
    # Followers share the leader's term.
    for node in c.nodes.values():
        assert node.current_term == leader.current_term


def test_five_node_cluster_also_converges():
    c = Cluster(5)
    assert c.run_until_leader() is not None
    assert len(c.leaders()) == 1


def test_new_leader_emerges_after_the_leader_is_isolated():
    c = Cluster(5)
    first = c.run_until_leader()
    assert first is not None

    c.isolate(first.id)
    # The remaining majority must elect someone new at a higher term.
    new_leader = None
    for _ in range(300):
        c.step()
        candidates = [n for n in c.leaders() if n.id != first.id]
        if candidates:
            new_leader = candidates[0]
            break
    assert new_leader is not None
    assert new_leader.id != first.id
    assert new_leader.current_term > first.current_term


def test_term_increments_on_election_timeout():
    c = Cluster(3)
    c.run_until_leader()
    assert c.leader().current_term >= 1


def test_stale_candidate_is_denied():
    # A node with an empty log should not get a vote from a node with a longer log.
    voter = RaftNode(0, [0, 1], election_timeout=10)
    voter.current_term = 5
    voter.log.append(LogEntry(5, "committed"))

    from raft.messages import RequestVote

    stale = RequestVote(src=1, dst=0, term=6, last_log_index=0, last_log_term=0)
    reply = voter.handle(stale)[0]
    assert reply.granted is False
    assert voter.role == Role.FOLLOWER  # it did adopt the higher term
