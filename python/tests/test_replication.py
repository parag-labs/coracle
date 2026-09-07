"""Log replication and commit safety: a proposal reaches a majority and is applied
in order on every node, a lagging follower catches up, and a minority partition
cannot commit."""

from __future__ import annotations

from raft.cluster import Cluster


def _elect(c: Cluster):
    leader = c.run_until_leader()
    assert leader is not None
    return leader


def test_proposal_replicates_and_applies_in_order():
    c = Cluster(3)
    leader = _elect(c)
    assert leader.propose("x=1") is True
    assert leader.propose("x=2") is True
    c.run(40)

    for node_id in c.nodes:
        assert c.committed_commands(node_id) == ["x=1", "x=2"]


def test_follower_rejects_client_proposals():
    c = Cluster(3)
    leader = _elect(c)
    follower = next(n for n in c.nodes.values() if n.id != leader.id)
    assert follower.propose("nope") is False


def test_commit_index_reaches_a_majority():
    c = Cluster(5)
    leader = _elect(c)
    leader.propose("a")
    c.run(50)
    committed_on = sum(1 for nid in c.nodes if c.committed_commands(nid) == ["a"])
    assert committed_on >= 3  # a majority of 5


def test_lagging_follower_catches_up_after_healing():
    c = Cluster(3, election_timeout=10, heartbeat_interval=2)
    leader = _elect(c)
    lagging = next(n for n in c.nodes.values() if n.id != leader.id)

    c.isolate(lagging.id)
    for cmd in ["c1", "c2", "c3"]:
        leader.propose(cmd)
    c.run(60)
    # The isolated follower missed everything.
    assert c.committed_commands(lagging.id) == []

    c.heal_all()
    c.run(80)
    # After healing, the leader backfills it to a full match.
    assert c.committed_commands(lagging.id) == ["c1", "c2", "c3"]


def test_minority_partition_cannot_commit():
    c = Cluster(5)
    leader = _elect(c)
    # Cut the leader down to itself + one follower (a minority of 2).
    others = [n.id for n in c.nodes.values() if n.id != leader.id]
    keep = others[0]
    for nid in others[1:]:
        c.partition(leader.id, nid)

    leader.propose("lonely")
    c.run(60)
    # With only 2 of 5 reachable, the entry must not commit anywhere.
    for nid in c.nodes:
        assert "lonely" not in c.committed_commands(nid)
    _ = keep
