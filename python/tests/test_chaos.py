"""Chaos suite: hammer the cluster with a lossy, out-of-order, duplicating network
and rolling partitions, and assert Raft's safety invariants never break.

The invariants checked after (nearly) every step:

- Election safety: at most one leader per term.
- Log matching / state-machine safety: any two nodes' applied logs agree on every
  index they share (one is a prefix of the other).
- Commit durability: once an index is applied anywhere, no node ever applies a
  different command at that index.

Liveness is checked separately: after the network is fully healed, a majority must
converge on the leader's committed log.
"""

from __future__ import annotations

import random

from raft.cluster import Cluster
from raft.node import Role


def _check_election_safety(cluster: Cluster) -> None:
    leaders_by_term: dict[int, int] = {}
    for node in cluster.nodes.values():
        if node.role == Role.LEADER:
            leaders_by_term[node.current_term] = leaders_by_term.get(node.current_term, 0) + 1
    for term, count in leaders_by_term.items():
        assert count <= 1, f"two leaders in term {term}"


def _check_logs_consistent(cluster: Cluster, committed: dict[int, object]) -> None:
    for node in cluster.nodes.values():
        for i, entry in enumerate(node.applied, start=1):
            prior = committed.setdefault(i, entry.command)
            assert prior == entry.command, f"index {i} diverged: {prior!r} vs {entry.command!r}"


def _drain_and_assert(cluster: Cluster, committed: dict[int, object], steps: int) -> None:
    for _ in range(steps):
        cluster.step()
        _check_election_safety(cluster)
        _check_logs_consistent(cluster, committed)


def test_safety_holds_under_message_drops():
    committed: dict[int, object] = {}
    c = Cluster(5, seed=1, drop_prob=0.15)
    leader = c.run_until_leader(300)
    assert leader is not None
    for i in range(10):
        (c.leader() or leader).propose(f"cmd-{i}")
        _drain_and_assert(c, committed, 8)
    # Heal implicitly (no partitions here) and let it settle; safety must hold.
    _drain_and_assert(c, committed, 200)


def test_safety_holds_under_duplication_and_reorder():
    committed: dict[int, object] = {}
    c = Cluster(5, seed=7, duplicate_prob=0.25, reorder=True)
    leader = c.run_until_leader(300)
    assert leader is not None
    for i in range(15):
        (c.leader() or leader).propose(f"x{i}")
        _drain_and_assert(c, committed, 6)
    _drain_and_assert(c, committed, 150)


def test_rolling_partitions_keep_safety_and_eventually_make_progress():
    committed: dict[int, object] = {}
    rng = random.Random(42)
    c = Cluster(5, seed=42, drop_prob=0.05)
    c.run_until_leader(300)

    proposed = 0
    for _ in range(40):
        # Randomly cut or heal a link, then propose on whoever leads right now.
        a, b = rng.sample(range(5), 2)
        if rng.random() < 0.5:
            c.partition(a, b)
        else:
            c.heal(a, b)
        leader = c.leader()
        if leader is not None:
            leader.propose(f"v{proposed}")
            proposed += 1
        _drain_and_assert(c, committed, 6)

    # Heal everything and give it plenty of time to converge.
    c.heal_all()
    _drain_and_assert(c, committed, 400)

    leader = c.run_until_leader(200)
    assert leader is not None
    # A majority must match the leader's committed log exactly.
    target = [e.command for e in leader.applied]
    agree = sum(
        1
        for n in c.nodes.values()
        if [e.command for e in n.applied][: len(target)] == target
    )
    assert agree >= 3, f"only {agree}/5 nodes converged"
    assert len(target) > 0, "cluster made no progress at all"


def test_soak_many_random_schedules():
    """Many independent randomized runs; every one must preserve safety."""
    for seed in range(25):
        committed: dict[int, object] = {}
        rng = random.Random(seed)
        c = Cluster(
            5,
            seed=seed,
            drop_prob=rng.choice([0.0, 0.1, 0.2]),
            duplicate_prob=rng.choice([0.0, 0.15]),
            reorder=rng.random() < 0.5,
        )
        c.run_until_leader(300)
        for _ in range(30):
            leader = c.leader()
            if leader is not None:
                leader.propose(rng.randint(0, 1_000_000))
            _drain_and_assert(c, committed, rng.randint(3, 9))
