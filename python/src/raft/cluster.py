"""A deterministic in-memory cluster that drives time and delivers messages.

There are no real timers or sockets. ``tick()`` advances every node one step and
routes whatever messages they emit. Message delivery order is stable, and the
network can be partitioned or healed, so an entire election or a replication round
replays identically every run - which is what makes the tests reliable.
"""

from __future__ import annotations

import random
from collections import deque

from .messages import Message
from .node import RaftNode, Role


class Cluster:
    def __init__(self, size: int, election_timeout: int = 10, heartbeat_interval: int = 3,
                 seed: int | None = None, drop_prob: float = 0.0,
                 duplicate_prob: float = 0.0, reorder: bool = False) -> None:
        ids = list(range(size))
        self._rng = random.Random(seed) if seed is not None else None
        # Stagger election timeouts by node id. Real Raft randomizes these so the
        # cluster doesn't split the vote forever; a deterministic per-node offset
        # gives the same effect while keeping the simulation reproducible. When a
        # seed is given we randomize timeouts in [T, 2T) like the paper recommends,
        # which is what the benchmarks use to get a realistic election-time spread.
        self.nodes: dict[int, RaftNode] = {}
        for i in ids:
            if self._rng is not None:
                timeout = self._rng.randint(election_timeout, 2 * election_timeout - 1)
            else:
                timeout = election_timeout + i * 2
            self.nodes[i] = RaftNode(i, ids, timeout, heartbeat_interval)

        # Fault knobs (all off by default so plain Cluster(n) stays deterministic).
        self.drop_prob = drop_prob
        self.duplicate_prob = duplicate_prob
        self.reorder = reorder

        self._inbox: deque[Message] = deque()
        # A pair (a, b) in here means the link a<->b is cut in both directions.
        self._partitioned: set[frozenset[int]] = set()
        self.time = 0
        self.dropped = 0  # count of messages the network threw away

    # ---- network control ----

    def partition(self, a: int, b: int) -> None:
        self._partitioned.add(frozenset((a, b)))

    def heal(self, a: int, b: int) -> None:
        self._partitioned.discard(frozenset((a, b)))

    def heal_all(self) -> None:
        self._partitioned.clear()

    def isolate(self, node: int) -> None:
        for other in self.nodes:
            if other != node:
                self.partition(node, other)

    def _reachable(self, src: int, dst: int) -> bool:
        return frozenset((src, dst)) not in self._partitioned

    # ---- driving time ----

    def step(self) -> None:
        """Advance one tick: deliver queued messages, then tick every node.

        With the fault knobs off this is fully deterministic. With a seeded RNG it
        can drop, duplicate, and reorder in-flight messages to model a lossy,
        out-of-order network - which is exactly what the chaos suite leans on."""
        self.time += 1

        pending = list(self._inbox)
        self._inbox.clear()

        if self.reorder and self._rng is not None and len(pending) > 1:
            self._rng.shuffle(pending)

        for msg in pending:
            if not (self._reachable(msg.src, msg.dst) and msg.dst in self.nodes):
                continue
            if self._rng is not None and self.drop_prob > 0 and self._rng.random() < self.drop_prob:
                self.dropped += 1
                continue
            deliveries = 1
            if self._rng is not None and self.duplicate_prob > 0 and self._rng.random() < self.duplicate_prob:
                deliveries = 2  # deliver the same message twice
            for _ in range(deliveries):
                self._enqueue(self.nodes[msg.dst].handle(msg))

        for node in self.nodes.values():
            self._enqueue(node.tick())

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.step()

    def run_until_leader(self, max_steps: int = 200) -> RaftNode | None:
        for _ in range(max_steps):
            self.step()
            leader = self.leader()
            if leader is not None:
                return leader
        return None

    def _enqueue(self, messages: list[Message]) -> None:
        for m in messages:
            if self._reachable(m.src, m.dst):
                self._inbox.append(m)

    # ---- inspection ----

    def leader(self) -> RaftNode | None:
        """The unique current-term leader, or None if there's a split/no leader."""
        leaders = [n for n in self.nodes.values() if n.role == Role.LEADER]
        if not leaders:
            return None
        top_term = max(n.current_term for n in leaders)
        top = [n for n in leaders if n.current_term == top_term]
        return top[0] if len(top) == 1 else None

    def leaders(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n.role == Role.LEADER]

    def committed_commands(self, node_id: int) -> list[object]:
        return [e.command for e in self.nodes[node_id].applied]
