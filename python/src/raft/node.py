"""A single Raft node.

The node is a deterministic state machine. It never reads a clock or sends on a
socket itself - it only reacts:

- ``tick()`` advances its internal timers and may start an election or emit
  heartbeats, returning the messages to send.
- ``handle(message)`` processes one inbound RPC and returns the replies to send.

Keeping I/O out of the node is what lets the simulator replay elections and log
replication deterministically, and lets tests assert exact behavior.
"""

from __future__ import annotations

from enum import Enum

from .log import Log, LogEntry
from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Message,
    RequestVote,
    RequestVoteReply,
)


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, node_id: int, peers: list[int], election_timeout: int = 10,
                 heartbeat_interval: int = 3) -> None:
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval

        # Persistent state.
        self.current_term = 0
        self.voted_for: int | None = None
        self.log = Log()

        # Volatile state.
        self.role = Role.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.applied: list[LogEntry] = []  # entries handed to the state machine

        # Leader state (reset on election).
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        # Election bookkeeping.
        self._votes: set[int] = set()
        self._election_elapsed = 0
        self._heartbeat_elapsed = 0

    # ---- driving the node ----

    def tick(self) -> list[Message]:
        if self.role == Role.LEADER:
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self.heartbeat_interval:
                self._heartbeat_elapsed = 0
                return self._broadcast_append()
            return []

        self._election_elapsed += 1
        if self._election_elapsed >= self.election_timeout:
            return self._start_election()
        return []

    def handle(self, msg: Message) -> list[Message]:
        # Any message from a higher term makes us a follower of that term.
        if msg.term > self.current_term:
            self._become_follower(msg.term)

        if isinstance(msg, RequestVote):
            return [self._on_request_vote(msg)]
        if isinstance(msg, RequestVoteReply):
            return self._on_request_vote_reply(msg)
        if isinstance(msg, AppendEntries):
            return [self._on_append_entries(msg)]
        if isinstance(msg, AppendEntriesReply):
            return self._on_append_entries_reply(msg)
        return []

    def propose(self, command: object) -> bool:
        """Client entry point. Only a leader accepts a proposal; it appends to its
        own log and lets the next heartbeat replicate it."""
        if self.role != Role.LEADER:
            return False
        self.log.append(LogEntry(self.current_term, command))
        self.match_index[self.id] = self.log.last_index()
        return True

    # ---- role transitions ----

    def _become_follower(self, term: int) -> None:
        self.current_term = term
        self.voted_for = None
        self.role = Role.FOLLOWER
        self._election_elapsed = 0
        self._votes = set()

    def _become_candidate(self) -> None:
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self._votes = {self.id}
        self._election_elapsed = 0

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self._heartbeat_elapsed = 0
        last = self.log.last_index()
        self.next_index = {p: last + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.match_index[self.id] = last

    # ---- elections ----

    def _start_election(self) -> list[Message]:
        self._become_candidate()
        if not self.peers:  # single-node cluster: instant win
            self._become_leader()
            return []
        out: list[Message] = []
        for p in self.peers:
            out.append(
                RequestVote(
                    src=self.id,
                    dst=p,
                    term=self.current_term,
                    last_log_index=self.log.last_index(),
                    last_log_term=self.log.last_term(),
                )
            )
        return out

    def _on_request_vote(self, msg: RequestVote) -> RequestVoteReply:
        granted = False
        if msg.term >= self.current_term:
            free = self.voted_for is None or self.voted_for == msg.src
            current = self.log.is_up_to_date(msg.last_log_index, msg.last_log_term)
            if free and current:
                granted = True
                self.voted_for = msg.src
                self._election_elapsed = 0
        return RequestVoteReply(src=self.id, dst=msg.src, term=self.current_term, granted=granted)

    def _on_request_vote_reply(self, msg: RequestVoteReply) -> list[Message]:
        if self.role != Role.CANDIDATE or msg.term != self.current_term:
            return []
        if msg.granted:
            self._votes.add(msg.src)
            if len(self._votes) >= self._majority():
                self._become_leader()
                return self._broadcast_append()
        return []

    # ---- log replication ----

    def _broadcast_append(self) -> list[Message]:
        return [self._append_for(p) for p in self.peers]

    def _append_for(self, peer: int) -> AppendEntries:
        next_idx = self.next_index.get(peer, self.log.last_index() + 1)
        prev_index = next_idx - 1
        return AppendEntries(
            src=self.id,
            dst=peer,
            term=self.current_term,
            prev_log_index=prev_index,
            prev_log_term=self.log.term_at(prev_index),
            entries=self.log.slice_from(next_idx),
            leader_commit=self.commit_index,
        )

    def _on_append_entries(self, msg: AppendEntries) -> AppendEntriesReply:
        reply = AppendEntriesReply(src=self.id, dst=msg.src, term=self.current_term, success=False)
        if msg.term < self.current_term:
            return reply  # stale leader

        # Valid current-term leader: defer to it and reset our election timer.
        self.role = Role.FOLLOWER
        self._election_elapsed = 0

        if not self.log.matches(msg.prev_log_index, msg.prev_log_term):
            return reply  # log gap/conflict at prev; leader will back off

        self.log.truncate_and_append(msg.prev_log_index, msg.entries)
        if msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, self.log.last_index())
            self._apply_committed()

        reply.success = True
        reply.match_index = msg.prev_log_index + len(msg.entries)
        return reply

    def _on_append_entries_reply(self, msg: AppendEntriesReply) -> list[Message]:
        if self.role != Role.LEADER or msg.term != self.current_term:
            return []
        if msg.success:
            self.match_index[msg.src] = msg.match_index
            self.next_index[msg.src] = msg.match_index + 1
            self._advance_commit()
            return []
        # Rejected: step next_index back and retry.
        self.next_index[msg.src] = max(1, self.next_index.get(msg.src, 1) - 1)
        return [self._append_for(msg.src)]

    def _advance_commit(self) -> None:
        """A leader commits index N when a majority has it AND it's from the current
        term (the paper's safety rule against committing stale entries)."""
        for n in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                continue
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= self._majority():
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.applied.append(self.log.get(self.last_applied))

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1
