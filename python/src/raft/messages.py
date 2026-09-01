"""The RPC messages exchanged between nodes. These are plain data; the node
returns them from its handlers and the cluster delivers them."""

from __future__ import annotations

from dataclasses import dataclass, field

from .log import LogEntry


@dataclass
class Message:
    src: int
    dst: int
    term: int


@dataclass
class RequestVote(Message):
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class RequestVoteReply(Message):
    granted: bool = False


@dataclass
class AppendEntries(Message):
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesReply(Message):
    success: bool = False
    # On success, the follower's last index so the leader can advance matchIndex.
    match_index: int = 0
