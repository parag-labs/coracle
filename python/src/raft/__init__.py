"""A small, readable Raft consensus implementation.

The node logic is deterministic and side-effect free: a node never touches the
clock or the network directly. It receives events (a tick, or an inbound message)
and returns the messages it wants sent. A simulated ``Cluster`` drives time and
delivers messages, which is what makes leader election and log replication
reproducible - and testable without threads.
"""

from .cluster import Cluster
from .log import Log, LogEntry
from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Message,
    RequestVote,
    RequestVoteReply,
)
from .node import RaftNode, Role

__all__ = [
    "AppendEntries",
    "AppendEntriesReply",
    "Cluster",
    "Log",
    "LogEntry",
    "Message",
    "RaftNode",
    "RequestVote",
    "RequestVoteReply",
    "Role",
]
