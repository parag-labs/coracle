// The RPC messages exchanged between nodes. These are plain data; the node returns
// them from its handlers and the cluster delivers them.

namespace MiniRaft;

public abstract class Message
{
    public int Src { get; init; }
    public int Dst { get; init; }
    public int Term { get; init; }
}

public sealed class RequestVote : Message
{
    public int LastLogIndex { get; init; }
    public int LastLogTerm { get; init; }
}

public sealed class RequestVoteReply : Message
{
    public bool Granted { get; init; }
}

public sealed class AppendEntries : Message
{
    public int PrevLogIndex { get; init; }
    public int PrevLogTerm { get; init; }
    public List<LogEntry> Entries { get; init; } = new();
    public int LeaderCommit { get; init; }
}

public sealed class AppendEntriesReply : Message
{
    public bool Success { get; set; }
    // On success, the follower's last index so the leader can advance matchIndex.
    public int MatchIndex { get; set; }
}
