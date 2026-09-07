// A single Raft node.
//
// The node is a deterministic state machine. It never reads a clock or sends on a
// socket itself - it only reacts:
//   - Tick() advances its internal timers and may start an election or emit
//     heartbeats, returning the messages to send.
//   - Handle(message) processes one inbound RPC and returns the replies to send.
//
// Keeping I/O out of the node is what lets the simulator replay elections and log
// replication deterministically, and lets tests assert exact behavior.

namespace MiniRaft;

public enum Role { Follower, Candidate, Leader }

public sealed class RaftNode
{
    public int Id { get; }
    public List<int> Peers { get; }
    public int ElectionTimeout { get; }
    public int HeartbeatInterval { get; }

    // Persistent state.
    public int CurrentTerm { get; private set; }
    public int? VotedFor { get; private set; }
    public Log Log { get; } = new();

    // Volatile state.
    public Role Role { get; private set; } = Role.Follower;
    public int CommitIndex { get; private set; }
    public int LastApplied { get; private set; }
    public List<LogEntry> Applied { get; } = new(); // entries handed to the state machine

    // Leader state (reset on election).
    private readonly Dictionary<int, int> _nextIndex = new();
    private readonly Dictionary<int, int> _matchIndex = new();

    // Election bookkeeping.
    private readonly HashSet<int> _votes = new();
    private int _electionElapsed;
    private int _heartbeatElapsed;

    public RaftNode(int nodeId, IEnumerable<int> peers, int electionTimeout = 10, int heartbeatInterval = 3)
    {
        Id = nodeId;
        Peers = peers.Where(p => p != nodeId).ToList();
        ElectionTimeout = electionTimeout;
        HeartbeatInterval = heartbeatInterval;
    }

    // ---- driving the node ----

    public List<Message> Tick()
    {
        if (Role == Role.Leader)
        {
            _heartbeatElapsed++;
            if (_heartbeatElapsed >= HeartbeatInterval)
            {
                _heartbeatElapsed = 0;
                return BroadcastAppend();
            }
            return new List<Message>();
        }

        _electionElapsed++;
        if (_electionElapsed >= ElectionTimeout) return StartElection();
        return new List<Message>();
    }

    public List<Message> Handle(Message msg)
    {
        // Any message from a higher term makes us a follower of that term.
        if (msg.Term > CurrentTerm) BecomeFollower(msg.Term);

        return msg switch
        {
            RequestVote rv => new List<Message> { OnRequestVote(rv) },
            RequestVoteReply rvr => OnRequestVoteReply(rvr),
            AppendEntries ae => new List<Message> { OnAppendEntries(ae) },
            AppendEntriesReply aer => OnAppendEntriesReply(aer),
            _ => new List<Message>(),
        };
    }

    /// <summary>Client entry point. Only a leader accepts a proposal; it appends to
    /// its own log and lets the next heartbeat replicate it.</summary>
    public bool Propose(object command)
    {
        if (Role != Role.Leader) return false;
        Log.Append(new LogEntry(CurrentTerm, command));
        _matchIndex[Id] = Log.LastIndex();
        return true;
    }

    // ---- role transitions ----

    private void BecomeFollower(int term)
    {
        CurrentTerm = term;
        VotedFor = null;
        Role = Role.Follower;
        _electionElapsed = 0;
        _votes.Clear();
    }

    private void BecomeCandidate()
    {
        CurrentTerm++;
        Role = Role.Candidate;
        VotedFor = Id;
        _votes.Clear();
        _votes.Add(Id);
        _electionElapsed = 0;
    }

    private void BecomeLeader()
    {
        Role = Role.Leader;
        _heartbeatElapsed = 0;
        int last = Log.LastIndex();
        _nextIndex.Clear();
        _matchIndex.Clear();
        foreach (var p in Peers)
        {
            _nextIndex[p] = last + 1;
            _matchIndex[p] = 0;
        }
        _matchIndex[Id] = last;
    }

    // ---- elections ----

    private List<Message> StartElection()
    {
        BecomeCandidate();
        if (Peers.Count == 0) // single-node cluster: instant win
        {
            BecomeLeader();
            return new List<Message>();
        }
        var outMsgs = new List<Message>();
        foreach (var p in Peers)
        {
            outMsgs.Add(new RequestVote
            {
                Src = Id,
                Dst = p,
                Term = CurrentTerm,
                LastLogIndex = Log.LastIndex(),
                LastLogTerm = Log.LastTerm(),
            });
        }
        return outMsgs;
    }

    private RequestVoteReply OnRequestVote(RequestVote msg)
    {
        bool granted = false;
        if (msg.Term >= CurrentTerm)
        {
            bool free = VotedFor is null || VotedFor == msg.Src;
            bool current = Log.IsUpToDate(msg.LastLogIndex, msg.LastLogTerm);
            if (free && current)
            {
                granted = true;
                VotedFor = msg.Src;
                _electionElapsed = 0;
            }
        }
        return new RequestVoteReply { Src = Id, Dst = msg.Src, Term = CurrentTerm, Granted = granted };
    }

    private List<Message> OnRequestVoteReply(RequestVoteReply msg)
    {
        if (Role != Role.Candidate || msg.Term != CurrentTerm) return new List<Message>();
        if (msg.Granted)
        {
            _votes.Add(msg.Src);
            if (_votes.Count >= Majority())
            {
                BecomeLeader();
                return BroadcastAppend();
            }
        }
        return new List<Message>();
    }

    // ---- log replication ----

    private List<Message> BroadcastAppend() => Peers.Select(AppendFor).Cast<Message>().ToList();

    private AppendEntries AppendFor(int peer)
    {
        int nextIdx = _nextIndex.TryGetValue(peer, out var v) ? v : Log.LastIndex() + 1;
        int prevIndex = nextIdx - 1;
        return new AppendEntries
        {
            Src = Id,
            Dst = peer,
            Term = CurrentTerm,
            PrevLogIndex = prevIndex,
            PrevLogTerm = Log.TermAt(prevIndex),
            Entries = Log.SliceFrom(nextIdx),
            LeaderCommit = CommitIndex,
        };
    }

    private AppendEntriesReply OnAppendEntries(AppendEntries msg)
    {
        var reply = new AppendEntriesReply { Src = Id, Dst = msg.Src, Term = CurrentTerm, Success = false };
        if (msg.Term < CurrentTerm) return reply; // stale leader

        // Valid current-term leader: defer to it and reset our election timer.
        Role = Role.Follower;
        _electionElapsed = 0;

        if (!Log.Matches(msg.PrevLogIndex, msg.PrevLogTerm)) return reply; // gap/conflict at prev

        Log.TruncateAndAppend(msg.PrevLogIndex, msg.Entries);
        if (msg.LeaderCommit > CommitIndex)
        {
            CommitIndex = Math.Min(msg.LeaderCommit, Log.LastIndex());
            ApplyCommitted();
        }

        reply.Success = true;
        reply.MatchIndex = msg.PrevLogIndex + msg.Entries.Count;
        return reply;
    }

    private List<Message> OnAppendEntriesReply(AppendEntriesReply msg)
    {
        if (Role != Role.Leader || msg.Term != CurrentTerm) return new List<Message>();
        if (msg.Success)
        {
            _matchIndex[msg.Src] = msg.MatchIndex;
            _nextIndex[msg.Src] = msg.MatchIndex + 1;
            AdvanceCommit();
            return new List<Message>();
        }
        // Rejected: step next_index back and retry.
        int cur = _nextIndex.TryGetValue(msg.Src, out var v) ? v : 1;
        _nextIndex[msg.Src] = Math.Max(1, cur - 1);
        return new List<Message> { AppendFor(msg.Src) };
    }

    /// <summary>A leader commits index N when a majority has it AND it's from the
    /// current term (the paper's safety rule against committing stale entries).</summary>
    private void AdvanceCommit()
    {
        for (int n = Log.LastIndex(); n > CommitIndex; n--)
        {
            if (Log.TermAt(n) != CurrentTerm) continue;
            int replicas = 1 + Peers.Count(p => _matchIndex.GetValueOrDefault(p, 0) >= n);
            if (replicas >= Majority())
            {
                CommitIndex = n;
                ApplyCommitted();
                break;
            }
        }
    }

    private void ApplyCommitted()
    {
        while (LastApplied < CommitIndex)
        {
            LastApplied++;
            Applied.Add(Log.Get(LastApplied));
        }
    }

    private int Majority() => (Peers.Count + 1) / 2 + 1;
}
