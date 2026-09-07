// A deterministic in-memory cluster that drives time and delivers messages.
//
// There are no real timers or sockets. Step() advances every node one step and
// routes whatever messages they emit. Message delivery order is stable, and the
// network can be partitioned or healed, so an entire election or a replication
// round replays identically every run - which is what makes the tests reliable.

namespace Coracle;

public sealed class Cluster
{
    public Dictionary<int, RaftNode> Nodes { get; } = new();
    public int Time { get; private set; }
    public int Dropped { get; private set; } // count of messages the network threw away

    private readonly Random? _rng;
    private readonly double _dropProb;
    private readonly double _duplicateProb;
    private readonly bool _reorder;

    private readonly Queue<Message> _inbox = new();
    // A pair (a, b) in here means the link a<->b is cut in both directions.
    private readonly HashSet<(int, int)> _partitioned = new();

    public Cluster(int size, int electionTimeout = 10, int heartbeatInterval = 3,
        int? seed = null, double dropProb = 0.0, double duplicateProb = 0.0, bool reorder = false)
    {
        var ids = Enumerable.Range(0, size).ToList();
        _rng = seed is not null ? new Random(seed.Value) : null;
        // Stagger election timeouts by node id. Real Raft randomizes these so the
        // cluster doesn't split the vote forever; a deterministic per-node offset
        // gives the same effect while keeping the simulation reproducible. When a
        // seed is given we randomize timeouts in [T, 2T) like the paper recommends.
        foreach (var i in ids)
        {
            int timeout = _rng is not null
                ? _rng.Next(electionTimeout, 2 * electionTimeout) // [T, 2T-1]
                : electionTimeout + i * 2;
            Nodes[i] = new RaftNode(i, ids, timeout, heartbeatInterval);
        }

        _dropProb = dropProb;
        _duplicateProb = duplicateProb;
        _reorder = reorder;
    }

    // ---- network control ----

    private static (int, int) Key(int a, int b) => a < b ? (a, b) : (b, a);

    public void Partition(int a, int b) => _partitioned.Add(Key(a, b));
    public void Heal(int a, int b) => _partitioned.Remove(Key(a, b));
    public void HealAll() => _partitioned.Clear();

    public void Isolate(int node)
    {
        foreach (var other in Nodes.Keys)
            if (other != node) Partition(node, other);
    }

    private bool Reachable(int src, int dst) => !_partitioned.Contains(Key(src, dst));

    // ---- driving time ----

    public void Step()
    {
        Time++;

        var pending = new List<Message>(_inbox);
        _inbox.Clear();

        if (_reorder && _rng is not null && pending.Count > 1)
            Shuffle(pending, _rng);

        foreach (var msg in pending)
        {
            if (!(Reachable(msg.Src, msg.Dst) && Nodes.ContainsKey(msg.Dst))) continue;
            if (_rng is not null && _dropProb > 0 && _rng.NextDouble() < _dropProb)
            {
                Dropped++;
                continue;
            }
            int deliveries = 1;
            if (_rng is not null && _duplicateProb > 0 && _rng.NextDouble() < _duplicateProb)
                deliveries = 2; // deliver the same message twice
            for (int d = 0; d < deliveries; d++)
                Enqueue(Nodes[msg.Dst].Handle(msg));
        }

        foreach (var node in Nodes.Values)
            Enqueue(node.Tick());
    }

    public void Run(int steps)
    {
        for (int i = 0; i < steps; i++) Step();
    }

    public RaftNode? RunUntilLeader(int maxSteps = 200)
    {
        for (int i = 0; i < maxSteps; i++)
        {
            Step();
            var leader = Leader();
            if (leader is not null) return leader;
        }
        return null;
    }

    private void Enqueue(List<Message> messages)
    {
        foreach (var m in messages)
            if (Reachable(m.Src, m.Dst)) _inbox.Enqueue(m);
    }

    // Fisher-Yates using the same RNG, matching Python's random.shuffle semantics
    // (any uniform shuffle is fine - the chaos tests assert invariants, not order).
    private static void Shuffle(List<Message> list, Random rng)
    {
        for (int i = list.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }

    // ---- inspection ----

    /// <summary>The unique current-term leader, or null if there's a split/no leader.</summary>
    public RaftNode? Leader()
    {
        var leaders = Nodes.Values.Where(n => n.Role == Role.Leader).ToList();
        if (leaders.Count == 0) return null;
        int topTerm = leaders.Max(n => n.CurrentTerm);
        var top = leaders.Where(n => n.CurrentTerm == topTerm).ToList();
        return top.Count == 1 ? top[0] : null;
    }

    public List<RaftNode> Leaders() => Nodes.Values.Where(n => n.Role == Role.Leader).ToList();

    public List<object> CommittedCommands(int nodeId) =>
        Nodes[nodeId].Applied.Select(e => e.Command).ToList();
}
