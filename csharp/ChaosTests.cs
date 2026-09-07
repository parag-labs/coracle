// Chaos suite: hammer the cluster with a lossy, out-of-order, duplicating network
// and rolling partitions, and assert Raft's safety invariants never break.
//
// Invariants checked after (nearly) every step:
//   - Election safety: at most one leader per term.
//   - State-machine safety: once an index is applied anywhere, no node ever applies
//     a different command at that index.
//
// Liveness is checked separately: after the network is healed, a majority must
// converge on the leader's committed log.

using Xunit;

namespace Coracle.Tests;

public class ChaosTests
{
    private static void CheckElectionSafety(Cluster cluster)
    {
        var leadersByTerm = new Dictionary<int, int>();
        foreach (var node in cluster.Nodes.Values)
            if (node.Role == Role.Leader)
                leadersByTerm[node.CurrentTerm] = leadersByTerm.GetValueOrDefault(node.CurrentTerm, 0) + 1;
        foreach (var (term, count) in leadersByTerm)
            Assert.True(count <= 1, $"two leaders in term {term}");
    }

    private static void CheckLogsConsistent(Cluster cluster, Dictionary<int, object> committed)
    {
        foreach (var node in cluster.Nodes.Values)
        {
            for (int i = 0; i < node.Applied.Count; i++)
            {
                int index = i + 1;
                var command = node.Applied[i].Command;
                if (!committed.TryGetValue(index, out var prior))
                {
                    committed[index] = command;
                }
                else
                {
                    Assert.True(Equals(prior, command), $"index {index} diverged: {prior} vs {command}");
                }
            }
        }
    }

    private static void DrainAndAssert(Cluster cluster, Dictionary<int, object> committed, int steps)
    {
        for (int i = 0; i < steps; i++)
        {
            cluster.Step();
            CheckElectionSafety(cluster);
            CheckLogsConsistent(cluster, committed);
        }
    }

    [Fact]
    public void SafetyHoldsUnderMessageDrops()
    {
        var committed = new Dictionary<int, object>();
        var c = new Cluster(5, seed: 1, dropProb: 0.15);
        var leader = c.RunUntilLeader(300);
        Assert.NotNull(leader);
        for (int i = 0; i < 10; i++)
        {
            (c.Leader() ?? leader!).Propose($"cmd-{i}");
            DrainAndAssert(c, committed, 8);
        }
        DrainAndAssert(c, committed, 200);
    }

    [Fact]
    public void SafetyHoldsUnderDuplicationAndReorder()
    {
        var committed = new Dictionary<int, object>();
        var c = new Cluster(5, seed: 7, duplicateProb: 0.25, reorder: true);
        var leader = c.RunUntilLeader(300);
        Assert.NotNull(leader);
        for (int i = 0; i < 15; i++)
        {
            (c.Leader() ?? leader!).Propose($"x{i}");
            DrainAndAssert(c, committed, 6);
        }
        DrainAndAssert(c, committed, 150);
    }

    [Fact]
    public void RollingPartitionsKeepSafetyAndEventuallyMakeProgress()
    {
        var committed = new Dictionary<int, object>();
        var rng = new Random(42);
        var c = new Cluster(5, seed: 42, dropProb: 0.05);
        c.RunUntilLeader(300);

        int proposed = 0;
        for (int i = 0; i < 40; i++)
        {
            // Randomly cut or heal a link, then propose on whoever leads right now.
            int a = rng.Next(5), b = rng.Next(5);
            while (b == a) b = rng.Next(5);
            if (rng.NextDouble() < 0.5) c.Partition(a, b); else c.Heal(a, b);
            var leader = c.Leader();
            if (leader is not null) { leader.Propose($"v{proposed}"); proposed++; }
            DrainAndAssert(c, committed, 6);
        }

        c.HealAll();
        DrainAndAssert(c, committed, 400);

        var final = c.RunUntilLeader(200);
        Assert.NotNull(final);
        var target = final!.Applied.Select(e => e.Command).ToList();
        int agree = c.Nodes.Values.Count(n =>
            n.Applied.Select(e => e.Command).Take(target.Count).SequenceEqual(target));
        Assert.True(agree >= 3, $"only {agree}/5 nodes converged");
        Assert.True(target.Count > 0, "cluster made no progress at all");
    }

    [Fact]
    public void SoakManyRandomSchedules()
    {
        for (int seed = 0; seed < 25; seed++)
        {
            var committed = new Dictionary<int, object>();
            var rng = new Random(seed);
            var c = new Cluster(5,
                seed: seed,
                dropProb: new[] { 0.0, 0.1, 0.2 }[rng.Next(3)],
                duplicateProb: new[] { 0.0, 0.15 }[rng.Next(2)],
                reorder: rng.NextDouble() < 0.5);
            c.RunUntilLeader(300);
            for (int i = 0; i < 30; i++)
            {
                var leader = c.Leader();
                leader?.Propose(rng.Next(1_000_000));
                DrainAndAssert(c, committed, rng.Next(3, 10));
            }
        }
    }
}
