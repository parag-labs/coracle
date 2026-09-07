// Leader election: a cluster converges on exactly one leader, re-elects after a
// leader is isolated, and won't elect a candidate with a stale log.

using Xunit;

namespace Coracle.Tests;

public class ElectionTests
{
    [Fact]
    public void SingleNodeElectsItself()
    {
        var c = new Cluster(1);
        var leader = c.RunUntilLeader();
        Assert.NotNull(leader);
        Assert.Equal(0, leader!.Id);
    }

    [Fact]
    public void ClusterConvergesOnOneLeader()
    {
        var c = new Cluster(3);
        var leader = c.RunUntilLeader();
        Assert.NotNull(leader);
        Assert.Single(c.Leaders());
        foreach (var node in c.Nodes.Values)
            Assert.Equal(leader!.CurrentTerm, node.CurrentTerm);
    }

    [Fact]
    public void FiveNodeClusterAlsoConverges()
    {
        var c = new Cluster(5);
        Assert.NotNull(c.RunUntilLeader());
        Assert.Single(c.Leaders());
    }

    [Fact]
    public void NewLeaderEmergesAfterTheLeaderIsIsolated()
    {
        var c = new Cluster(5);
        var first = c.RunUntilLeader();
        Assert.NotNull(first);

        c.Isolate(first!.Id);
        RaftNode? newLeader = null;
        for (int i = 0; i < 300; i++)
        {
            c.Step();
            var candidates = c.Leaders().Where(n => n.Id != first.Id).ToList();
            if (candidates.Count > 0) { newLeader = candidates[0]; break; }
        }
        Assert.NotNull(newLeader);
        Assert.NotEqual(first.Id, newLeader!.Id);
        Assert.True(newLeader.CurrentTerm > first.CurrentTerm);
    }

    [Fact]
    public void TermIncrementsOnElectionTimeout()
    {
        var c = new Cluster(3);
        c.RunUntilLeader();
        Assert.True(c.Leader()!.CurrentTerm >= 1);
    }

    [Fact]
    public void StaleCandidateIsDenied()
    {
        // A node with an empty log should not get a vote from a node with a longer log.
        var voter = new RaftNode(0, new[] { 0, 1 }, electionTimeout: 10);
        // Push the voter's term up and give it a committed entry via a term-6 leader.
        voter.Log.Append(new LogEntry(5, "committed"));

        var stale = new RequestVote { Src = 1, Dst = 0, Term = 6, LastLogIndex = 0, LastLogTerm = 0 };
        var reply = (RequestVoteReply)voter.Handle(stale)[0];
        Assert.False(reply.Granted);
        Assert.Equal(Role.Follower, voter.Role); // it did adopt the higher term
    }
}
