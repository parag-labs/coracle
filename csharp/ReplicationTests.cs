// Log replication and commit safety: a proposal reaches a majority and is applied
// in order on every node, a lagging follower catches up, and a minority partition
// cannot commit.

using Xunit;

namespace Coracle.Tests;

public class ReplicationTests
{
    private static RaftNode Elect(Cluster c)
    {
        var leader = c.RunUntilLeader();
        Assert.NotNull(leader);
        return leader!;
    }

    [Fact]
    public void ProposalReplicatesAndAppliesInOrder()
    {
        var c = new Cluster(3);
        var leader = Elect(c);
        Assert.True(leader.Propose("x=1"));
        Assert.True(leader.Propose("x=2"));
        c.Run(40);

        foreach (var nodeId in c.Nodes.Keys)
            Assert.Equal(new object[] { "x=1", "x=2" }, c.CommittedCommands(nodeId));
    }

    [Fact]
    public void FollowerRejectsClientProposals()
    {
        var c = new Cluster(3);
        var leader = Elect(c);
        var follower = c.Nodes.Values.First(n => n.Id != leader.Id);
        Assert.False(follower.Propose("nope"));
    }

    [Fact]
    public void CommitIndexReachesAMajority()
    {
        var c = new Cluster(5);
        var leader = Elect(c);
        leader.Propose("a");
        c.Run(50);
        int committedOn = c.Nodes.Keys.Count(nid =>
            c.CommittedCommands(nid).SequenceEqual(new object[] { "a" }));
        Assert.True(committedOn >= 3); // a majority of 5
    }

    [Fact]
    public void LaggingFollowerCatchesUpAfterHealing()
    {
        var c = new Cluster(3, electionTimeout: 10, heartbeatInterval: 2);
        var leader = Elect(c);
        var lagging = c.Nodes.Values.First(n => n.Id != leader.Id);

        c.Isolate(lagging.Id);
        foreach (var cmd in new[] { "c1", "c2", "c3" }) leader.Propose(cmd);
        c.Run(60);
        Assert.Empty(c.CommittedCommands(lagging.Id)); // isolated follower missed everything

        c.HealAll();
        c.Run(80);
        // After healing, the leader backfills it to a full match.
        Assert.Equal(new object[] { "c1", "c2", "c3" }, c.CommittedCommands(lagging.Id));
    }

    [Fact]
    public void MinorityPartitionCannotCommit()
    {
        var c = new Cluster(5);
        var leader = Elect(c);
        // Cut the leader down to itself + one follower (a minority of 2).
        var others = c.Nodes.Values.Where(n => n.Id != leader.Id).Select(n => n.Id).ToList();
        foreach (var nid in others.Skip(1)) c.Partition(leader.Id, nid);

        leader.Propose("lonely");
        c.Run(60);
        // With only 2 of 5 reachable, the entry must not commit anywhere.
        foreach (var nid in c.Nodes.Keys)
            Assert.DoesNotContain("lonely", c.CommittedCommands(nid));
    }
}
