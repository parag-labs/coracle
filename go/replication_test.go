package coracle

import "testing"

func elect(t *testing.T, c *Cluster) *RaftNode {
	t.Helper()
	leader := c.RunUntilLeader()
	if leader == nil {
		t.Fatal("expected a leader to be elected")
	}
	return leader
}

func TestProposalReplicatesAndAppliesInOrder(t *testing.T) {
	c := NewCluster(3)
	leader := elect(t, c)
	if !leader.Propose("x=1") {
		t.Fatal("leader should accept proposal x=1")
	}
	if !leader.Propose("x=2") {
		t.Fatal("leader should accept proposal x=2")
	}
	c.Run(40)

	for _, id := range []int{0, 1, 2} {
		if !equalCommands(c.CommittedCommands(id), "x=1", "x=2") {
			t.Fatalf("node %d applied %v, want [x=1 x=2]", id, c.CommittedCommands(id))
		}
	}
}

func TestFollowerRejectsClientProposals(t *testing.T) {
	c := NewCluster(3)
	leader := elect(t, c)
	var follower *RaftNode
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}
	if follower.Propose("nope") {
		t.Fatal("a follower must reject client proposals")
	}
}

func TestCommitIndexReachesAMajority(t *testing.T) {
	c := NewCluster(5)
	leader := elect(t, c)
	leader.Propose("a")
	c.Run(50)
	committedOn := 0
	for _, id := range []int{0, 1, 2, 3, 4} {
		if equalCommands(c.CommittedCommands(id), "a") {
			committedOn++
		}
	}
	if committedOn < 3 {
		t.Fatalf("committed on %d nodes, want a majority (>= 3)", committedOn)
	}
}

func TestLaggingFollowerCatchesUpAfterHealing(t *testing.T) {
	c := NewClusterWithOptions(3, ClusterOptions{HeartbeatInterval: 2})
	leader := elect(t, c)
	var lagging *RaftNode
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			lagging = n
			break
		}
	}

	c.Isolate(lagging.ID)
	for _, cmd := range []string{"c1", "c2", "c3"} {
		leader.Propose(cmd)
	}
	c.Run(60)
	if len(c.CommittedCommands(lagging.ID)) != 0 {
		t.Fatalf("isolated follower applied %v, want nothing", c.CommittedCommands(lagging.ID))
	}

	c.HealAll()
	c.Run(80)
	if !equalCommands(c.CommittedCommands(lagging.ID), "c1", "c2", "c3") {
		t.Fatalf("after healing follower applied %v, want [c1 c2 c3]", c.CommittedCommands(lagging.ID))
	}
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := NewCluster(5)
	leader := elect(t, c)
	var others []int
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			others = append(others, n.ID)
		}
	}
	// Cut the leader down to itself plus one follower (a minority of 2 of 5).
	for _, nid := range others[1:] {
		c.Partition(leader.ID, nid)
	}

	leader.Propose("lonely")
	c.Run(60)
	for _, id := range []int{0, 1, 2, 3, 4} {
		for _, cmd := range c.CommittedCommands(id) {
			if cmd == "lonely" {
				t.Fatalf("node %d committed 'lonely' from a minority partition", id)
			}
		}
	}
}

func TestSingleNodeAcceptsButCannotCommitAlone(t *testing.T) {
	// A lone node elects itself, but commit advancement is driven solely by
	// AppendEntries replies. With no peers it never broadcasts, never gets a
	// reply, and so never commits - the entry sits uncommitted in its log. This
	// mirrors the Python reference exactly.
	c := NewCluster(1)
	leader := elect(t, c)
	if !leader.Propose("solo") {
		t.Fatal("single-node leader should accept the proposal")
	}
	c.Run(20)
	if len(c.CommittedCommands(0)) != 0 {
		t.Fatalf("single node applied %v, want nothing committed", c.CommittedCommands(0))
	}
	if leader.Log.LastIndex() != 1 {
		t.Fatalf("proposal should still sit in the log; last index = %d, want 1", leader.Log.LastIndex())
	}
	if leader.MatchIndex[0] != 1 {
		t.Fatalf("leader match index = %d, want 1", leader.MatchIndex[0])
	}
}
