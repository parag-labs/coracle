package coracle

import "testing"

func TestSingleNodeElectsItself(t *testing.T) {
	c := NewCluster(1)
	leader := c.RunUntilLeader()
	if leader == nil {
		t.Fatal("single node should elect itself")
	}
	if leader.ID != 0 {
		t.Fatalf("leader id = %d, want 0", leader.ID)
	}
}

func TestClusterConvergesOnOneLeader(t *testing.T) {
	c := NewCluster(3)
	leader := c.RunUntilLeader()
	if leader == nil {
		t.Fatal("expected a leader")
	}
	if len(c.Leaders()) != 1 {
		t.Fatalf("leaders = %d, want 1", len(c.Leaders()))
	}
	for _, n := range c.Nodes {
		if n.CurrentTerm != leader.CurrentTerm {
			t.Fatalf("node %d term = %d, want %d", n.ID, n.CurrentTerm, leader.CurrentTerm)
		}
	}
}

func TestFiveNodeClusterAlsoConverges(t *testing.T) {
	c := NewCluster(5)
	if c.RunUntilLeader() == nil {
		t.Fatal("expected a leader")
	}
	if len(c.Leaders()) != 1 {
		t.Fatalf("leaders = %d, want 1", len(c.Leaders()))
	}
}

func TestNewLeaderEmergesAfterTheLeaderIsIsolated(t *testing.T) {
	c := NewCluster(5)
	first := c.RunUntilLeader()
	if first == nil {
		t.Fatal("expected a first leader")
	}

	c.Isolate(first.ID)
	var newLeader *RaftNode
	for i := 0; i < 300; i++ {
		c.Step()
		for _, n := range c.Leaders() {
			if n.ID != first.ID {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatal("majority should elect a new leader")
	}
	if newLeader.ID == first.ID {
		t.Fatal("new leader must differ from the isolated one")
	}
	if newLeader.CurrentTerm <= first.CurrentTerm {
		t.Fatalf("new term %d should exceed old term %d", newLeader.CurrentTerm, first.CurrentTerm)
	}
}

func TestTermIncrementsOnElectionTimeout(t *testing.T) {
	c := NewCluster(3)
	c.RunUntilLeader()
	if c.Leader().CurrentTerm < 1 {
		t.Fatalf("term = %d, want >= 1", c.Leader().CurrentTerm)
	}
}

func TestStaleCandidateIsDenied(t *testing.T) {
	voter := NewRaftNode(0, []int{0, 1}, 10, 3)
	voter.CurrentTerm = 5
	voter.Log.Append(LogEntry{Term: 5, Command: "committed"})

	stale := RequestVote{
		header:       header{src: 1, dst: 0, term: 6},
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	replies := voter.Handle(stale)
	reply := replies[0].(RequestVoteReply)
	if reply.Granted {
		t.Fatal("a stale candidate must not get the vote")
	}
	if voter.Role != Follower {
		t.Fatal("voter should adopt the higher term as a follower")
	}
	if voter.CurrentTerm != 6 {
		t.Fatalf("voter term = %d, want 6", voter.CurrentTerm)
	}
}

func TestUpToDateCandidateIsGrantedTheVote(t *testing.T) {
	voter := NewRaftNode(0, []int{0, 1}, 10, 3)
	voter.CurrentTerm = 2
	voter.Log.Append(LogEntry{Term: 1, Command: "a"})

	req := RequestVote{
		header:       header{src: 1, dst: 0, term: 3},
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	reply := voter.Handle(req)[0].(RequestVoteReply)
	if !reply.Granted {
		t.Fatal("an up-to-date candidate at a higher term should get the vote")
	}
}

func TestVoteIsNotGrantedTwiceToDifferentCandidates(t *testing.T) {
	voter := NewRaftNode(0, []int{0, 1, 2}, 10, 3)

	first := RequestVote{header: header{src: 1, dst: 0, term: 1}}
	if !voter.Handle(first)[0].(RequestVoteReply).Granted {
		t.Fatal("first candidate at a new term should get the vote")
	}
	second := RequestVote{header: header{src: 2, dst: 0, term: 1}}
	if voter.Handle(second)[0].(RequestVoteReply).Granted {
		t.Fatal("a second candidate in the same term must be denied")
	}
}
