package coracle

import (
	"fmt"
	"testing"
)

// The chaos suite hammers the cluster with a lossy, out-of-order, duplicating
// network and rolling partitions, and asserts Raft's safety invariants never
// break: at most one leader per term, and no two nodes ever apply a different
// command at the same log index. Liveness is checked separately, only after the
// network is fully healed.

func checkElectionSafety(t *testing.T, c *Cluster) {
	t.Helper()
	leadersByTerm := map[int]int{}
	for _, n := range c.Nodes {
		if n.Role == Leader {
			leadersByTerm[n.CurrentTerm]++
		}
	}
	for term, count := range leadersByTerm {
		if count > 1 {
			t.Fatalf("two leaders in term %d", term)
		}
	}
}

func checkLogsConsistent(t *testing.T, c *Cluster, committed map[int]any) {
	t.Helper()
	for _, id := range c.ids {
		for offset, entry := range c.Nodes[id].Applied {
			index := offset + 1
			prior, ok := committed[index]
			if !ok {
				committed[index] = entry.Command
				prior = entry.Command
			}
			if prior != entry.Command {
				t.Fatalf("index %d diverged: %v vs %v", index, prior, entry.Command)
			}
		}
	}
}

func drainAndAssert(t *testing.T, c *Cluster, committed map[int]any, steps int) {
	t.Helper()
	for i := 0; i < steps; i++ {
		c.Step()
		checkElectionSafety(t, c)
		checkLogsConsistent(t, c, committed)
	}
}

func seed(v int64) *int64 { return &v }

// sampleTwo returns two distinct values in [0, n).
func sampleTwo(r *rng, n int) (int, int) {
	a := r.intn(n)
	b := r.intn(n - 1)
	if b >= a {
		b++
	}
	return a, b
}

func TestSafetyHoldsUnderMessageDrops(t *testing.T) {
	committed := map[int]any{}
	c := NewClusterWithOptions(5, ClusterOptions{Seed: seed(1), DropProb: 0.15})
	leader := c.RunUntilLeader(300)
	if leader == nil {
		t.Fatal("expected a leader even under message drops")
	}
	for i := 0; i < 10; i++ {
		l := c.Leader()
		if l == nil {
			l = leader
		}
		l.Propose(fmt.Sprintf("cmd-%d", i))
		drainAndAssert(t, c, committed, 8)
	}
	drainAndAssert(t, c, committed, 200)
}

func TestSafetyHoldsUnderDuplicationAndReorder(t *testing.T) {
	committed := map[int]any{}
	c := NewClusterWithOptions(5, ClusterOptions{Seed: seed(7), DuplicateProb: 0.25, Reorder: true})
	leader := c.RunUntilLeader(300)
	if leader == nil {
		t.Fatal("expected a leader even under duplication and reordering")
	}
	for i := 0; i < 15; i++ {
		l := c.Leader()
		if l == nil {
			l = leader
		}
		l.Propose(fmt.Sprintf("x%d", i))
		drainAndAssert(t, c, committed, 6)
	}
	drainAndAssert(t, c, committed, 150)
}

func TestRollingPartitionsKeepSafetyAndEventuallyMakeProgress(t *testing.T) {
	committed := map[int]any{}
	r := newRNG(42)
	c := NewClusterWithOptions(5, ClusterOptions{Seed: seed(42), DropProb: 0.05})
	c.RunUntilLeader(300)

	proposed := 0
	for i := 0; i < 40; i++ {
		a, b := sampleTwo(r, 5)
		if r.float64() < 0.5 {
			c.Partition(a, b)
		} else {
			c.Heal(a, b)
		}
		if leader := c.Leader(); leader != nil {
			leader.Propose(fmt.Sprintf("v%d", proposed))
			proposed++
		}
		drainAndAssert(t, c, committed, 6)
	}

	c.HealAll()
	drainAndAssert(t, c, committed, 400)

	leader := c.RunUntilLeader(200)
	if leader == nil {
		t.Fatal("cluster should elect a leader once healed")
	}
	target := commandsOf(leader.Applied)
	agree := 0
	for _, id := range c.ids {
		applied := commandsOf(c.Nodes[id].Applied)
		if len(applied) < len(target) {
			continue
		}
		match := true
		for j := range target {
			if applied[j] != target[j] {
				match = false
				break
			}
		}
		if match {
			agree++
		}
	}
	if agree < 3 {
		t.Fatalf("only %d/5 nodes converged", agree)
	}
	if len(target) == 0 {
		t.Fatal("cluster made no progress at all")
	}
}

func TestSoakManyRandomSchedules(t *testing.T) {
	dropChoices := []float64{0.0, 0.1, 0.2}
	dupChoices := []float64{0.0, 0.15}
	for s := 0; s < 25; s++ {
		committed := map[int]any{}
		r := newRNG(uint64(s))
		drop := dropChoices[r.intn(len(dropChoices))]
		dup := dupChoices[r.intn(len(dupChoices))]
		reorder := r.float64() < 0.5
		sv := int64(s)
		c := NewClusterWithOptions(5, ClusterOptions{
			Seed:          &sv,
			DropProb:      drop,
			DuplicateProb: dup,
			Reorder:       reorder,
		})
		c.RunUntilLeader(300)
		for i := 0; i < 30; i++ {
			if leader := c.Leader(); leader != nil {
				leader.Propose(r.randint(0, 1_000_000))
			}
			drainAndAssert(t, c, committed, r.randint(3, 9))
		}
	}
}
