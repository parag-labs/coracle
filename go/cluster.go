package coracle

// Cluster is a deterministic in-memory cluster that drives time and delivers
// messages between nodes. There are no real timers or sockets: Step advances
// every node one step and routes whatever messages they emit. With the fault
// knobs off, NewCluster(size) is fully deterministic and replays identically
// every run; a seed enables dropping, duplicating, and reordering messages.
type Cluster struct {
	// Nodes maps node id to node.
	Nodes map[int]*RaftNode
	// Time is the number of steps executed.
	Time int
	// Dropped counts the messages the network threw away.
	Dropped int

	ids           []int
	rng           *rng
	dropProb      float64
	duplicateProb float64
	reorder       bool

	inbox       []Message
	partitioned map[[2]int]bool
}

// ClusterOptions configures a Cluster. Zero values mean "use the default":
// ElectionTimeout defaults to 10 and HeartbeatInterval to 3. A nil Seed keeps
// the cluster fully deterministic; a non-nil Seed enables the fault knobs and
// randomizes election timeouts in [T, 2T-1] like the paper recommends.
type ClusterOptions struct {
	ElectionTimeout   int
	HeartbeatInterval int
	Seed              *int64
	DropProb          float64
	DuplicateProb     float64
	Reorder           bool
}

// NewCluster builds a cluster of the given size with default timers and no
// faults, matching the reference implementation's plain Cluster(n).
func NewCluster(size int) *Cluster {
	return NewClusterWithOptions(size, ClusterOptions{})
}

// NewClusterWithOptions builds a cluster with explicit options.
func NewClusterWithOptions(size int, opts ClusterOptions) *Cluster {
	electionTimeout := opts.ElectionTimeout
	if electionTimeout == 0 {
		electionTimeout = 10
	}
	heartbeatInterval := opts.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 3
	}

	ids := make([]int, size)
	for i := range ids {
		ids[i] = i
	}

	c := &Cluster{
		Nodes:         map[int]*RaftNode{},
		ids:           ids,
		dropProb:      opts.DropProb,
		duplicateProb: opts.DuplicateProb,
		reorder:       opts.Reorder,
		partitioned:   map[[2]int]bool{},
	}
	if opts.Seed != nil {
		c.rng = newRNG(uint64(*opts.Seed))
	}

	// Stagger election timeouts by node id so the cluster doesn't split the vote
	// forever; a deterministic per-node offset keeps the simulation reproducible.
	// When a seed is given we randomize timeouts in [T, 2T-1] instead.
	for _, i := range ids {
		timeout := electionTimeout + i*2
		if c.rng != nil {
			timeout = c.rng.randint(electionTimeout, 2*electionTimeout-1)
		}
		c.Nodes[i] = NewRaftNode(i, ids, timeout, heartbeatInterval)
	}
	return c
}

// ---- network control ----

// key normalizes an unordered pair of node ids.
func key(a, b int) [2]int {
	if a < b {
		return [2]int{a, b}
	}
	return [2]int{b, a}
}

// Partition cuts the link between a and b in both directions.
func (c *Cluster) Partition(a, b int) { c.partitioned[key(a, b)] = true }

// Heal restores the link between a and b.
func (c *Cluster) Heal(a, b int) { delete(c.partitioned, key(a, b)) }

// HealAll restores every cut link.
func (c *Cluster) HealAll() { c.partitioned = map[[2]int]bool{} }

// Isolate cuts every link to the given node.
func (c *Cluster) Isolate(node int) {
	for _, other := range c.ids {
		if other != node {
			c.Partition(node, other)
		}
	}
}

func (c *Cluster) reachable(src, dst int) bool {
	return !c.partitioned[key(src, dst)]
}

func (c *Cluster) hasNode(id int) bool {
	_, ok := c.Nodes[id]
	return ok
}

// ---- driving time ----

// Step advances one tick: it delivers queued messages (optionally dropping,
// duplicating, or reordering them under a seed) and then ticks every node.
func (c *Cluster) Step() {
	c.Time++

	pending := c.inbox
	c.inbox = nil

	if c.reorder && c.rng != nil && len(pending) > 1 {
		c.rng.shuffle(len(pending), func(i, j int) {
			pending[i], pending[j] = pending[j], pending[i]
		})
	}

	for _, msg := range pending {
		if !(c.reachable(msg.Src(), msg.Dst()) && c.hasNode(msg.Dst())) {
			continue
		}
		if c.rng != nil && c.dropProb > 0 && c.rng.float64() < c.dropProb {
			c.Dropped++
			continue
		}
		deliveries := 1
		if c.rng != nil && c.duplicateProb > 0 && c.rng.float64() < c.duplicateProb {
			deliveries = 2 // deliver the same message twice
		}
		for d := 0; d < deliveries; d++ {
			c.enqueue(c.Nodes[msg.Dst()].Handle(msg))
		}
	}

	for _, i := range c.ids {
		c.enqueue(c.Nodes[i].Tick())
	}
}

// Run advances the cluster by the given number of steps.
func (c *Cluster) Run(steps int) {
	for i := 0; i < steps; i++ {
		c.Step()
	}
}

// RunUntilLeader steps until a unique leader emerges or the step budget is
// exhausted, returning the leader or nil. The budget defaults to 200 steps.
func (c *Cluster) RunUntilLeader(maxSteps ...int) *RaftNode {
	steps := 200
	if len(maxSteps) > 0 {
		steps = maxSteps[0]
	}
	for i := 0; i < steps; i++ {
		c.Step()
		if leader := c.Leader(); leader != nil {
			return leader
		}
	}
	return nil
}

func (c *Cluster) enqueue(messages []Message) {
	for _, m := range messages {
		if c.reachable(m.Src(), m.Dst()) {
			c.inbox = append(c.inbox, m)
		}
	}
}

// ---- inspection ----

// Leader returns the unique current-term leader, or nil if there is a split or
// no leader.
func (c *Cluster) Leader() *RaftNode {
	leaders := c.Leaders()
	if len(leaders) == 0 {
		return nil
	}
	topTerm := leaders[0].CurrentTerm
	for _, n := range leaders {
		if n.CurrentTerm > topTerm {
			topTerm = n.CurrentTerm
		}
	}
	var top []*RaftNode
	for _, n := range leaders {
		if n.CurrentTerm == topTerm {
			top = append(top, n)
		}
	}
	if len(top) == 1 {
		return top[0]
	}
	return nil
}

// Leaders returns every node that currently believes it is the leader.
func (c *Cluster) Leaders() []*RaftNode {
	var out []*RaftNode
	for _, i := range c.ids {
		if c.Nodes[i].Role == Leader {
			out = append(out, c.Nodes[i])
		}
	}
	return out
}

// CommittedCommands returns the commands applied on the given node, in order.
func (c *Cluster) CommittedCommands(nodeID int) []any {
	node := c.Nodes[nodeID]
	out := make([]any, 0, len(node.Applied))
	for _, e := range node.Applied {
		out = append(out, e.Command)
	}
	return out
}
