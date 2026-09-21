package coracle

// Role is the current role of a node in the Raft protocol.
type Role int

const (
	// Follower passively replicates entries and grants votes.
	Follower Role = iota
	// Candidate is standing for election.
	Candidate
	// Leader replicates log entries to the followers.
	Leader
)

// String returns the lowercase role name, matching the reference implementation.
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// RaftNode is a single deterministic Raft state machine. It never touches a clock
// or a socket: Tick advances its timers and Handle processes one inbound message,
// each returning the messages the node wants sent.
type RaftNode struct {
	// ID is this node's identifier.
	ID int
	// Peers are the other node ids in the cluster (this node's id excluded).
	Peers []int
	// ElectionTimeout is how many ticks without contact trigger an election.
	ElectionTimeout int
	// HeartbeatInterval is how often a leader broadcasts heartbeats.
	HeartbeatInterval int

	// CurrentTerm is the latest term the node has seen.
	CurrentTerm int
	// Log is the node's replicated log.
	Log *Log

	// Role is the node's current role.
	Role Role
	// CommitIndex is the highest index known to be committed.
	CommitIndex int
	// LastApplied is the highest index handed to the state machine.
	LastApplied int
	// Applied is the ordered list of entries applied to the state machine.
	Applied []LogEntry

	// NextIndex tracks, per follower, the next log index to send (leader state).
	NextIndex map[int]int
	// MatchIndex tracks, per node, the highest replicated index (leader state).
	MatchIndex map[int]int

	votedFor         int // -1 means "no vote cast this term"
	votes            map[int]struct{}
	electionElapsed  int
	heartbeatElapsed int
}

// NewRaftNode creates a follower with the given id, peer ids (which may include
// its own id, which is filtered out), and timers.
func NewRaftNode(id int, peers []int, electionTimeout, heartbeatInterval int) *RaftNode {
	filtered := make([]int, 0, len(peers))
	for _, p := range peers {
		if p != id {
			filtered = append(filtered, p)
		}
	}
	return &RaftNode{
		ID:                id,
		Peers:             filtered,
		ElectionTimeout:   electionTimeout,
		HeartbeatInterval: heartbeatInterval,
		Log:               &Log{},
		Role:              Follower,
		votedFor:          -1,
		votes:             map[int]struct{}{},
		NextIndex:         map[int]int{},
		MatchIndex:        map[int]int{},
	}
}

// ---- driving the node ----

// Tick advances the node's timers by one step. A leader may emit heartbeats; a
// follower or candidate may start an election. It returns any messages to send.
func (n *RaftNode) Tick() []Message {
	if n.Role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.HeartbeatInterval {
			n.heartbeatElapsed = 0
			return n.broadcastAppend()
		}
		return nil
	}

	n.electionElapsed++
	if n.electionElapsed >= n.ElectionTimeout {
		return n.startElection()
	}
	return nil
}

// Handle processes one inbound RPC and returns the replies to send.
func (n *RaftNode) Handle(msg Message) []Message {
	// Any message from a higher term makes us a follower of that term.
	if msg.Term() > n.CurrentTerm {
		n.becomeFollower(msg.Term())
	}

	switch m := msg.(type) {
	case RequestVote:
		return []Message{n.onRequestVote(m)}
	case RequestVoteReply:
		return n.onRequestVoteReply(m)
	case AppendEntries:
		return []Message{n.onAppendEntries(m)}
	case AppendEntriesReply:
		return n.onAppendEntriesReply(m)
	}
	return nil
}

// Propose is the client entry point. Only a leader accepts a proposal; it
// appends to its own log and lets the next heartbeat replicate it. It reports
// whether the proposal was accepted.
func (n *RaftNode) Propose(command any) bool {
	if n.Role != Leader {
		return false
	}
	n.Log.Append(LogEntry{Term: n.CurrentTerm, Command: command})
	n.MatchIndex[n.ID] = n.Log.LastIndex()
	return true
}

// ---- role transitions ----

func (n *RaftNode) becomeFollower(term int) {
	n.CurrentTerm = term
	n.votedFor = -1
	n.Role = Follower
	n.electionElapsed = 0
	n.votes = map[int]struct{}{}
}

func (n *RaftNode) becomeCandidate() {
	n.CurrentTerm++
	n.Role = Candidate
	n.votedFor = n.ID
	n.votes = map[int]struct{}{n.ID: {}}
	n.electionElapsed = 0
}

func (n *RaftNode) becomeLeader() {
	n.Role = Leader
	n.heartbeatElapsed = 0
	last := n.Log.LastIndex()
	n.NextIndex = map[int]int{}
	n.MatchIndex = map[int]int{}
	for _, p := range n.Peers {
		n.NextIndex[p] = last + 1
		n.MatchIndex[p] = 0
	}
	n.MatchIndex[n.ID] = last
}

// ---- elections ----

func (n *RaftNode) startElection() []Message {
	n.becomeCandidate()
	if len(n.Peers) == 0 { // single-node cluster: instant win
		n.becomeLeader()
		return nil
	}
	out := make([]Message, 0, len(n.Peers))
	for _, p := range n.Peers {
		out = append(out, RequestVote{
			header:       header{src: n.ID, dst: p, term: n.CurrentTerm},
			LastLogIndex: n.Log.LastIndex(),
			LastLogTerm:  n.Log.LastTerm(),
		})
	}
	return out
}

func (n *RaftNode) onRequestVote(msg RequestVote) Message {
	granted := false
	if msg.Term() >= n.CurrentTerm {
		free := n.votedFor == -1 || n.votedFor == msg.Src()
		current := n.Log.IsUpToDate(msg.LastLogIndex, msg.LastLogTerm)
		if free && current {
			granted = true
			n.votedFor = msg.Src()
			n.electionElapsed = 0
		}
	}
	return RequestVoteReply{
		header:  header{src: n.ID, dst: msg.Src(), term: n.CurrentTerm},
		Granted: granted,
	}
}

func (n *RaftNode) onRequestVoteReply(msg RequestVoteReply) []Message {
	if n.Role != Candidate || msg.Term() != n.CurrentTerm {
		return nil
	}
	if msg.Granted {
		n.votes[msg.Src()] = struct{}{}
		if len(n.votes) >= n.majority() {
			n.becomeLeader()
			return n.broadcastAppend()
		}
	}
	return nil
}

// ---- log replication ----

func (n *RaftNode) broadcastAppend() []Message {
	out := make([]Message, 0, len(n.Peers))
	for _, p := range n.Peers {
		out = append(out, n.appendFor(p))
	}
	return out
}

func (n *RaftNode) appendFor(peer int) AppendEntries {
	nextIdx, ok := n.NextIndex[peer]
	if !ok {
		nextIdx = n.Log.LastIndex() + 1
	}
	prevIndex := nextIdx - 1
	return AppendEntries{
		header:       header{src: n.ID, dst: peer, term: n.CurrentTerm},
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.Log.TermAt(prevIndex),
		Entries:      n.Log.SliceFrom(nextIdx),
		LeaderCommit: n.CommitIndex,
	}
}

func (n *RaftNode) onAppendEntries(msg AppendEntries) Message {
	reply := AppendEntriesReply{
		header:  header{src: n.ID, dst: msg.Src(), term: n.CurrentTerm},
		Success: false,
	}
	if msg.Term() < n.CurrentTerm {
		return reply // stale leader
	}

	// Valid current-term leader: defer to it and reset our election timer.
	n.Role = Follower
	n.electionElapsed = 0

	if !n.Log.Matches(msg.PrevLogIndex, msg.PrevLogTerm) {
		return reply // log gap/conflict at prev; leader will back off
	}

	n.Log.TruncateAndAppend(msg.PrevLogIndex, msg.Entries)
	if msg.LeaderCommit > n.CommitIndex {
		n.CommitIndex = min(msg.LeaderCommit, n.Log.LastIndex())
		n.applyCommitted()
	}

	reply.Success = true
	reply.MatchIndex = msg.PrevLogIndex + len(msg.Entries)
	return reply
}

func (n *RaftNode) onAppendEntriesReply(msg AppendEntriesReply) []Message {
	if n.Role != Leader || msg.Term() != n.CurrentTerm {
		return nil
	}
	if msg.Success {
		n.MatchIndex[msg.Src()] = msg.MatchIndex
		n.NextIndex[msg.Src()] = msg.MatchIndex + 1
		n.advanceCommit()
		return nil
	}
	// Rejected: step next_index back and retry.
	cur, ok := n.NextIndex[msg.Src()]
	if !ok {
		cur = 1
	}
	n.NextIndex[msg.Src()] = max(1, cur-1)
	return []Message{n.appendFor(msg.Src())}
}

// advanceCommit commits index N when a majority stores it AND it is from the
// current term (the paper's safety rule against committing stale entries).
func (n *RaftNode) advanceCommit() {
	for idx := n.Log.LastIndex(); idx > n.CommitIndex; idx-- {
		if n.Log.TermAt(idx) != n.CurrentTerm {
			continue
		}
		replicas := 1
		for _, p := range n.Peers {
			if n.MatchIndex[p] >= idx {
				replicas++
			}
		}
		if replicas >= n.majority() {
			n.CommitIndex = idx
			n.applyCommitted()
			break
		}
	}
}

func (n *RaftNode) applyCommitted() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		n.Applied = append(n.Applied, n.Log.Get(n.LastApplied))
	}
}

func (n *RaftNode) majority() int {
	return (len(n.Peers)+1)/2 + 1
}
