package coracle

// Message is an RPC exchanged between nodes. Concrete messages embed a header
// carrying the common source, destination, and term fields, so any message can
// be routed by the cluster without knowing its concrete type.
type Message interface {
	// Src is the id of the node that sent the message.
	Src() int
	// Dst is the id of the node the message is addressed to.
	Dst() int
	// Term is the sender's term at the time the message was created.
	Term() int
}

// header holds the fields common to every message. It is embedded by each
// concrete message type and supplies the Message interface methods.
type header struct {
	src  int
	dst  int
	term int
}

func (h header) Src() int  { return h.src }
func (h header) Dst() int  { return h.dst }
func (h header) Term() int { return h.term }

// RequestVote is sent by a candidate to gather votes for an election.
type RequestVote struct {
	header
	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex int
	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm int
}

// RequestVoteReply answers a RequestVote, granting or denying the vote.
type RequestVoteReply struct {
	header
	// Granted reports whether the vote was given.
	Granted bool
}

// AppendEntries replicates log entries from the leader and doubles as a
// heartbeat when it carries no entries.
type AppendEntries struct {
	header
	// PrevLogIndex is the index immediately preceding Entries.
	PrevLogIndex int
	// PrevLogTerm is the term of the entry at PrevLogIndex.
	PrevLogTerm int
	// Entries are the new entries to store (empty for a heartbeat).
	Entries []LogEntry
	// LeaderCommit is the leader's commit index.
	LeaderCommit int
}

// AppendEntriesReply answers an AppendEntries request.
type AppendEntriesReply struct {
	header
	// Success reports whether the entries were accepted.
	Success bool
	// MatchIndex is, on success, the follower's last index, so the leader can
	// advance its match index for that follower.
	MatchIndex int
}
