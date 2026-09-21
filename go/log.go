package coracle

// LogEntry is a single (term, command) pair in the replicated log. The command
// is opaque to Raft; it is whatever the client proposed.
type LogEntry struct {
	// Term is the leader's term when the entry was created.
	Term int
	// Command is the opaque client payload.
	Command any
}

// Log is the replicated log: an ordered list of entries. Indices are 1-based to
// match the Raft paper, so Entries[0] is log index 1 and index 0 is the empty
// sentinel "before the first entry".
type Log struct {
	// Entries holds the log entries in order.
	Entries []LogEntry
}

// LastIndex returns the index of the last entry, or 0 for an empty log.
func (l *Log) LastIndex() int {
	return len(l.Entries)
}

// LastTerm returns the term of the last entry, or 0 for an empty log.
func (l *Log) LastTerm() int {
	if len(l.Entries) == 0 {
		return 0
	}
	return l.Entries[len(l.Entries)-1].Term
}

// TermAt returns the term of the entry at the 1-based index, or 0 for index 0
// (the sentinel) or any out-of-range index.
func (l *Log) TermAt(index int) int {
	if index <= 0 || index > len(l.Entries) {
		return 0
	}
	return l.Entries[index-1].Term
}

// Get returns the entry at the 1-based index. It panics for an out-of-range
// index, matching the reference implementation's list indexing.
func (l *Log) Get(index int) LogEntry {
	return l.Entries[index-1]
}

// SliceFrom returns a copy of the entries at index and beyond (1-based). The
// copy means callers (such as an AppendEntries payload) never alias the log's
// backing storage.
func (l *Log) SliceFrom(index int) []LogEntry {
	start := 0
	if index >= 1 {
		start = index - 1
	}
	out := make([]LogEntry, len(l.Entries)-start)
	copy(out, l.Entries[start:])
	return out
}

// Append adds an entry and returns the new last index.
func (l *Log) Append(entry LogEntry) int {
	l.Entries = append(l.Entries, entry)
	return l.LastIndex()
}

// Matches reports whether the entry at index has exactly term. Index 0 always
// matches, since both sides agree on the empty prefix.
func (l *Log) Matches(index, term int) bool {
	if index == 0 {
		return true
	}
	return index >= 1 && index <= len(l.Entries) && l.Entries[index-1].Term == term
}

// TruncateAndAppend appends newEntries after prevIndex, dropping any conflicting
// suffix first. A conflict is an existing entry at the same index whose term
// differs; matching entries are kept so committed history is never rewritten,
// which also makes re-delivering the same entries idempotent.
func (l *Log) TruncateAndAppend(prevIndex int, newEntries []LogEntry) {
	for offset, entry := range newEntries {
		index := prevIndex + 1 + offset
		if index <= len(l.Entries) {
			if l.Entries[index-1].Term != entry.Term {
				l.Entries = l.Entries[:index-1] // drop the conflicting suffix
				l.Entries = append(l.Entries, entry)
			}
		} else {
			l.Entries = append(l.Entries, entry)
		}
	}
}

// IsUpToDate applies Raft's election restriction: it reports whether a
// candidate's log (given its last index and term) is at least as current as this
// one. A higher last term wins; on a tie, the longer or equal log wins.
func (l *Log) IsUpToDate(otherLastIndex, otherLastTerm int) bool {
	if otherLastTerm != l.LastTerm() {
		return otherLastTerm > l.LastTerm()
	}
	return otherLastIndex >= l.LastIndex()
}
