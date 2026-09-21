package coracle

import "testing"

func commandsOf(entries []LogEntry) []any {
	out := make([]any, len(entries))
	for i, e := range entries {
		out[i] = e.Command
	}
	return out
}

func equalCommands(got []any, want ...any) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestAppendAndIndicesAreOneBased(t *testing.T) {
	log := &Log{}
	if log.LastIndex() != 0 {
		t.Fatalf("empty log last index = %d, want 0", log.LastIndex())
	}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Command: "b"})
	if log.LastIndex() != 2 {
		t.Fatalf("last index = %d, want 2", log.LastIndex())
	}
	if log.Get(1).Command != "a" {
		t.Fatalf("entry 1 = %v, want a", log.Get(1).Command)
	}
	if log.TermAt(2) != 1 {
		t.Fatalf("term at 2 = %d, want 1", log.TermAt(2))
	}
}

func TestIndexZeroAlwaysMatches(t *testing.T) {
	if !(&Log{}).Matches(0, 0) {
		t.Fatal("index 0 should always match on an empty log")
	}
}

func TestMatchesRequiresSameTerm(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 2, Command: "x"})
	if !log.Matches(1, 2) {
		t.Fatal("matches(1, 2) should be true")
	}
	if log.Matches(1, 3) {
		t.Fatal("matches(1, 3) should be false")
	}
}

func TestMatchesRejectsOutOfRangeIndex(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	if log.Matches(2, 1) {
		t.Fatal("matches past the end of the log should be false")
	}
}

func TestTruncateDropsConflictingSuffix(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Command: "b"})
	log.Append(LogEntry{Term: 1, Command: "c"})
	log.TruncateAndAppend(1, []LogEntry{{Term: 2, Command: "B"}, {Term: 2, Command: "C"}})
	if !equalCommands(commandsOf(log.Entries), "a", "B", "C") {
		t.Fatalf("entries = %v, want [a B C]", commandsOf(log.Entries))
	}
	if log.TermAt(2) != 2 {
		t.Fatalf("term at 2 = %d, want 2", log.TermAt(2))
	}
}

func TestTruncateKeepsMatchingPrefixIdempotent(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Command: "b"})
	log.TruncateAndAppend(0, []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}})
	if !equalCommands(commandsOf(log.Entries), "a", "b") {
		t.Fatalf("entries = %v, want [a b]", commandsOf(log.Entries))
	}
}

func TestTruncateAppendsBeyondEnd(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.TruncateAndAppend(1, []LogEntry{{Term: 1, Command: "b"}, {Term: 1, Command: "c"}})
	if !equalCommands(commandsOf(log.Entries), "a", "b", "c") {
		t.Fatalf("entries = %v, want [a b c]", commandsOf(log.Entries))
	}
}

func TestUpToDatePrefersHigherTermThenLongerLog(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.Append(LogEntry{Term: 2, Command: "b"}) // last term 2, last index 2
	if !log.IsUpToDate(1, 3) {
		t.Fatal("higher term should win even if shorter")
	}
	if !log.IsUpToDate(2, 2) {
		t.Fatal("same term, equal length should be up to date")
	}
	if log.IsUpToDate(1, 2) {
		t.Fatal("same term, shorter log should lose")
	}
}

func TestLastTermAndTermAtSentinels(t *testing.T) {
	log := &Log{}
	if log.LastTerm() != 0 {
		t.Fatalf("empty last term = %d, want 0", log.LastTerm())
	}
	if log.TermAt(0) != 0 || log.TermAt(5) != 0 {
		t.Fatal("term at sentinel or out-of-range index should be 0")
	}
}

func TestSliceFromReturnsIndependentCopy(t *testing.T) {
	log := &Log{}
	log.Append(LogEntry{Term: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Command: "b"})
	suffix := log.SliceFrom(1)
	if !equalCommands(commandsOf(suffix), "a", "b") {
		t.Fatalf("slice = %v, want [a b]", commandsOf(suffix))
	}
	// Mutating the returned slice must not affect the log.
	suffix[0] = LogEntry{Term: 9, Command: "z"}
	if log.Get(1).Command != "a" {
		t.Fatal("SliceFrom must return a copy, not an alias")
	}
}
