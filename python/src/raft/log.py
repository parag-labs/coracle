"""The replicated log: an ordered list of (term, command) entries.

Indices are 1-based to match the Raft paper, so ``entries[0]`` is log index 1.
Index 0 is the empty sentinel "before the first entry".
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: object


@dataclass
class Log:
    entries: list[LogEntry] = field(default_factory=list)

    def last_index(self) -> int:
        return len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else 0

    def term_at(self, index: int) -> int:
        """Term of the entry at 1-based ``index``; 0 for index 0 (the sentinel)."""
        if index <= 0 or index > len(self.entries):
            return 0
        return self.entries[index - 1].term

    def get(self, index: int) -> LogEntry:
        return self.entries[index - 1]

    def slice_from(self, index: int) -> list[LogEntry]:
        """Entries at ``index`` and beyond (1-based)."""
        return self.entries[index - 1 :] if index >= 1 else list(self.entries)

    def append(self, entry: LogEntry) -> int:
        self.entries.append(entry)
        return self.last_index()

    def matches(self, index: int, term: int) -> bool:
        """Does the entry at ``index`` have exactly ``term``? Index 0 always matches
        (both sides agree on the empty prefix)."""
        if index == 0:
            return True
        return 1 <= index <= len(self.entries) and self.entries[index - 1].term == term

    def truncate_and_append(self, prev_index: int, new_entries: list[LogEntry]) -> None:
        """Append ``new_entries`` after ``prev_index``, dropping any conflicting
        suffix first. A conflict is an existing entry at the same index whose term
        differs; matching entries are kept so we never rewrite committed history."""
        for offset, entry in enumerate(new_entries):
            index = prev_index + 1 + offset
            if index <= len(self.entries):
                if self.entries[index - 1].term != entry.term:
                    del self.entries[index - 1 :]  # drop the conflicting suffix
                    self.entries.append(entry)
            else:
                self.entries.append(entry)

    def is_up_to_date(self, other_last_index: int, other_last_term: int) -> bool:
        """Raft's election restriction: is a candidate's log at least as current as
        ours? A higher last term wins; on a tie, the longer log wins."""
        if other_last_term != self.last_term():
            return other_last_term > self.last_term()
        return other_last_index >= self.last_index()
