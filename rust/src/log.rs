//! The replicated log: an ordered list of (term, command) entries.
//!
//! Indices are 1-based to match the Raft paper, so `entries[0]` is log index 1.
//! Index 0 is the empty sentinel "before the first entry".

/// A single entry in the replicated log. The command is opaque to Raft; it is
/// whatever the client proposed.
#[derive(Clone, Debug)]
pub struct LogEntry<C> {
    /// The leader's term when the entry was created.
    pub term: u64,
    /// The opaque client payload.
    pub command: C,
}

impl<C> LogEntry<C> {
    /// Creates an entry.
    pub fn new(term: u64, command: C) -> Self {
        LogEntry { term, command }
    }
}

/// The replicated log.
#[derive(Clone, Debug)]
pub struct Log<C> {
    /// The log entries, in order.
    pub entries: Vec<LogEntry<C>>,
}

impl<C> Default for Log<C> {
    fn default() -> Self {
        Log {
            entries: Vec::new(),
        }
    }
}

impl<C: Clone> Log<C> {
    /// Creates an empty log.
    pub fn new() -> Self {
        Log::default()
    }

    /// The index of the last entry, or 0 for an empty log.
    pub fn last_index(&self) -> usize {
        self.entries.len()
    }

    /// The term of the last entry, or 0 for an empty log.
    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.term)
    }

    /// The term of the entry at the 1-based `index`; 0 for index 0 (the sentinel)
    /// or any out-of-range index.
    pub fn term_at(&self, index: usize) -> u64 {
        if index == 0 || index > self.entries.len() {
            0
        } else {
            self.entries[index - 1].term
        }
    }

    /// The entry at the 1-based `index`. Panics if `index` is out of range,
    /// matching the reference implementation's list indexing.
    pub fn get(&self, index: usize) -> &LogEntry<C> {
        &self.entries[index - 1]
    }

    /// A copy of the entries at `index` and beyond (1-based). The copy means a
    /// caller (such as an `AppendEntries` payload) never aliases the log's
    /// backing storage.
    pub fn slice_from(&self, index: usize) -> Vec<LogEntry<C>> {
        let start = index.saturating_sub(1);
        self.entries[start..].to_vec()
    }

    /// Appends an entry and returns the new last index.
    pub fn append(&mut self, entry: LogEntry<C>) -> usize {
        self.entries.push(entry);
        self.last_index()
    }

    /// Whether the entry at `index` has exactly `term`. Index 0 always matches,
    /// since both sides agree on the empty prefix.
    pub fn matches(&self, index: usize, term: u64) -> bool {
        if index == 0 {
            return true;
        }
        index >= 1 && index <= self.entries.len() && self.entries[index - 1].term == term
    }

    /// Appends `new_entries` after `prev_index`, dropping any conflicting suffix
    /// first. A conflict is an existing entry at the same index whose term
    /// differs; matching entries are kept so committed history is never
    /// rewritten, which also makes re-delivering the same entries idempotent.
    pub fn truncate_and_append(&mut self, prev_index: usize, new_entries: &[LogEntry<C>]) {
        for (offset, entry) in new_entries.iter().enumerate() {
            let index = prev_index + 1 + offset;
            if index <= self.entries.len() {
                if self.entries[index - 1].term != entry.term {
                    self.entries.truncate(index - 1); // drop the conflicting suffix
                    self.entries.push(entry.clone());
                }
            } else {
                self.entries.push(entry.clone());
            }
        }
    }

    /// Raft's election restriction: whether a candidate's log (given its last
    /// index and term) is at least as current as this one. A higher last term
    /// wins; on a tie, the longer or equal log wins.
    pub fn is_up_to_date(&self, other_last_index: usize, other_last_term: u64) -> bool {
        if other_last_term != self.last_term() {
            return other_last_term > self.last_term();
        }
        other_last_index >= self.last_index()
    }
}
