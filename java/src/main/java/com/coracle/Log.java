// The replicated log: an ordered list of (term, command) entries.
//
// Indices are 1-based to match the Raft paper, so entries.get(0) is log index 1.
// Index 0 is the empty sentinel "before the first entry".

package com.coracle;

import java.util.ArrayList;
import java.util.List;

public final class Log {

    public record Entry(int term, Object command) {}

    private final List<Entry> entries = new ArrayList<>();

    public List<Entry> entries() {
        return entries;
    }

    public int lastIndex() {
        return entries.size();
    }

    public int lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    /** Term of the entry at 1-based index; 0 for index 0 (the sentinel). */
    public int termAt(int index) {
        if (index <= 0 || index > entries.size()) return 0;
        return entries.get(index - 1).term();
    }

    public Entry get(int index) {
        return entries.get(index - 1);
    }

    /** Entries at index and beyond (1-based). */
    public List<Entry> sliceFrom(int index) {
        if (index >= 1) return new ArrayList<>(entries.subList(index - 1, entries.size()));
        return new ArrayList<>(entries);
    }

    public int append(Entry entry) {
        entries.add(entry);
        return lastIndex();
    }

    /** Does the entry at index have exactly term? Index 0 always matches
     * (both sides agree on the empty prefix). */
    public boolean matches(int index, int term) {
        if (index == 0) return true;
        return index >= 1 && index <= entries.size() && entries.get(index - 1).term() == term;
    }

    /** Append newEntries after prevIndex, dropping any conflicting suffix first.
     * A conflict is an existing entry at the same index whose term differs;
     * matching entries are kept so we never rewrite committed history. */
    public void truncateAndAppend(int prevIndex, List<Entry> newEntries) {
        for (int offset = 0; offset < newEntries.size(); offset++) {
            int index = prevIndex + 1 + offset;
            if (index <= entries.size()) {
                if (entries.get(index - 1).term() != newEntries.get(offset).term()) {
                    // drop the conflicting suffix
                    entries.subList(index - 1, entries.size()).clear();
                    entries.add(newEntries.get(offset));
                }
            } else {
                entries.add(newEntries.get(offset));
            }
        }
    }

    /** Raft's election restriction: is a candidate's log at least as current as
     * ours? A higher last term wins; on a tie, the longer log wins. */
    public boolean isUpToDate(int otherLastIndex, int otherLastTerm) {
        if (otherLastTerm != lastTerm()) return otherLastTerm > lastTerm();
        return otherLastIndex >= lastIndex();
    }
}
