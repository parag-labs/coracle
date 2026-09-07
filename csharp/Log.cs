// The replicated log: an ordered list of (term, command) entries.
//
// Indices are 1-based to match the Raft paper, so entries[0] is log index 1.
// Index 0 is the empty sentinel "before the first entry".

namespace MiniRaft;

public sealed record LogEntry(int Term, object Command);

public sealed class Log
{
    public List<LogEntry> Entries { get; } = new();

    public int LastIndex() => Entries.Count;

    public int LastTerm() => Entries.Count > 0 ? Entries[^1].Term : 0;

    /// <summary>Term of the entry at 1-based index; 0 for index 0 (the sentinel).</summary>
    public int TermAt(int index)
    {
        if (index <= 0 || index > Entries.Count) return 0;
        return Entries[index - 1].Term;
    }

    public LogEntry Get(int index) => Entries[index - 1];

    /// <summary>Entries at index and beyond (1-based).</summary>
    public List<LogEntry> SliceFrom(int index) =>
        index >= 1 ? Entries.GetRange(index - 1, Entries.Count - (index - 1)) : new List<LogEntry>(Entries);

    public int Append(LogEntry entry)
    {
        Entries.Add(entry);
        return LastIndex();
    }

    /// <summary>Does the entry at index have exactly term? Index 0 always matches
    /// (both sides agree on the empty prefix).</summary>
    public bool Matches(int index, int term)
    {
        if (index == 0) return true;
        return index >= 1 && index <= Entries.Count && Entries[index - 1].Term == term;
    }

    /// <summary>Append newEntries after prevIndex, dropping any conflicting suffix
    /// first. A conflict is an existing entry at the same index whose term differs;
    /// matching entries are kept so we never rewrite committed history.</summary>
    public void TruncateAndAppend(int prevIndex, List<LogEntry> newEntries)
    {
        for (int offset = 0; offset < newEntries.Count; offset++)
        {
            int index = prevIndex + 1 + offset;
            if (index <= Entries.Count)
            {
                if (Entries[index - 1].Term != newEntries[offset].Term)
                {
                    Entries.RemoveRange(index - 1, Entries.Count - (index - 1)); // drop conflicting suffix
                    Entries.Add(newEntries[offset]);
                }
            }
            else
            {
                Entries.Add(newEntries[offset]);
            }
        }
    }

    /// <summary>Raft's election restriction: is a candidate's log at least as current
    /// as ours? A higher last term wins; on a tie, the longer log wins.</summary>
    public bool IsUpToDate(int otherLastIndex, int otherLastTerm)
    {
        if (otherLastTerm != LastTerm()) return otherLastTerm > LastTerm();
        return otherLastIndex >= LastIndex();
    }
}
