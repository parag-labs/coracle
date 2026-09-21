/**
 * The replicated log: an ordered list of (term, command) entries.
 *
 * Indices are 1-based to match the Raft paper, so `entries[0]` is log index 1.
 * Index 0 is the empty sentinel "before the first entry".
 */

/** A single entry in the replicated log. The command is opaque to Raft. */
export interface LogEntry<C> {
  /** The leader's term when the entry was created. */
  term: number;
  /** The opaque client payload. */
  command: C;
}

/** The replicated log. */
export class Log<C> {
  /** The log entries, in order. */
  entries: LogEntry<C>[] = [];

  /** The index of the last entry, or 0 for an empty log. */
  lastIndex(): number {
    return this.entries.length;
  }

  /** The term of the last entry, or 0 for an empty log. */
  lastTerm(): number {
    return this.entries.length === 0 ? 0 : this.entries[this.entries.length - 1].term;
  }

  /**
   * The term of the entry at the 1-based `index`; 0 for index 0 (the sentinel)
   * or any out-of-range index.
   */
  termAt(index: number): number {
    if (index <= 0 || index > this.entries.length) {
      return 0;
    }
    return this.entries[index - 1].term;
  }

  /** The entry at the 1-based `index`. Throws if `index` is out of range. */
  get(index: number): LogEntry<C> {
    if (index < 1 || index > this.entries.length) {
      throw new RangeError(`log index ${index} out of range`);
    }
    return this.entries[index - 1];
  }

  /**
   * A copy of the entries at `index` and beyond (1-based). The copy means a
   * caller (such as an `AppendEntries` payload) never aliases the log's backing
   * array.
   */
  sliceFrom(index: number): LogEntry<C>[] {
    const start = index >= 1 ? index - 1 : 0;
    return this.entries.slice(start);
  }

  /** Appends an entry and returns the new last index. */
  append(entry: LogEntry<C>): number {
    this.entries.push(entry);
    return this.lastIndex();
  }

  /**
   * Whether the entry at `index` has exactly `term`. Index 0 always matches,
   * since both sides agree on the empty prefix.
   */
  matches(index: number, term: number): boolean {
    if (index === 0) {
      return true;
    }
    return index >= 1 && index <= this.entries.length && this.entries[index - 1].term === term;
  }

  /**
   * Appends `newEntries` after `prevIndex`, dropping any conflicting suffix
   * first. A conflict is an existing entry at the same index whose term differs;
   * matching entries are kept so committed history is never rewritten, which
   * also makes re-delivering the same entries idempotent.
   */
  truncateAndAppend(prevIndex: number, newEntries: LogEntry<C>[]): void {
    newEntries.forEach((entry, offset) => {
      const index = prevIndex + 1 + offset;
      if (index <= this.entries.length) {
        if (this.entries[index - 1].term !== entry.term) {
          this.entries.length = index - 1; // drop the conflicting suffix
          this.entries.push(entry);
        }
      } else {
        this.entries.push(entry);
      }
    });
  }

  /**
   * Raft's election restriction: whether a candidate's log (given its last index
   * and term) is at least as current as this one. A higher last term wins; on a
   * tie, the longer or equal log wins.
   */
  isUpToDate(otherLastIndex: number, otherLastTerm: number): boolean {
    if (otherLastTerm !== this.lastTerm()) {
      return otherLastTerm > this.lastTerm();
    }
    return otherLastIndex >= this.lastIndex();
  }
}
