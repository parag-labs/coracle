import { describe, expect, it } from "vitest";
import { Log, type LogEntry } from "./log.ts";

function commands<C>(entries: LogEntry<C>[]): C[] {
  return entries.map((e) => e.command);
}

describe("Log", () => {
  it("appends with one-based indices", () => {
    const log = new Log<string>();
    expect(log.lastIndex()).toBe(0);
    log.append({ term: 1, command: "a" });
    log.append({ term: 1, command: "b" });
    expect(log.lastIndex()).toBe(2);
    expect(log.get(1).command).toBe("a");
    expect(log.termAt(2)).toBe(1);
  });

  it("treats index 0 as always matching", () => {
    expect(new Log<string>().matches(0, 0)).toBe(true);
  });

  it("matches only when the term is the same", () => {
    const log = new Log<string>();
    log.append({ term: 2, command: "x" });
    expect(log.matches(1, 2)).toBe(true);
    expect(log.matches(1, 3)).toBe(false);
  });

  it("does not match an out-of-range index", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    expect(log.matches(2, 1)).toBe(false);
  });

  it("drops a conflicting suffix on truncate", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    log.append({ term: 1, command: "b" });
    log.append({ term: 1, command: "c" });
    log.truncateAndAppend(1, [
      { term: 2, command: "B" },
      { term: 2, command: "C" },
    ]);
    expect(commands(log.entries)).toEqual(["a", "B", "C"]);
    expect(log.termAt(2)).toBe(2);
  });

  it("keeps a matching prefix so re-delivery is idempotent", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    log.append({ term: 1, command: "b" });
    log.truncateAndAppend(0, [
      { term: 1, command: "a" },
      { term: 1, command: "b" },
    ]);
    expect(commands(log.entries)).toEqual(["a", "b"]);
  });

  it("appends entries beyond the current end", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    log.truncateAndAppend(1, [
      { term: 1, command: "b" },
      { term: 1, command: "c" },
    ]);
    expect(commands(log.entries)).toEqual(["a", "b", "c"]);
  });

  it("prefers a higher last term, then a longer log, when judging currency", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    log.append({ term: 2, command: "b" }); // last term 2, last index 2
    expect(log.isUpToDate(1, 3)).toBe(true); // higher term wins even if shorter
    expect(log.isUpToDate(2, 2)).toBe(true); // same term, equal length
    expect(log.isUpToDate(1, 2)).toBe(false); // same term, shorter log loses
  });

  it("returns 0 for last term and sentinel/out-of-range term lookups", () => {
    const log = new Log<string>();
    expect(log.lastTerm()).toBe(0);
    expect(log.termAt(0)).toBe(0);
    expect(log.termAt(5)).toBe(0);
  });

  it("returns an independent copy from sliceFrom", () => {
    const log = new Log<string>();
    log.append({ term: 1, command: "a" });
    log.append({ term: 1, command: "b" });
    const suffix = log.sliceFrom(1);
    expect(commands(suffix)).toEqual(["a", "b"]);
    suffix[0] = { term: 9, command: "z" };
    expect(log.get(1).command).toBe("a"); // mutating the copy must not affect the log
  });

  it("throws when getting an out-of-range index", () => {
    const log = new Log<string>();
    expect(() => log.get(1)).toThrow();
  });
});
