import { describe, expect, it } from "vitest";
import { Cluster } from "./cluster.ts";

function elect(c: Cluster<string>): number {
  const leader = c.runUntilLeader();
  if (leader === null) {
    throw new Error("expected a leader to be elected");
  }
  return leader;
}

describe("replication", () => {
  it("replicates proposals and applies them in order everywhere", () => {
    const c = new Cluster<string>(3);
    const leader = elect(c);
    expect(c.node(leader).propose("x=1")).toBe(true);
    expect(c.node(leader).propose("x=2")).toBe(true);
    c.run(40);

    for (const id of [0, 1, 2]) {
      expect(c.committedCommands(id)).toEqual(["x=1", "x=2"]);
    }
  });

  it("rejects client proposals on a follower", () => {
    const c = new Cluster<string>(3);
    const leader = elect(c);
    const follower = [0, 1, 2].find((id) => id !== leader) as number;
    expect(c.node(follower).propose("nope")).toBe(false);
  });

  it("commits once a majority stores the entry", () => {
    const c = new Cluster<string>(5);
    const leader = elect(c);
    c.node(leader).propose("a");
    c.run(50);
    const committedOn = [0, 1, 2, 3, 4].filter(
      (id) => c.committedCommands(id).length === 1 && c.committedCommands(id)[0] === "a",
    ).length;
    expect(committedOn).toBeGreaterThanOrEqual(3);
  });

  it("lets a lagging follower catch up after healing", () => {
    const c = new Cluster<string>(3, { heartbeatInterval: 2 });
    const leader = elect(c);
    const lagging = [0, 1, 2].find((id) => id !== leader) as number;

    c.isolate(lagging);
    for (const cmd of ["c1", "c2", "c3"]) {
      c.node(leader).propose(cmd);
    }
    c.run(60);
    expect(c.committedCommands(lagging)).toHaveLength(0);

    c.healAll();
    c.run(80);
    expect(c.committedCommands(lagging)).toEqual(["c1", "c2", "c3"]);
  });

  it("cannot commit from a minority partition", () => {
    const c = new Cluster<string>(5);
    const leader = elect(c);
    const others = [0, 1, 2, 3, 4].filter((id) => id !== leader);
    // Cut the leader down to itself plus one follower (a minority of 2 of 5).
    for (const nid of others.slice(1)) {
      c.partition(leader, nid);
    }

    c.node(leader).propose("lonely");
    c.run(60);
    for (const id of [0, 1, 2, 3, 4]) {
      expect(c.committedCommands(id)).not.toContain("lonely");
    }
  });

  it("accepts a proposal on a lone node but never commits it", () => {
    // A lone node elects itself, but commit advancement is driven solely by
    // AppendEntries replies. With no peers it never broadcasts, never gets a
    // reply, and so never commits - the entry sits uncommitted in its log. This
    // mirrors the Python reference exactly.
    const c = new Cluster<string>(1);
    const leader = elect(c);
    expect(c.node(leader).propose("solo")).toBe(true);
    c.run(20);
    expect(c.committedCommands(0)).toHaveLength(0);
    expect(c.node(0).log.lastIndex()).toBe(1);
    expect(c.node(0).matchIndex.get(0)).toBe(1);
  });
});
