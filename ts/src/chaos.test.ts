/**
 * The chaos suite hammers the cluster with a lossy, out-of-order, duplicating
 * network and rolling partitions, and asserts Raft's safety invariants never
 * break: at most one leader per term, and no two nodes ever apply a different
 * command at the same log index. Liveness is checked separately, only after the
 * network is fully healed.
 */

import { describe, expect, it } from "vitest";
import { Cluster } from "./cluster.ts";
import { Role } from "./node.ts";
import { Rng } from "./rng.ts";

function checkElectionSafety<C>(c: Cluster<C>): void {
  const byTerm = new Map<number, number>();
  for (const n of c.nodes) {
    if (n.role === Role.Leader) {
      byTerm.set(n.currentTerm, (byTerm.get(n.currentTerm) ?? 0) + 1);
    }
  }
  for (const [term, count] of byTerm) {
    expect(count, `two leaders in term ${term}`).toBeLessThanOrEqual(1);
  }
}

function checkLogsConsistent<C>(c: Cluster<C>, committed: Map<number, C>): void {
  for (const n of c.nodes) {
    n.applied.forEach((entry, offset) => {
      const index = offset + 1;
      if (!committed.has(index)) {
        committed.set(index, entry.command);
      }
      expect(committed.get(index), `index ${index} diverged`).toEqual(entry.command);
    });
  }
}

function drainAndAssert<C>(c: Cluster<C>, committed: Map<number, C>, steps: number): void {
  for (let i = 0; i < steps; i++) {
    c.step();
    checkElectionSafety(c);
    checkLogsConsistent(c, committed);
  }
}

/** Returns two distinct values in [0, n). */
function sampleTwo(r: Rng, n: number): [number, number] {
  const a = r.below(n);
  let b = r.below(n - 1);
  if (b >= a) {
    b += 1;
  }
  return [a, b];
}

describe("chaos", () => {
  it("keeps safety under message drops", () => {
    const committed = new Map<number, string>();
    const c = new Cluster<string>(5, { seed: 1, dropProb: 0.15 });
    const leader = c.runUntilLeader(300);
    expect(leader).not.toBeNull();
    for (let i = 0; i < 10; i++) {
      const l = c.leaderId() ?? (leader as number);
      c.node(l).propose(`cmd-${i}`);
      drainAndAssert(c, committed, 8);
    }
    drainAndAssert(c, committed, 200);
  });

  it("keeps safety under duplication and reordering", () => {
    const committed = new Map<number, string>();
    const c = new Cluster<string>(5, { seed: 7, duplicateProb: 0.25, reorder: true });
    const leader = c.runUntilLeader(300);
    expect(leader).not.toBeNull();
    for (let i = 0; i < 15; i++) {
      const l = c.leaderId() ?? (leader as number);
      c.node(l).propose(`x${i}`);
      drainAndAssert(c, committed, 6);
    }
    drainAndAssert(c, committed, 150);
  });

  it("keeps safety under rolling partitions and eventually makes progress", () => {
    const committed = new Map<number, string>();
    const r = new Rng(42);
    const c = new Cluster<string>(5, { seed: 42, dropProb: 0.05 });
    c.runUntilLeader(300);

    let proposed = 0;
    for (let i = 0; i < 40; i++) {
      const [a, b] = sampleTwo(r, 5);
      if (r.nextFloat() < 0.5) {
        c.partition(a, b);
      } else {
        c.heal(a, b);
      }
      const leader = c.leaderId();
      if (leader !== null) {
        c.node(leader).propose(`v${proposed}`);
        proposed += 1;
      }
      drainAndAssert(c, committed, 6);
    }

    c.healAll();
    drainAndAssert(c, committed, 400);

    const leader = c.runUntilLeader(200);
    expect(leader).not.toBeNull();
    const target = c.committedCommands(leader as number);
    let agree = 0;
    for (const id of [0, 1, 2, 3, 4]) {
      const applied = c.committedCommands(id);
      if (applied.length < target.length) {
        continue;
      }
      if (target.every((cmd, j) => applied[j] === cmd)) {
        agree += 1;
      }
    }
    expect(agree).toBeGreaterThanOrEqual(3);
    expect(target.length).toBeGreaterThan(0);
  });

  it("survives many random schedules (soak)", () => {
    const dropChoices = [0.0, 0.1, 0.2];
    const dupChoices = [0.0, 0.15];
    for (let s = 0; s < 25; s++) {
      const committed = new Map<number, number>();
      const r = new Rng(s);
      const drop = dropChoices[r.below(dropChoices.length)];
      const dup = dupChoices[r.below(dupChoices.length)];
      const reorder = r.nextFloat() < 0.5;
      const c = new Cluster<number>(5, { seed: s, dropProb: drop, duplicateProb: dup, reorder });
      c.runUntilLeader(300);
      for (let i = 0; i < 30; i++) {
        const leader = c.leaderId();
        if (leader !== null) {
          c.node(leader).propose(r.randint(0, 1_000_000));
        }
        drainAndAssert(c, committed, r.randint(3, 9));
      }
    }
  });
});
