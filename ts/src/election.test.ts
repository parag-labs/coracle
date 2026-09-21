import { describe, expect, it } from "vitest";
import { Cluster } from "./cluster.ts";
import { RaftNode, Role } from "./node.ts";
import type { Message, RequestVoteReply } from "./messages.ts";

function granted<C>(replies: Message<C>[]): boolean {
  const reply = replies[0];
  if (reply.kind !== "RequestVoteReply") {
    throw new Error("expected a RequestVoteReply");
  }
  return (reply as RequestVoteReply).granted;
}

describe("elections", () => {
  it("lets a single node elect itself", () => {
    const c = new Cluster<string>(1);
    expect(c.runUntilLeader()).toBe(0);
  });

  it("converges on exactly one leader in a three-node cluster", () => {
    const c = new Cluster<string>(3);
    const leader = c.runUntilLeader();
    expect(leader).not.toBeNull();
    expect(c.leaders()).toHaveLength(1);
    const term = c.node(leader as number).currentTerm;
    for (const n of c.nodes) {
      expect(n.currentTerm).toBe(term);
    }
  });

  it("converges in a five-node cluster too", () => {
    const c = new Cluster<string>(5);
    expect(c.runUntilLeader()).not.toBeNull();
    expect(c.leaders()).toHaveLength(1);
  });

  it("elects a new leader after the first one is isolated", () => {
    const c = new Cluster<string>(5);
    const first = c.runUntilLeader();
    expect(first).not.toBeNull();
    const firstId = first as number;
    const firstTerm = c.node(firstId).currentTerm;

    c.isolate(firstId);
    let newLeader: number | null = null;
    for (let i = 0; i < 300; i++) {
      c.step();
      const other = c.leaders().find((id) => id !== firstId);
      if (other !== undefined) {
        newLeader = other;
        break;
      }
    }
    expect(newLeader).not.toBeNull();
    expect(newLeader).not.toBe(firstId);
    expect(c.node(newLeader as number).currentTerm).toBeGreaterThan(firstTerm);
  });

  it("increments the term to at least one on an election", () => {
    const c = new Cluster<string>(3);
    c.runUntilLeader();
    const leader = c.leaderId();
    expect(leader).not.toBeNull();
    expect(c.node(leader as number).currentTerm).toBeGreaterThanOrEqual(1);
  });

  it("denies a stale candidate and adopts its higher term", () => {
    const voter = new RaftNode<string>(0, [0, 1], 10, 3);
    voter.currentTerm = 5;
    voter.log.append({ term: 5, command: "committed" });

    const stale: Message<string> = {
      kind: "RequestVote",
      src: 1,
      dst: 0,
      term: 6,
      lastLogIndex: 0,
      lastLogTerm: 0,
    };
    const replies = voter.handle(stale);
    expect(granted(replies)).toBe(false);
    expect(voter.role).toBe(Role.Follower);
    expect(voter.currentTerm).toBe(6);
  });

  it("grants the vote to an up-to-date candidate at a higher term", () => {
    const voter = new RaftNode<string>(0, [0, 1], 10, 3);
    voter.currentTerm = 2;
    voter.log.append({ term: 1, command: "a" });

    const req: Message<string> = {
      kind: "RequestVote",
      src: 1,
      dst: 0,
      term: 3,
      lastLogIndex: 1,
      lastLogTerm: 1,
    };
    expect(granted(voter.handle(req))).toBe(true);
  });

  it("does not grant a vote twice to different candidates in one term", () => {
    const voter = new RaftNode<string>(0, [0, 1, 2], 10, 3);

    const first: Message<string> = { kind: "RequestVote", src: 1, dst: 0, term: 1, lastLogIndex: 0, lastLogTerm: 0 };
    expect(granted(voter.handle(first))).toBe(true);

    const second: Message<string> = { kind: "RequestVote", src: 2, dst: 0, term: 1, lastLogIndex: 0, lastLogTerm: 0 };
    expect(granted(voter.handle(second))).toBe(false);
  });
});
