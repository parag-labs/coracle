/**
 * A single Raft node.
 *
 * The node is a deterministic state machine. It never reads a clock or sends on
 * a socket itself - it only reacts: {@link RaftNode.tick} advances its internal
 * timers and may start an election or emit heartbeats, and
 * {@link RaftNode.handle} processes one inbound RPC. Both return the messages to
 * send. Keeping I/O out of the node is what lets the simulator replay elections
 * and log replication deterministically.
 */

import { Log, type LogEntry } from "./log.ts";
import type { Message } from "./messages.ts";

/** The current role of a node in the Raft protocol. */
export enum Role {
  Follower = "follower",
  Candidate = "candidate",
  Leader = "leader",
}

/** A single deterministic Raft state machine. */
export class RaftNode<C> {
  /** The other node ids in the cluster (this node's id excluded). */
  readonly peers: number[];

  /** The latest term the node has seen. */
  currentTerm = 0;
  /** The candidate voted for in the current term, or null. */
  votedFor: number | null = null;
  /** The node's replicated log. */
  log = new Log<C>();

  /** The node's current role. */
  role: Role = Role.Follower;
  /** The highest index known to be committed. */
  commitIndex = 0;
  /** The highest index handed to the state machine. */
  lastApplied = 0;
  /** The ordered list of entries applied to the state machine. */
  applied: LogEntry<C>[] = [];

  /** Per-follower next index to send (leader state). */
  nextIndex = new Map<number, number>();
  /** Per-node highest replicated index (leader state). */
  matchIndex = new Map<number, number>();

  private votes = new Set<number>();
  private electionElapsed = 0;
  private heartbeatElapsed = 0;

  /**
   * Creates a follower with the given id, peer ids (its own id is filtered out),
   * and timers.
   */
  constructor(
    readonly id: number,
    peers: number[],
    readonly electionTimeout: number,
    readonly heartbeatInterval: number,
  ) {
    this.peers = peers.filter((p) => p !== id);
  }

  // ---- driving the node ----

  /**
   * Advances the node's timers by one step. A leader may emit heartbeats; a
   * follower or candidate may start an election. Returns any messages to send.
   */
  tick(): Message<C>[] {
    if (this.role === Role.Leader) {
      this.heartbeatElapsed += 1;
      if (this.heartbeatElapsed >= this.heartbeatInterval) {
        this.heartbeatElapsed = 0;
        return this.broadcastAppend();
      }
      return [];
    }

    this.electionElapsed += 1;
    if (this.electionElapsed >= this.electionTimeout) {
      return this.startElection();
    }
    return [];
  }

  /** Processes one inbound RPC and returns the replies to send. */
  handle(msg: Message<C>): Message<C>[] {
    // Any message from a higher term makes us a follower of that term.
    if (msg.term > this.currentTerm) {
      this.becomeFollower(msg.term);
    }

    switch (msg.kind) {
      case "RequestVote":
        return [this.onRequestVote(msg.src, msg.term, msg.lastLogIndex, msg.lastLogTerm)];
      case "RequestVoteReply":
        return this.onRequestVoteReply(msg.src, msg.term, msg.granted);
      case "AppendEntries":
        return [
          this.onAppendEntries(
            msg.src,
            msg.term,
            msg.prevLogIndex,
            msg.prevLogTerm,
            msg.entries,
            msg.leaderCommit,
          ),
        ];
      case "AppendEntriesReply":
        return this.onAppendEntriesReply(msg.src, msg.term, msg.success, msg.matchIndex);
    }
  }

  /**
   * Client entry point. Only a leader accepts a proposal; it appends to its own
   * log and lets the next heartbeat replicate it. Returns whether the proposal
   * was accepted.
   */
  propose(command: C): boolean {
    if (this.role !== Role.Leader) {
      return false;
    }
    this.log.append({ term: this.currentTerm, command });
    this.matchIndex.set(this.id, this.log.lastIndex());
    return true;
  }

  // ---- role transitions ----

  private becomeFollower(term: number): void {
    this.currentTerm = term;
    this.votedFor = null;
    this.role = Role.Follower;
    this.electionElapsed = 0;
    this.votes = new Set();
  }

  private becomeCandidate(): void {
    this.currentTerm += 1;
    this.role = Role.Candidate;
    this.votedFor = this.id;
    this.votes = new Set([this.id]);
    this.electionElapsed = 0;
  }

  private becomeLeader(): void {
    this.role = Role.Leader;
    this.heartbeatElapsed = 0;
    const last = this.log.lastIndex();
    this.nextIndex = new Map();
    this.matchIndex = new Map();
    for (const p of this.peers) {
      this.nextIndex.set(p, last + 1);
      this.matchIndex.set(p, 0);
    }
    this.matchIndex.set(this.id, last);
  }

  // ---- elections ----

  private startElection(): Message<C>[] {
    this.becomeCandidate();
    if (this.peers.length === 0) {
      // single-node cluster: instant win
      this.becomeLeader();
      return [];
    }
    return this.peers.map((p) => ({
      kind: "RequestVote" as const,
      src: this.id,
      dst: p,
      term: this.currentTerm,
      lastLogIndex: this.log.lastIndex(),
      lastLogTerm: this.log.lastTerm(),
    }));
  }

  private onRequestVote(
    src: number,
    term: number,
    lastLogIndex: number,
    lastLogTerm: number,
  ): Message<C> {
    let granted = false;
    if (term >= this.currentTerm) {
      const free = this.votedFor === null || this.votedFor === src;
      const current = this.log.isUpToDate(lastLogIndex, lastLogTerm);
      if (free && current) {
        granted = true;
        this.votedFor = src;
        this.electionElapsed = 0;
      }
    }
    return {
      kind: "RequestVoteReply",
      src: this.id,
      dst: src,
      term: this.currentTerm,
      granted,
    };
  }

  private onRequestVoteReply(src: number, term: number, granted: boolean): Message<C>[] {
    if (this.role !== Role.Candidate || term !== this.currentTerm) {
      return [];
    }
    if (granted) {
      this.votes.add(src);
      if (this.votes.size >= this.majority()) {
        this.becomeLeader();
        return this.broadcastAppend();
      }
    }
    return [];
  }

  // ---- log replication ----

  private broadcastAppend(): Message<C>[] {
    return this.peers.map((p) => this.appendFor(p));
  }

  private appendFor(peer: number): Message<C> {
    const nextIdx = this.nextIndex.get(peer) ?? this.log.lastIndex() + 1;
    const prevIndex = nextIdx - 1;
    return {
      kind: "AppendEntries",
      src: this.id,
      dst: peer,
      term: this.currentTerm,
      prevLogIndex: prevIndex,
      prevLogTerm: this.log.termAt(prevIndex),
      entries: this.log.sliceFrom(nextIdx),
      leaderCommit: this.commitIndex,
    };
  }

  private onAppendEntries(
    src: number,
    term: number,
    prevLogIndex: number,
    prevLogTerm: number,
    entries: LogEntry<C>[],
    leaderCommit: number,
  ): Message<C> {
    if (term < this.currentTerm) {
      // stale leader
      return { kind: "AppendEntriesReply", src: this.id, dst: src, term: this.currentTerm, success: false, matchIndex: 0 };
    }

    // Valid current-term leader: defer to it and reset our election timer.
    this.role = Role.Follower;
    this.electionElapsed = 0;

    if (!this.log.matches(prevLogIndex, prevLogTerm)) {
      // log gap/conflict at prev; leader will back off
      return { kind: "AppendEntriesReply", src: this.id, dst: src, term: this.currentTerm, success: false, matchIndex: 0 };
    }

    this.log.truncateAndAppend(prevLogIndex, entries);
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.log.lastIndex());
      this.applyCommitted();
    }

    return {
      kind: "AppendEntriesReply",
      src: this.id,
      dst: src,
      term: this.currentTerm,
      success: true,
      matchIndex: prevLogIndex + entries.length,
    };
  }

  private onAppendEntriesReply(
    src: number,
    term: number,
    success: boolean,
    matchIndex: number,
  ): Message<C>[] {
    if (this.role !== Role.Leader || term !== this.currentTerm) {
      return [];
    }
    if (success) {
      this.matchIndex.set(src, matchIndex);
      this.nextIndex.set(src, matchIndex + 1);
      this.advanceCommit();
      return [];
    }
    // Rejected: step next_index back and retry.
    const cur = this.nextIndex.get(src) ?? 1;
    this.nextIndex.set(src, Math.max(1, cur - 1));
    return [this.appendFor(src)];
  }

  /**
   * Commits index N when a majority stores it AND it is from the current term
   * (the paper's safety rule against committing stale entries).
   */
  private advanceCommit(): void {
    for (let idx = this.log.lastIndex(); idx > this.commitIndex; idx--) {
      if (this.log.termAt(idx) !== this.currentTerm) {
        continue;
      }
      let replicas = 1;
      for (const p of this.peers) {
        if ((this.matchIndex.get(p) ?? 0) >= idx) {
          replicas += 1;
        }
      }
      if (replicas >= this.majority()) {
        this.commitIndex = idx;
        this.applyCommitted();
        break;
      }
    }
  }

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      this.applied.push(this.log.get(this.lastApplied));
    }
  }

  private majority(): number {
    return Math.floor((this.peers.length + 1) / 2) + 1;
  }
}
