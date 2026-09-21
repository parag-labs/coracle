/**
 * The RPC messages exchanged between nodes. These are plain data; the node
 * returns them from its handlers and the cluster delivers them.
 *
 * Raft has four message types; they are modelled as a discriminated union so the
 * cluster can route any message by its common header (`src`, `dst`, `term`)
 * while the node narrows on `kind` to handle each variant.
 */

import type { LogEntry } from "./log.ts";

/** Sent by a candidate to gather votes for an election. */
export interface RequestVote {
  kind: "RequestVote";
  /** Sender id. */
  src: number;
  /** Recipient id. */
  dst: number;
  /** Candidate's term. */
  term: number;
  /** Index of the candidate's last log entry. */
  lastLogIndex: number;
  /** Term of the candidate's last log entry. */
  lastLogTerm: number;
}

/** Answers a `RequestVote`, granting or denying the vote. */
export interface RequestVoteReply {
  kind: "RequestVoteReply";
  /** Sender id. */
  src: number;
  /** Recipient id. */
  dst: number;
  /** Voter's term. */
  term: number;
  /** Whether the vote was given. */
  granted: boolean;
}

/** Replicates log entries from the leader; a heartbeat when it carries none. */
export interface AppendEntries<C> {
  kind: "AppendEntries";
  /** Sender (leader) id. */
  src: number;
  /** Recipient id. */
  dst: number;
  /** Leader's term. */
  term: number;
  /** Index immediately preceding `entries`. */
  prevLogIndex: number;
  /** Term of the entry at `prevLogIndex`. */
  prevLogTerm: number;
  /** New entries to store (empty for a heartbeat). */
  entries: LogEntry<C>[];
  /** Leader's commit index. */
  leaderCommit: number;
}

/** Answers an `AppendEntries` request. */
export interface AppendEntriesReply {
  kind: "AppendEntriesReply";
  /** Sender (follower) id. */
  src: number;
  /** Recipient id. */
  dst: number;
  /** Follower's term. */
  term: number;
  /** Whether the entries were accepted. */
  success: boolean;
  /** On success, the follower's last index, so the leader can advance its match index. */
  matchIndex: number;
}

/** Any Raft RPC. Every variant carries the common `src`/`dst`/`term` header. */
export type Message<C> = RequestVote | RequestVoteReply | AppendEntries<C> | AppendEntriesReply;
