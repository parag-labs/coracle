/**
 * A small, readable implementation of the Raft consensus algorithm - leader
 * election and log replication - with a deterministic simulator so an election,
 * a partition, or a replication round replays identically every run.
 *
 * A node never touches the clock or the network: it is a pure state machine with
 * two entry points, {@link RaftNode.tick} and {@link RaftNode.handle}, each
 * returning the messages it wants sent. All time and I/O live in the
 * {@link Cluster} simulator. This mirrors the Python reference implementation
 * module for module.
 */

export { Cluster, type ClusterOptions } from "./cluster.ts";
export { Log, type LogEntry } from "./log.ts";
export type {
  AppendEntries,
  AppendEntriesReply,
  Message,
  RequestVote,
  RequestVoteReply,
} from "./messages.ts";
export { RaftNode, Role } from "./node.ts";
export { Rng } from "./rng.ts";
