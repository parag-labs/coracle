/**
 * A deterministic in-memory cluster that drives time and delivers messages.
 *
 * There are no real timers or sockets. {@link Cluster.step} advances every node
 * one step and routes whatever messages they emit. Message delivery order is
 * stable, and the network can be partitioned or healed, so an entire election or
 * a replication round replays identically every run - which is what makes the
 * tests reliable. With the fault knobs off, `new Cluster(n)` is fully
 * deterministic; a seed enables dropping, duplicating, and reordering messages.
 */

import type { Message } from "./messages.ts";
import { RaftNode, Role } from "./node.ts";
import { Rng } from "./rng.ts";

/**
 * Configuration for a {@link Cluster}. Every field is optional. A `seed` of
 * `undefined` keeps the cluster fully deterministic; a number enables the fault
 * knobs and randomizes election timeouts in `[T, 2T-1]` like the paper
 * recommends.
 */
export interface ClusterOptions {
  /** Base election timeout `T`. Defaults to 10. */
  electionTimeout?: number;
  /** Heartbeat interval. Defaults to 3. */
  heartbeatInterval?: number;
  /** Seed for fault injection, or `undefined` for a deterministic cluster. */
  seed?: number;
  /** Probability of dropping a message in flight. Defaults to 0. */
  dropProb?: number;
  /** Probability of delivering a message twice. Defaults to 0. */
  duplicateProb?: number;
  /** Whether to shuffle the pending queue each step. Defaults to false. */
  reorder?: boolean;
}

/** A deterministic in-memory cluster of Raft nodes. */
export class Cluster<C> {
  /** The nodes, indexed by id. */
  readonly nodes: RaftNode<C>[] = [];
  /** The number of steps executed. */
  time = 0;
  /** The number of messages the network threw away. */
  dropped = 0;

  private readonly rng: Rng | null;
  private readonly dropProb: number;
  private readonly duplicateProb: number;
  private readonly reorder: boolean;
  private inbox: Message<C>[] = [];
  private readonly partitioned = new Set<string>();

  /** Builds a cluster of the given size with the supplied options. */
  constructor(size: number, options: ClusterOptions = {}) {
    const electionTimeout = options.electionTimeout ?? 10;
    const heartbeatInterval = options.heartbeatInterval ?? 3;
    this.dropProb = options.dropProb ?? 0;
    this.duplicateProb = options.duplicateProb ?? 0;
    this.reorder = options.reorder ?? false;
    this.rng = options.seed === undefined ? null : new Rng(options.seed);

    const ids = Array.from({ length: size }, (_, i) => i);
    // Stagger election timeouts by node id so the cluster doesn't split the vote
    // forever; a deterministic per-node offset keeps the simulation
    // reproducible. With a seed we randomize timeouts in [T, 2T-1] instead.
    for (let i = 0; i < size; i++) {
      const timeout = this.rng ? electionTimeout + this.rng.below(electionTimeout) : electionTimeout + i * 2;
      this.nodes.push(new RaftNode<C>(i, ids, timeout, heartbeatInterval));
    }
  }

  // ---- network control ----

  private static key(a: number, b: number): string {
    return a < b ? `${a}-${b}` : `${b}-${a}`;
  }

  /** Cuts the link between `a` and `b` in both directions. */
  partition(a: number, b: number): void {
    this.partitioned.add(Cluster.key(a, b));
  }

  /** Restores the link between `a` and `b`. */
  heal(a: number, b: number): void {
    this.partitioned.delete(Cluster.key(a, b));
  }

  /** Restores every cut link. */
  healAll(): void {
    this.partitioned.clear();
  }

  /** Cuts every link to the given node. */
  isolate(node: number): void {
    for (let other = 0; other < this.nodes.length; other++) {
      if (other !== node) {
        this.partition(node, other);
      }
    }
  }

  private reachable(src: number, dst: number): boolean {
    return !this.partitioned.has(Cluster.key(src, dst));
  }

  // ---- driving time ----

  /**
   * Advances one tick: delivers queued messages (optionally dropping,
   * duplicating, or reordering them under a seed) and then ticks every node.
   */
  step(): void {
    this.time += 1;

    const pending = this.inbox;
    this.inbox = [];

    if (this.reorder && this.rng && pending.length > 1) {
      this.rng.shuffle(pending);
    }

    for (const msg of pending) {
      const dst = msg.dst;
      if (!(this.reachable(msg.src, dst) && dst < this.nodes.length)) {
        continue;
      }
      if (this.rng && this.dropProb > 0 && this.rng.nextFloat() < this.dropProb) {
        this.dropped += 1;
        continue;
      }
      let deliveries = 1;
      if (this.rng && this.duplicateProb > 0 && this.rng.nextFloat() < this.duplicateProb) {
        deliveries = 2; // deliver the same message twice
      }
      for (let d = 0; d < deliveries; d++) {
        this.enqueue(this.nodes[dst].handle(msg));
      }
    }

    for (const node of this.nodes) {
      this.enqueue(node.tick());
    }
  }

  /** Advances the cluster by the given number of steps. */
  run(steps: number): void {
    for (let i = 0; i < steps; i++) {
      this.step();
    }
  }

  /**
   * Steps until a unique leader emerges or the step budget is exhausted,
   * returning the leader's id or null. The budget defaults to 200 steps.
   */
  runUntilLeader(maxSteps = 200): number | null {
    for (let i = 0; i < maxSteps; i++) {
      this.step();
      const leader = this.leaderId();
      if (leader !== null) {
        return leader;
      }
    }
    return null;
  }

  private enqueue(messages: Message<C>[]): void {
    for (const m of messages) {
      if (this.reachable(m.src, m.dst)) {
        this.inbox.push(m);
      }
    }
  }

  // ---- inspection ----

  /** The node with the given id. */
  node(id: number): RaftNode<C> {
    return this.nodes[id];
  }

  /**
   * The unique current-term leader's id, or null if there is a split or no
   * leader.
   */
  leaderId(): number | null {
    const leaders = this.nodes.filter((n) => n.role === Role.Leader);
    if (leaders.length === 0) {
      return null;
    }
    const topTerm = Math.max(...leaders.map((n) => n.currentTerm));
    const top = leaders.filter((n) => n.currentTerm === topTerm);
    return top.length === 1 ? top[0].id : null;
  }

  /** The ids of every node that currently believes it is the leader. */
  leaders(): number[] {
    return this.nodes.filter((n) => n.role === Role.Leader).map((n) => n.id);
  }

  /** The commands applied on the given node, in order. */
  committedCommands(nodeId: number): C[] {
    return this.nodes[nodeId].applied.map((e) => e.command);
  }
}
