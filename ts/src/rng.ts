/**
 * A small deterministic pseudo-random generator (a SplitMix64 core) used by the
 * cluster's fault injection and by the chaos tests.
 *
 * The reference implementations each use their own language's PRNG - the chaos
 * suite asserts Raft's safety invariants, not an exact message schedule - so
 * this generator only needs to be deterministic and reasonably uniform, not to
 * match Python's Mersenne Twister byte for byte. It uses BigInt so the 64-bit
 * wrapping arithmetic is exact.
 */

const MASK = (1n << 64n) - 1n;
const GOLDEN = 0x9e3779b97f4a7c15n;
const MUL1 = 0xbf58476d1ce4e5b9n;
const MUL2 = 0x94d049bb133111ebn;
const TWO53 = Number(1n << 53n);

/** A seedable SplitMix64 generator. */
export class Rng {
  private state: bigint;

  /** Creates a generator seeded with `seed`. */
  constructor(seed: number | bigint) {
    this.state = BigInt(seed) & MASK;
  }

  /** Returns the next 64-bit value in the sequence. */
  nextU64(): bigint {
    this.state = (this.state + GOLDEN) & MASK;
    let z = this.state;
    z = ((z ^ (z >> 30n)) * MUL1) & MASK;
    z = ((z ^ (z >> 27n)) * MUL2) & MASK;
    return (z ^ (z >> 31n)) & MASK;
  }

  /** Returns a value in `[0, 1)`. */
  nextFloat(): number {
    return Number(this.nextU64() >> 11n) / TWO53;
  }

  /** Returns a value in `[0, n)`. Throws if `n` is not positive. */
  below(n: number): number {
    if (n <= 0) {
      throw new Error("below requires n > 0");
    }
    return Number(this.nextU64() % BigInt(n));
  }

  /** Returns a value in the inclusive range `[a, b]`. */
  randint(a: number, b: number): number {
    return a + this.below(b - a + 1);
  }

  /** Shuffles `items` in place with a Fisher-Yates pass. */
  shuffle<T>(items: T[]): void {
    for (let i = items.length - 1; i > 0; i--) {
      const j = this.below(i + 1);
      [items[i], items[j]] = [items[j], items[i]];
    }
  }
}
