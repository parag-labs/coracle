//! A small deterministic pseudo-random generator (a SplitMix64 core) used by the
//! cluster's fault injection and by the chaos tests.
//!
//! The reference implementations each use their own language's PRNG - the chaos
//! suite asserts Raft's safety invariants, not an exact message schedule - so
//! this generator only needs to be deterministic and reasonably uniform, not to
//! match Python's Mersenne Twister byte for byte.

/// A seedable SplitMix64 generator.
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Creates a generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Returns the next 64-bit value in the sequence.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a value in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// Returns a value in `[0, n)`. Panics if `n` is zero.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Returns a value in the inclusive range `[a, b]`.
    pub fn randint(&mut self, a: i64, b: i64) -> i64 {
        let span = (b - a + 1) as u64;
        a + (self.next_u64() % span) as i64
    }

    /// Shuffles `items` in place with a Fisher-Yates pass.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        let mut i = items.len();
        while i > 1 {
            i -= 1;
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}
