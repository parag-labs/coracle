package coracle

// rng is a small deterministic pseudo-random generator (a SplitMix64 core) used
// by the cluster's fault injection and by the chaos tests. The reference
// implementations each use their own language's PRNG - the chaos suite asserts
// Raft's safety invariants, not an exact message schedule - so this generator
// only needs to be deterministic and reasonably uniform, not to match Python's
// Mersenne Twister byte for byte.
type rng struct {
	state uint64
}

// newRNG seeds a generator.
func newRNG(seed uint64) *rng {
	return &rng{state: seed}
}

// next returns the next 64-bit value in the sequence.
func (r *rng) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// float64 returns a value in [0, 1).
func (r *rng) float64() float64 {
	return float64(r.next()>>11) * (1.0 / (1 << 53))
}

// intn returns a value in [0, n). It panics if n <= 0.
func (r *rng) intn(n int) int {
	return int(r.next() % uint64(n))
}

// randint returns a value in the inclusive range [a, b].
func (r *rng) randint(a, b int) int {
	return a + r.intn(b-a+1)
}

// shuffle performs a Fisher-Yates shuffle of n items via the swap callback.
func (r *rng) shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := r.intn(i + 1)
		swap(i, j)
	}
}
