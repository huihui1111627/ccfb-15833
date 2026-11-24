package sampling

import "testing"

func TestLowBitDecisionIsStable(t *testing.T) {
	const p = 0.37
	limit := Threshold(p)
	for _, id := range []uint64{1, 42, 1<<40 + 12345, ^uint64(0), 987654321} {
		h1, b1, l1 := Hit(id, p)
		h2, b2, l2 := Hit(id, p)
		if h1 != h2 || b1 != b2 || l1 != l2 {
			t.Fatalf("decision not stable for %d", id)
		}
		if b1 != Bucket(id) || l1 != limit {
			t.Fatalf("unexpected bucket/threshold for %d", id)
		}
	}
}

func TestProbabilityBoundsAndDistribution(t *testing.T) {
	if hit, _, _ := Hit(123, 0); hit {
		t.Fatal("p=0 must never hit")
	}
	if hit, _, limit := Hit(0, 1); !hit || limit != BucketCount {
		t.Fatal("p=1 must always hit")
	}

	const n = 200000
	p := 0.25
	hits := 0
	for i := uint64(0); i < n; i++ {
		// Spread ids across the low-bit space while varying high bits.
		id := i*1_000_003 ^ (i << 37)
		if hit, _, _ := Hit(id, p); hit {
			hits++
		}
	}
	got := float64(hits) / n
	if got < 0.24 || got > 0.26 {
		t.Fatalf("distribution %.4f outside expected tolerance", got)
	}
}
