package sampling

// BucketBits uses the low 20 bits of a trace id. Comparing those bits with a
// probability-derived threshold makes the answer deterministic for a trace:
// evaluating the same trace at another stage with the same threshold yields
// the same result.
const (
	BucketBits  = 20
	BucketCount = uint64(1) << BucketBits
	BucketMask  = BucketCount - 1
)

// Bucket returns the deterministic low-bit bucket of the trace id.
func Bucket(traceID uint64) uint64 {
	return traceID & BucketMask
}

// Threshold converts a probability in [0,1] to an exclusive bucket limit.
func Threshold(probability float64) uint64 {
	switch {
	case probability <= 0:
		return 0
	case probability >= 1:
		return BucketCount
	default:
		return uint64(probability * float64(BucketCount))
	}
}

// Hit compares the low bits directly against the threshold.
func Hit(traceID uint64, probability float64) (hit bool, bucket, limit uint64) {
	bucket = Bucket(traceID)
	limit = Threshold(probability)
	return bucket < limit, bucket, limit
}
