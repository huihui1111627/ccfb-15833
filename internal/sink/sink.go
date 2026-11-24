package sink

import (
	"sync"

	"skiresort/tail-sampling/internal/model"
)

// Sink receives provisional batches, final decisions, and late spans.
type Sink interface {
	BatchSent(model.Batch)
	DecisionMade(model.Decision)
	LateSpanHandled(model.LateRecord, model.Span)
}

// NopSink is used by the long-running demo server, where Stats and the web UI
// provide the operational view and compact verification manifests are not needed.
type NopSink struct{}

func (NopSink) BatchSent(model.Batch)                        {}
func (NopSink) DecisionMade(model.Decision)                  {}
func (NopSink) LateSpanHandled(model.LateRecord, model.Span) {}

// Manifest proves that all batches belonging to a decision contain exactly
// the spans the pipeline had aggregated at decision time.
type Manifest struct {
	TraceID         uint64
	Decision        model.Decision
	Batches         int
	SpansInBatches  int
	ChecksumSum     uint64
	ChecksumXOR     uint64
	LateSpans       int
	LateChecksumSum uint64
}

// VerifyingSink stores only compact manifests, not full retained span payloads.
type VerifyingSink struct {
	mu sync.Mutex

	rawBatches    int64
	rawBatchSpans int64
	decisions     int64
	kept          int64
	dropped       int64
	lateSpans     int64
	mismatches    []string
	Manifests     map[uint64]*Manifest
}

func NewVerifyingSink() *VerifyingSink {
	return &VerifyingSink{Manifests: make(map[uint64]*Manifest)}
}

func checksumSpan(s model.Span) (sum, xor uint64) {
	return s.SpanID, s.SpanID
}

func (v *VerifyingSink) BatchSent(b model.Batch) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rawBatches++
	v.rawBatchSpans += int64(len(b.Spans))
	m := v.Manifests[b.TraceID]
	if m == nil {
		m = &Manifest{TraceID: b.TraceID}
		v.Manifests[b.TraceID] = m
	}
	m.Batches++
	m.SpansInBatches += len(b.Spans)
	for _, sp := range b.Spans {
		sum, x := checksumSpan(sp)
		m.ChecksumSum += sum
		m.ChecksumXOR ^= x
	}
}

func (v *VerifyingSink) DecisionMade(d model.Decision) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.decisions++
	if d.Keep {
		v.kept++
	} else {
		v.dropped++
	}
	m := v.Manifests[d.TraceID]
	if m == nil {
		m = &Manifest{TraceID: d.TraceID}
		v.Manifests[d.TraceID] = m
	}
	m.Decision = d
	// The pipeline checksum covers every span received before decision,
	// including spans already sent in early batches.
	if m.SpansInBatches != d.SpanCount {
		v.mismatches = append(v.mismatches,
			"batch span count mismatch for trace "+uint64String(d.TraceID))
	}
	if m.ChecksumSum != d.ChecksumSum || m.ChecksumXOR != d.ChecksumXOR {
		v.mismatches = append(v.mismatches,
			"batch checksum mismatch for trace "+uint64String(d.TraceID))
	}
}

func (v *VerifyingSink) LateSpanHandled(r model.LateRecord, s model.Span) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.lateSpans++
	if r.Mode == "cached_keep" || r.Mode == "independent_keep" {
		m := v.Manifests[r.TraceID]
		if m == nil {
			m = &Manifest{TraceID: r.TraceID}
			v.Manifests[r.TraceID] = m
		}
		m.LateSpans++
		m.LateChecksumSum += s.SpanID
	}
}

type Report struct {
	RawBatches    int64
	RawBatchSpans int64
	Decisions     int64
	Kept          int64
	Dropped       int64
	LateSpans     int64
	Mismatches    []string
	ManifestCount int
}

func (v *VerifyingSink) Report() Report {
	v.mu.Lock()
	defer v.mu.Unlock()
	ms := make([]string, len(v.mismatches))
	copy(ms, v.mismatches)
	return Report{
		RawBatches: v.rawBatches, RawBatchSpans: v.rawBatchSpans,
		Decisions: v.decisions, Kept: v.kept, Dropped: v.dropped,
		LateSpans: v.lateSpans, Mismatches: ms, ManifestCount: len(v.Manifests),
	}
}

func (v *VerifyingSink) KeptManifest(id uint64) (Manifest, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	m, ok := v.Manifests[id]
	if !ok || !m.Decision.Keep {
		return Manifest{}, false
	}
	return *m, true
}

func uint64String(id uint64) string {
	// Avoid importing strconv just for the rare mismatch path.
	const digits = "0123456789"
	if id == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for id > 0 {
		i--
		buf[i] = digits[id%10]
		id /= 10
	}
	return string(buf[i:])
}
