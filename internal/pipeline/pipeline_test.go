package pipeline

import (
	"testing"
	"time"

	"skiresort/tail-sampling/internal/model"
	"skiresort/tail-sampling/internal/sink"
)

func testPipeline(t *testing.T, mutate func(*Config)) (*Pipeline, *sink.VerifyingSink, Config) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DefaultSilence = 100 * time.Millisecond
	cfg.GracePeriod = time.Millisecond
	cfg.AdjustInterval = time.Hour
	cfg.RateWindowSeconds = 60
	cfg.InitialProbability = 0
	cfg.Targets = map[string]float64{"svc": 0}
	cfg.DecisionCacheCapacity = 8
	if mutate != nil {
		mutate(&cfg)
	}
	out := sink.NewVerifyingSink()
	p := New(cfg, out)
	return p, out, cfg
}

func span(traceID, spanID uint64, svc string, at time.Time) model.Span {
	return model.Span{
		TraceID: traceID, SpanID: spanID, RootService: "svc", Service: svc,
		Operation: "Op", Start: at, End: at.Add(20 * time.Millisecond), Arrival: at,
	}
}

func decideAfter(t *testing.T, p *Pipeline, id uint64, last time.Time) model.Decision {
	t.Helper()
	// One sweep arms the 1ms grace; the next finalizes.
	p.Sweep(last.Add(101 * time.Millisecond))
	ds := p.Sweep(last.Add(103 * time.Millisecond))
	for _, d := range ds {
		if d.TraceID == id {
			return d
		}
	}
	t.Fatalf("decision for trace %d not returned", id)
	return model.Decision{}
}

func TestAggregatesAndKeepsErrorTrace(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	p, out, _ := testPipeline(t, nil)

	s1 := span(1, 10, "svc", base)
	s2 := span(1, 11, "svc", base.Add(40*time.Millisecond))
	s2.Error = true
	p.Ingest(s1, s2)

	d := decideAfter(t, p, 1, s2.Arrival)
	if !d.Keep || !d.MandatoryKeep || d.PolicyID != "error" {
		t.Fatalf("expected mandatory error keep, got %+v", d)
	}
	if d.SpanCount != 2 {
		t.Fatalf("expected 2 spans, got %d", d.SpanCount)
	}
	if rep := out.Report(); len(rep.Mismatches) != 0 {
		t.Fatalf("sink mismatches: %v", rep.Mismatches)
	}
}

func TestDurationPolicyUsesFullTraceWindow(t *testing.T) {
	base := time.Unix(1_700_000_100, 0)
	p, _, _ := testPipeline(t, nil)

	s1 := span(2, 20, "svc", base)
	s1.Start = base
	s1.End = base.Add(700 * time.Millisecond)
	s2 := span(2, 21, "svc", base.Add(720*time.Millisecond))
	s2.Start = base.Add(700 * time.Millisecond)
	s2.End = base.Add(1_500 * time.Millisecond)
	p.Ingest(s1, s2)

	d := decideAfter(t, p, 2, s2.Arrival)
	if !d.Keep || d.PolicyID != "duration" {
		t.Fatalf("expected duration keep, got %+v", d)
	}
	if d.TotalDuration < time.Second {
		t.Fatalf("duration too short: %s", d.TotalDuration)
	}
}

func TestCachedLateSpanUsesCachedDecision(t *testing.T) {
	base := time.Unix(1_700_000_200, 0)
	p, out, _ := testPipeline(t, nil)

	s := span(3, 30, "svc", base)
	s.Error = true
	p.Ingest(s)
	d := decideAfter(t, p, 3, base)
	if !d.Keep {
		t.Fatal("setup: expected keep")
	}

	late := span(3, 31, "svc", base.Add(5*time.Second))
	late.End = base.Add(30 * time.Millisecond) // old enough to be orphan, but cache wins
	results := p.Ingest(late)
	if results[0].Late == nil || results[0].Late.Mode != "cached_keep" {
		t.Fatalf("expected cached_keep late result, got %+v", results)
	}
	m, ok := out.KeptManifest(3)
	if !ok || m.LateSpans != 1 {
		t.Fatalf("expected sink late span, manifest=%+v ok=%v", m, ok)
	}
}

func TestEvictedDecisionLateSpanIsIndependentlyJudged(t *testing.T) {
	base := time.Unix(1_700_000_300, 0)
	p, _, _ := testPipeline(t, func(c *Config) { c.DecisionCacheCapacity = 1 })

	// Trace A: normal/non-matching and dropped.
	a := span(100, 1000, "svc", base)
	p.Ingest(a)
	p.FlushAll(base.Add(time.Second))
	if d, ok := p.decisions.get(100); !ok || d.Keep {
		t.Fatalf("setup A expected cached drop: %+v ok=%v", d, ok)
	}

	// Trace B evicts A from the capacity-one cache.
	b := span(200, 2000, "svc", base)
	p.Ingest(b)
	p.FlushAll(base.Add(2 * time.Second))
	if _, ok := p.decisions.get(100); ok {
		t.Fatal("expected decision A to be evicted")
	}

	lateError := span(100, 1001, "svc", base.Add(30*time.Second))
	lateError.Error = true
	lateError.End = base.Add(20 * time.Millisecond)
	results := p.Ingest(lateError)
	if results[0].Late == nil {
		t.Fatal("expected late record")
	}
	if results[0].Late.Mode != "independent_keep" || results[0].Late.PolicyID != "error" {
		t.Fatalf("expected independent error keep, got %+v", results[0].Late)
	}
	snap := p.Stats.SnapshotAt(base.Add(31 * time.Second))
	if snap.Late.CacheEvicted != 1 || snap.Late.IndependentKeep != 1 {
		t.Fatalf("unexpected late stats: %+v", snap.Late)
	}
}

func TestMemoryPressureFinalizesOldestTrace(t *testing.T) {
	base := time.Unix(1_700_000_400, 0)
	p, _, _ := testPipeline(t, func(c *Config) { c.MaxBufferBytes = 2500 })

	var firstID uint64 = 400
	for i := 0; i < 6; i++ {
		id := uint64(400 + i)
		s := span(id, uint64(4000+i), "s", base.Add(time.Duration(i)*time.Millisecond))
		results := p.Ingest(s)
		if i == 0 {
			firstID = id
		}
		for _, r := range results {
			if r.ForcedDecision != nil {
				if r.ForcedDecision.TraceID != firstID || !r.ForcedDecision.ForcedOldest {
					t.Fatalf("expected oldest trace %d forced, got %+v", firstID, r.ForcedDecision)
				}
				return
			}
		}
	}
	t.Fatal("expected memory-pressure forced decision")
}

func TestSilenceExtensionIsCapped(t *testing.T) {
	base := time.Unix(1_700_000_500, 0)
	p, _, _ := testPipeline(t, func(c *Config) { c.MaxExtensions = 1 })

	p.Ingest(span(500, 5000, "svc", base))
	p.Ingest(span(500, 5001, "svc", base.Add(150*time.Millisecond)))
	if snap := p.Stats.SnapshotAt(base.Add(150 * time.Millisecond)); snap.Extensions != 1 {
		t.Fatalf("expected one extension, got %d", snap.Extensions)
	}

	// Arrival after the extended deadline consumes the cap and is included by
	// the immediate next sweep.
	thirdAt := base.Add(320 * time.Millisecond)
	p.Ingest(span(500, 5002, "svc", thirdAt))
	ds := p.Sweep(thirdAt.Add(2 * time.Millisecond))
	var found bool
	for _, d := range ds {
		if d.TraceID == 500 {
			found = true
			if d.SpanCount != 3 || d.ExtensionsUsed != 1 {
				t.Fatalf("unexpected capped-extension decision: %+v", d)
			}
		}
	}
	if !found {
		t.Fatal("expected trace finalized after cap")
	}
}

func TestServiceSpecificSilenceUsesAsyncConfiguration(t *testing.T) {
	base := time.Unix(1_700_000_700, 0)
	p, _, _ := testPipeline(t, func(c *Config) {
		c.DefaultSilence = 100 * time.Millisecond
		c.ServiceSilence = map[string]time.Duration{"event-bus": 500 * time.Millisecond}
	})

	normal := span(700, 7000, "api-gateway", base)
	p.Ingest(normal)
	p.mu.Lock()
	normalDeadline := p.active[700].deadline.Sub(base)
	p.mu.Unlock()
	if normalDeadline != 100*time.Millisecond {
		t.Fatalf("normal silence = %s", normalDeadline)
	}

	async := span(701, 7100, "event-bus", base)
	p.Ingest(async)
	p.mu.Lock()
	asyncDeadline := p.active[701].deadline.Sub(base)
	p.mu.Unlock()
	if asyncDeadline != 500*time.Millisecond {
		t.Fatalf("async silence = %s", asyncDeadline)
	}
}

func TestLargeTraceBatchesAndChecksumCompletes(t *testing.T) {
	base := time.Unix(1_700_000_600, 0)
	p, out, _ := testPipeline(t, func(c *Config) { c.BatchSize = 2 })

	spans := []model.Span{
		span(600, 6000, "svc", base),
		span(600, 6001, "svc", base.Add(10*time.Millisecond)),
		span(600, 6002, "svc", base.Add(20*time.Millisecond)),
		span(600, 6003, "svc", base.Add(30*time.Millisecond)),
		span(600, 6004, "svc", base.Add(40*time.Millisecond)),
	}
	p.Ingest(spans...)
	d := decideAfter(t, p, 600, spans[4].Arrival)
	if d.SpanCount != 5 || d.BatchCount != 3 {
		t.Fatalf("expected 5 spans / 3 batches, got %d spans / %d batches", d.SpanCount, d.BatchCount)
	}
	rep := out.Report()
	if len(rep.Mismatches) != 0 {
		t.Fatalf("checksum/count mismatches: %v", rep.Mismatches)
	}
	if rep.RawBatches != 3 || rep.RawBatchSpans != 5 {
		t.Fatalf("unexpected raw batches: %+v", rep)
	}
}
