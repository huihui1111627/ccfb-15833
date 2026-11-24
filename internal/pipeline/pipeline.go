package pipeline

import (
	"container/list"
	"sort"
	"sync"
	"time"

	"skiresort/tail-sampling/internal/controller"
	"skiresort/tail-sampling/internal/model"
	"skiresort/tail-sampling/internal/policy"
	"skiresort/tail-sampling/internal/sink"
	"skiresort/tail-sampling/internal/stats"
)

const (
	traceBaseBytes  = 512
	spanEstimate    = 448
	batchIndexBytes = 96
	rawFreedOnBatch = 256
)

type Config struct {
	DefaultSilence        time.Duration
	ServiceSilence        map[string]time.Duration
	GracePeriod           time.Duration
	MaxExtensions         int
	BatchSize             int
	DecisionCacheCapacity int
	MaxBufferBytes        int64
	LateAgeThreshold      time.Duration
	RateWindowSeconds     int
	AdjustInterval        time.Duration
	BudgetHorizon         time.Duration
	DurationThreshold     time.Duration
	Targets               map[string]float64
	InitialProbability    float64
}

func DefaultConfig() Config {
	return Config{
		DefaultSilence:        2 * time.Second,
		ServiceSilence:        map[string]time.Duration{},
		GracePeriod:           750 * time.Millisecond,
		MaxExtensions:         1,
		BatchSize:             64,
		DecisionCacheCapacity: 8192,
		MaxBufferBytes:        64 << 20,
		LateAgeThreshold:      20 * time.Second,
		RateWindowSeconds:     180,
		AdjustInterval:        15 * time.Second,
		BudgetHorizon:         10 * time.Minute,
		DurationThreshold:     time.Second,
		InitialProbability:    0.2,
	}
}

type IngestResult struct {
	Late           *model.LateRecord
	ForcedDecision *model.Decision
}

type Pipeline struct {
	cfg        Config
	engine     *policy.Engine
	Controller *controller.Controller
	Stats      *stats.Stats
	sink       sink.Sink

	mu          sync.Mutex
	active      map[uint64]*traceState
	order       *list.List
	decisions   *decisionCache
	bufferBytes int64
}

type traceState struct {
	id        uint64
	view      policy.TraceView
	spans     []model.Span
	orderElem *list.Element

	firstArrival  time.Time
	lastArrival   time.Time
	deadline      time.Time
	graceDeadline time.Time
	silence       time.Duration
	extensions    int

	bytes        int64
	batchCount   int
	batchedSpans int
	checksumSum  uint64
	checksumXOR  uint64
	minStart     time.Time
	maxEnd       time.Time
}

func New(cfg Config, output sink.Sink) *Pipeline {
	def := DefaultConfig()
	if cfg.DefaultSilence <= 0 {
		cfg.DefaultSilence = def.DefaultSilence
	}
	if cfg.ServiceSilence == nil {
		cfg.ServiceSilence = map[string]time.Duration{}
	}
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = def.GracePeriod
	}
	if cfg.MaxExtensions < 0 {
		cfg.MaxExtensions = def.MaxExtensions
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = def.BatchSize
	}
	if cfg.DecisionCacheCapacity <= 0 {
		cfg.DecisionCacheCapacity = def.DecisionCacheCapacity
	}
	if cfg.MaxBufferBytes <= 0 {
		cfg.MaxBufferBytes = def.MaxBufferBytes
	}
	if cfg.LateAgeThreshold <= 0 {
		cfg.LateAgeThreshold = def.LateAgeThreshold
	}
	if cfg.RateWindowSeconds <= 0 {
		cfg.RateWindowSeconds = def.RateWindowSeconds
	}
	if cfg.AdjustInterval <= 0 {
		cfg.AdjustInterval = def.AdjustInterval
	}
	if cfg.BudgetHorizon <= 0 {
		cfg.BudgetHorizon = def.BudgetHorizon
	}
	if cfg.DurationThreshold <= 0 {
		cfg.DurationThreshold = def.DurationThreshold
	}
	if cfg.InitialProbability < 0 {
		cfg.InitialProbability = def.InitialProbability
	}

	targets := cfg.Targets
	if targets == nil {
		targets = map[string]float64{}
	}
	ctrl := controller.New(cfg.AdjustInterval, cfg.InitialProbability, targets)
	policyDefs := policy.DefaultPolicies(cfg.DurationThreshold)
	engine := policy.NewEngine(ctrl.Probability, policyDefs...)

	policyStats := []stats.PolicyStat{
		{ID: "error", Name: "出错", Priority: 10},
		{ID: "duration", Name: "总耗时阈值", Priority: 20},
		{ID: "service_operation", Name: "重点服务/接口", Priority: 30},
		{ID: "tag", Name: "标签取值", Priority: 40},
		{ID: "probability", Name: "自适应概率", Priority: 100},
	}
	st := stats.New(cfg.RateWindowSeconds, cfg.MaxBufferBytes, policyStats)
	for svc, target := range targets {
		st.SetTarget(svc, target)
	}

	return &Pipeline{
		cfg:        cfg,
		engine:     engine,
		Controller: ctrl,
		Stats:      st,
		sink:       output,
		active:     make(map[uint64]*traceState),
		order:      list.New(),
		decisions:  newDecisionCache(cfg.DecisionCacheCapacity),
	}
}

func estimateSpan(s model.Span) int64 {
	n := spanEstimate
	n += len(s.Service) + len(s.Operation) + len(s.RootService) + len(s.Kind)
	for k, v := range s.Tags {
		n += 48 + len(k) + len(v)
	}
	return int64(n)
}

func (p *Pipeline) silenceFor(service string) time.Duration {
	if d, ok := p.cfg.ServiceSilence[service]; ok && d > 0 {
		return d
	}
	return p.cfg.DefaultSilence
}

// Ingest accepts reported spans. Arrival is used as the processing time when
// set; this allows deterministic virtual-time simulations.
func (p *Pipeline) Ingest(spans ...model.Span) []IngestResult {
	results := make([]IngestResult, 0, len(spans))
	p.mu.Lock()
	defer p.mu.Unlock()

	var processNow time.Time
	for _, original := range spans {
		s := original
		now := s.Arrival
		if now.IsZero() {
			now = time.Now()
			s.Arrival = now
		}
		processNow = now

		if t, ok := p.active[s.TraceID]; ok {
			p.addSpanLocked(t, s, now)
			results = append(results, IngestResult{})
			continue
		}

		if d, ok := p.decisions.get(s.TraceID); ok {
			rec := p.handleCachedLateLocked(d, s, now)
			results = append(results, IngestResult{Late: &rec})
			continue
		}

		if p.isOrphanLateLocked(s, now) {
			rec := p.judgeOrphanLateLocked(s, now)
			results = append(results, IngestResult{Late: &rec})
			continue
		}

		t := &traceState{
			id:      s.TraceID,
			view:    policy.NewTraceView(s.TraceID),
			silence: p.cfg.DefaultSilence,
			bytes:   traceBaseBytes,
		}
		t.orderElem = p.order.PushBack(t)
		p.active[s.TraceID] = t
		p.bufferBytes += traceBaseBytes
		p.addSpanLocked(t, s, now)
		root := s.RootService
		if root == "" {
			root = t.view.RootService
		}
		p.Stats.RecordArrival(root, now)
		results = append(results, IngestResult{})
	}

	for p.bufferBytes > p.cfg.MaxBufferBytes && p.order.Len() > 0 {
		if processNow.IsZero() {
			processNow = time.Now()
		}
		el := p.order.Front()
		t := el.Value.(*traceState)
		d := p.finalizeAndDispatchLocked(t, processNow, true, "缓冲内存超过上限，提前决策最老链路")
		results = append(results, IngestResult{ForcedDecision: &d})
	}

	p.Stats.RecordBuffer(len(p.active), p.bufferBytes)
	return results
}

func (p *Pipeline) isOrphanLateLocked(s model.Span, now time.Time) bool {
	end := s.End
	if end.IsZero() {
		end = s.Start
	}
	if end.IsZero() {
		return false
	}
	return now.Sub(end) > p.cfg.LateAgeThreshold
}

func (p *Pipeline) addSpanLocked(t *traceState, s model.Span, now time.Time) {
	est := estimateSpan(s)
	t.bytes += est
	p.bufferBytes += est

	expired := !t.deadline.IsZero() && !now.Before(t.deadline)
	if expired {
		if t.extensions < p.cfg.MaxExtensions {
			t.extensions++
			p.Stats.RecordExtension()
			t.graceDeadline = time.Time{}
			t.silence = maxDuration(t.silence, p.silenceFor(s.Service))
			t.deadline = now.Add(t.silence)
		} else {
			// Extension cap reached: include this fragment but do not grant
			// another quiet period. The next sweep finalizes immediately.
			t.deadline = now
			t.graceDeadline = now
		}
	} else {
		t.silence = maxDuration(t.silence, p.silenceFor(s.Service))
		t.deadline = now.Add(t.silence)
		t.graceDeadline = time.Time{}
	}

	t.spans = append(t.spans, s)
	t.view.AddSpan(s)
	t.checksumSum += s.SpanID
	t.checksumXOR ^= s.SpanID
	if t.firstArrival.IsZero() {
		t.firstArrival = now
		t.minStart = s.Start
		t.maxEnd = s.End
	}
	t.lastArrival = now
	if !s.Start.IsZero() && (t.minStart.IsZero() || s.Start.Before(t.minStart)) {
		t.minStart = s.Start
	}
	if !s.End.IsZero() && s.End.After(t.maxEnd) {
		t.maxEnd = s.End
	}
	if !t.minStart.IsZero() && !t.maxEnd.IsZero() && t.maxEnd.After(t.minStart) {
		t.view.TotalDuration = t.maxEnd.Sub(t.minStart)
	}

	if len(t.spans) >= p.cfg.BatchSize {
		p.flushBatchLocked(t, now, true)
	}
}

func (p *Pipeline) flushBatchLocked(t *traceState, now time.Time, early bool) model.Batch {
	batchNo := t.batchCount + 1
	spans := append([]model.Span(nil), t.spans...)
	b := model.Batch{
		TraceID:     t.id,
		BatchNo:     batchNo,
		Final:       !early,
		Provisional: early,
		SpanCount:   len(spans),
		Spans:       spans,
		SentAt:      now,
	}
	t.batchCount = batchNo
	t.batchedSpans += len(spans)

	// Raw span payloads leave the buffer. The compact policy view remains.
	freed := int64(len(spans)) * rawFreedOnBatch
	if freed > t.bytes {
		freed = t.bytes
	}
	t.bytes -= freed
	p.bufferBytes -= freed
	t.bytes += batchIndexBytes
	p.bufferBytes += batchIndexBytes
	t.spans = nil

	p.Stats.RecordBatch(len(spans), early)
	if p.sink != nil {
		p.sink.BatchSent(b)
	}
	return b
}

func (p *Pipeline) handleCachedLateLocked(d model.Decision, s model.Span, now time.Time) model.LateRecord {
	mode := "cached_drop"
	if d.Keep {
		mode = "cached_keep"
	}
	rec := model.LateRecord{
		At: now, TraceID: s.TraceID, SpanID: s.SpanID, Service: s.Service,
		RootService: d.RootService, Mode: mode, Reason: "使用决策缓存中的结论",
		PolicyID: d.PolicyID, DecisionKeep: d.Keep,
		Probability: d.Probability, Bucket: d.Bucket, BucketLimit: d.BucketLimit,
	}
	p.Stats.RecordLate(rec, false)
	if p.sink != nil {
		p.sink.LateSpanHandled(rec, s)
	}
	return rec
}

func (p *Pipeline) judgeOrphanLateLocked(s model.Span, now time.Time) model.LateRecord {
	view := policy.NewTraceView(s.TraceID)
	view.AddSpan(s)
	view.TotalDuration = s.Duration()
	res := p.engine.Evaluate(view)
	mode := "independent_drop"
	if res.Keep {
		mode = "independent_keep"
	}
	primary := res.Primary
	rec := model.LateRecord{
		At: now, TraceID: s.TraceID, SpanID: s.SpanID, Service: s.Service,
		RootService: view.RootService, Mode: mode,
		Reason:   "决策缓存已淘汰，按单个迟到片段重新判定: " + primary.Reason,
		PolicyID: primary.PolicyID, DecisionKeep: res.Keep,
		Probability: res.Probability, Bucket: res.Bucket, BucketLimit: res.BucketLimit,
	}
	if len(res.Matches) == 0 {
		rec.Reason = "决策缓存已淘汰，按单个迟到片段重新判定：无策略命中"
	}
	p.Stats.RecordLate(rec, true)
	if p.sink != nil {
		p.sink.LateSpanHandled(rec, s)
	}
	return rec
}

// Sweep finalizes traces whose silence and arrival grace have expired.
func (p *Pipeline) Sweep(now time.Time) []model.Decision {
	rates := p.Stats.WindowRates(now)
	corrections := p.Stats.BudgetCorrections(now, p.cfg.BudgetHorizon)
	if p.Controller.MaybeAdjust(now, rates, corrections) {
		p.Stats.ControllerAdjusted(now)
		for svc, prob := range p.Controller.Probabilities() {
			p.Stats.SetProbability(svc, prob)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	due := make([]*traceState, 0)
	for _, t := range p.active {
		if now.Before(t.deadline) {
			continue
		}
		if t.graceDeadline.IsZero() {
			// Give a short race/late window before the irreversible decision.
			t.graceDeadline = now.Add(p.cfg.GracePeriod)
			continue
		}
		if now.Before(t.graceDeadline) {
			continue
		}
		due = append(due, t)
	}
	sort.Slice(due, func(i, j int) bool { return due[i].firstArrival.Before(due[j].firstArrival) })

	decisions := make([]model.Decision, 0, len(due))
	for _, t := range due {
		// Another path cannot remove it while this lock is held, but keep the
		// map membership check for clarity.
		if _, ok := p.active[t.id]; !ok {
			continue
		}
		decisions = append(decisions, p.finalizeAndDispatchLocked(t, now, false, ""))
	}
	p.Stats.RecordBuffer(len(p.active), p.bufferBytes)
	return decisions
}

// FlushAll finalizes every buffered trace. Used by simulation drain/shutdown.
func (p *Pipeline) FlushAll(now time.Time) []model.Decision {
	p.mu.Lock()
	defer p.mu.Unlock()
	traces := make([]*traceState, 0, len(p.active))
	for _, t := range p.active {
		traces = append(traces, t)
	}
	sort.Slice(traces, func(i, j int) bool { return traces[i].firstArrival.Before(traces[j].firstArrival) })
	out := make([]model.Decision, 0, len(traces))
	for _, t := range traces {
		if _, ok := p.active[t.id]; ok {
			out = append(out, p.finalizeAndDispatchLocked(t, now, false, "flush_all"))
		}
	}
	p.Stats.RecordBuffer(len(p.active), p.bufferBytes)
	return out
}

func (p *Pipeline) finalizeAndDispatchLocked(t *traceState, now time.Time, forced bool, forcedReason string) model.Decision {
	if t.orderElem != nil {
		p.order.Remove(t.orderElem)
	}
	delete(p.active, t.id)

	// Send the final partial batch before publishing the decision. Flushing
	// updates t.bytes/bufferBytes to the compact batch-index footprint, so
	// remove the remaining trace footprint only after that.
	if len(t.spans) > 0 {
		p.flushBatchLocked(t, now, false)
	}
	batchCount := t.batchCount
	p.bufferBytes -= t.bytes
	if p.bufferBytes < 0 {
		p.bufferBytes = 0
	}

	res := p.engine.Evaluate(t.view)
	matchedIDs := make([]string, 0, len(res.Matches))
	for _, m := range res.Matches {
		matchedIDs = append(matchedIDs, m.PolicyID)
	}
	policyID := ""
	reason := "所有策略均未要求保留"
	if len(res.Matches) > 0 {
		policyID = res.Primary.PolicyID
		reason = res.Primary.Reason
	}
	if forced && forcedReason != "" {
		reason = forcedReason + "；当前判定: " + reason
	}

	d := model.Decision{
		TraceID:         t.id,
		Keep:            res.Keep,
		MandatoryKeep:   res.MandatoryKeep,
		Reason:          reason,
		PolicyID:        policyID,
		MatchedPolicies: matchedIDs,
		RootService:     t.view.RootService,
		Probability:     res.Probability,
		Bucket:          res.Bucket,
		BucketLimit:     res.BucketLimit,
		SpanCount:       t.view.SpanCount,
		BatchCount:      batchCount,
		ChecksumSum:     t.checksumSum,
		ChecksumXOR:     t.checksumXOR,
		TotalDuration:   t.view.TotalDuration,
		Error:           t.view.Error,
		DecidedAt:       now,
		ForcedOldest:    forced,
		ExtensionsUsed:  t.extensions,
	}

	evicted := p.decisions.add(d)
	_ = evicted // Eviction is capacity management; the later miss is recorded as orphan-late.
	p.Stats.RecordDecision(d)
	if p.sink != nil {
		// flushBatchLocked already sent BatchSent before the decision was built.
		p.sink.DecisionMade(d)
	}
	return d
}

func (p *Pipeline) ActiveTraceCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active)
}

func (p *Pipeline) DecisionCacheLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.decisions.len()
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
