package sim

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"skiresort/tail-sampling/internal/model"
	"skiresort/tail-sampling/internal/pipeline"
	"skiresort/tail-sampling/internal/sink"
	"skiresort/tail-sampling/internal/stats"
)

type Config struct {
	Seed         uint64
	Duration     time.Duration
	Tick         time.Duration
	MaxDeviation float64
}

func DefaultConfig() Config {
	return Config{Seed: 15833, Duration: 3 * time.Hour, Tick: time.Second, MaxDeviation: 0.15}
}

type Check struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

type ServiceReport struct {
	Name         string  `json:"name"`
	TargetRPS    float64 `json:"target_rps"`
	ActualRPS    float64 `json:"actual_rps"`
	DeviationPct float64 `json:"deviation_pct"`
	Probability  float64 `json:"probability"`
	Arrivals     int64   `json:"arrivals"`
	Kept         int64   `json:"kept"`
}

type Report struct {
	Config                Config          `json:"config"`
	ServiceCount          int             `json:"service_count"`
	GeneratedTraces       int64           `json:"generated_traces"`
	GeneratedSpans        int64           `json:"generated_spans"`
	ErrorTraces           int64           `json:"error_traces"`
	SlowTraces            int64           `json:"slow_traces"`
	HugeTraces            int64           `json:"huge_traces"`
	StragglerScheduled    int64           `json:"straggler_scheduled"`
	LateScheduled         int64           `json:"late_scheduled"`
	MustKeep              int             `json:"must_keep"`
	MissedMustKeep        []string        `json:"missed_must_keep"`
	MaxAbsDeviationPct    float64         `json:"max_abs_deviation_pct"`
	WorstService          string          `json:"worst_service"`
	ServiceReports        []ServiceReport `json:"service_reports"`
	Checks                []Check         `json:"checks"`
	Pass                  bool            `json:"pass"`
	SinkReport            sink.Report     `json:"sink_report"`
	Snapshot              stats.Snapshot  `json:"snapshot"`
	SteadySnapshot        stats.Snapshot  `json:"steady_snapshot"`
	MaxWindowDeviationPct float64         `json:"max_window_deviation_pct"`
	WorstWindowService    string          `json:"worst_window_service"`
}

type scheduledSpan struct {
	at   time.Time
	span model.Span
}

type spanHeap []scheduledSpan

func (h spanHeap) Len() int           { return len(h) }
func (h spanHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h spanHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *spanHeap) Push(x any)        { *h = append(*h, x.(scheduledSpan)) }
func (h *spanHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type mustTrace struct {
	reason           string
	spanCount        int
	expectedDuration time.Duration
	mainArrivals     []time.Time
	lastMainArrival  time.Time
	maxArrivalGap    time.Duration
	stragglerAt      time.Time
	trueLateAt       time.Time
}

type simulator struct {
	cfg        Config
	rng        *rand.Rand
	epoch      time.Time
	services   []ServiceInfo
	downstream []string
	asyncNames []string

	pipe *pipeline.Pipeline
	out  *sink.VerifyingSink
	heap spanHeap

	spanSeq  uint64
	seenIDs  map[uint64]struct{}
	mustKeep map[uint64]*mustTrace

	generatedTraces    int64
	generatedSpans     int64
	errorTraces        int64
	slowTraces         int64
	hugeTraces         int64
	stragglerScheduled int64
	lateScheduled      int64
}

// Run executes the deterministic multi-hour virtual traffic test.
func Run(cfg Config) (*Report, error) {
	if cfg.Duration <= 0 {
		cfg.Duration = DefaultConfig().Duration
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.MaxDeviation <= 0 {
		cfg.MaxDeviation = 0.15
	}

	services := Services()
	silence := map[string]time.Duration{}
	for _, svc := range services {
		if svc.Async {
			silence[svc.Name] = 8 * time.Second
		} else {
			silence[svc.Name] = 2 * time.Second
		}
	}
	pcfg := pipeline.DefaultConfig()
	pcfg.DefaultSilence = 2 * time.Second
	pcfg.ServiceSilence = silence
	pcfg.GracePeriod = 750 * time.Millisecond
	pcfg.MaxExtensions = 1
	pcfg.BatchSize = 64
	pcfg.DecisionCacheCapacity = 4096
	pcfg.MaxBufferBytes = 128 << 20
	pcfg.LateAgeThreshold = 18 * time.Second
	pcfg.RateWindowSeconds = 120
	pcfg.AdjustInterval = 10 * time.Second
	pcfg.Targets = TargetMap(services)

	out := sink.NewVerifyingSink()
	pipe := pipeline.New(pcfg, out)

	seed1, seed2 := cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15
	sim := &simulator{
		cfg:      cfg,
		rng:      rand.New(rand.NewPCG(seed1, seed2)),
		epoch:    time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC),
		services: services,
		pipe:     pipe,
		out:      out,
		seenIDs:  make(map[uint64]struct{}),
		mustKeep: make(map[uint64]*mustTrace),
	}
	for _, svc := range services {
		if svc.Name != "payment-service" {
			sim.downstream = append(sim.downstream, svc.Name)
		}
		if svc.Async {
			sim.asyncNames = append(sim.asyncNames, svc.Name)
		}
	}
	heap.Init(&sim.heap)

	lateHorizon := 300 * time.Second
	rootLimit := cfg.Duration - lateHorizon
	if rootLimit < time.Minute {
		rootLimit = cfg.Duration
	}
	nextRoot := make([]float64, len(services))
	for i, svc := range services {
		nextRoot[i] = sim.rng.Float64() / svc.RPS
	}

	totalSeconds := int(cfg.Duration / time.Second)
	tickSeconds := int(cfg.Tick / time.Second)
	if tickSeconds < 1 {
		tickSeconds = 1
	}
	var steadySnap stats.Snapshot
	rootLimitSeconds := int(rootLimit / time.Second)
	for sec := 0; sec <= totalSeconds; sec += tickSeconds {
		elapsed := time.Duration(sec) * time.Second

		for i, svc := range services {
			for nextRoot[i] <= float64(sec) && nextRoot[i] <= rootLimit.Seconds() {
				baseElapsed := time.Duration(nextRoot[i] * float64(time.Second))
				sim.makeTrace(svc, sim.epoch.Add(baseElapsed), baseElapsed)
				rate := sim.rootRate(svc, baseElapsed)
				nextRoot[i] += sim.rng.ExpFloat64() / rate
			}
		}

		now := sim.epoch.Add(elapsed)
		var due []model.Span
		for sim.heap.Len() > 0 && !sim.heap[0].at.After(now) {
			x := heap.Pop(&sim.heap).(scheduledSpan)
			due = append(due, x.span)
		}
		if len(due) > 0 {
			pipe.Ingest(due...)
		}
		pipe.Sweep(now)
		if sec == rootLimitSeconds {
			steadySnap = pipe.Stats.SnapshotAt(now)
		}
	}

	end := sim.epoch.Add(cfg.Duration)
	pipe.FlushAll(end)
	snap := pipe.Stats.SnapshotAt(end.Add(time.Second))
	srep := out.Report()

	report := &Report{
		Config: cfg, ServiceCount: len(services),
		GeneratedTraces: sim.generatedTraces, GeneratedSpans: sim.generatedSpans,
		ErrorTraces: sim.errorTraces, SlowTraces: sim.slowTraces, HugeTraces: sim.hugeTraces,
		StragglerScheduled: sim.stragglerScheduled, LateScheduled: sim.lateScheduled,
		MustKeep: len(sim.mustKeep), SinkReport: srep, Snapshot: snap,
		SteadySnapshot: steadySnap,
	}

	for id, info := range sim.mustKeep {
		if _, ok := out.KeptManifest(id); ok {
			continue
		}
		detail := fmt.Sprintf("%d(%s n=%d expectedDur=%s maxGap=%s lastMain=%s",
			id, info.reason, info.spanCount, info.expectedDuration.Round(time.Millisecond),
			info.maxArrivalGap.Round(time.Millisecond), info.lastMainArrival.Format("15:04:05.000"))
		if !info.stragglerAt.IsZero() {
			detail += " straggler+" + info.stragglerAt.Sub(info.lastMainArrival).Round(time.Millisecond).String()
		}
		if !info.trueLateAt.IsZero() {
			detail += " late+" + info.trueLateAt.Sub(info.lastMainArrival).Round(time.Millisecond).String()
		}
		if m, ok := out.Manifests[id]; ok {
			detail += fmt.Sprintf(" decisionKeep=%v decisionDur=%s ext=%d decisionSpans=%d manifestSpans=%d late=%d policy=%s",
				m.Decision.Keep, m.Decision.TotalDuration.Round(time.Millisecond), m.Decision.ExtensionsUsed,
				m.Decision.SpanCount, m.SpansInBatches, m.LateSpans, m.Decision.PolicyID)
		} else {
			detail += " no-manifest"
		}
		detail += ")"
		report.MissedMustKeep = append(report.MissedMustKeep, detail)
	}
	sort.Strings(report.MissedMustKeep)

	names := make([]string, 0, len(snap.Services))
	byName := map[string]stats.ServiceSnapshot{}
	for _, s := range snap.Services {
		byName[s.Name] = s
		names = append(names, s.Name)
	}
	sort.Strings(names)
	steadyByName := map[string]stats.ServiceSnapshot{}
	for _, s := range steadySnap.Services {
		steadyByName[s.Name] = s
	}
	for _, name := range names {
		s := byName[name]
		p := s.Probability
		// The final snapshot is taken after the artificial late-event drain,
		// when recent-window rates decay to zero. Show the last traffic-time
		// controller ratio in the human verification table.
		if steady, ok := steadyByName[name]; ok {
			p = steady.Probability
		}
		report.ServiceReports = append(report.ServiceReports, ServiceReport{
			Name: name, TargetRPS: s.TargetRPS, ActualRPS: s.ActualRPS,
			DeviationPct: s.DeviationPct, Probability: p,
			Arrivals: s.Arrivals, Kept: s.Kept,
		})
		abs := absFloat(s.DeviationPct)
		if abs > report.MaxAbsDeviationPct {
			report.MaxAbsDeviationPct = abs
			report.WorstService = name
		}
	}
	for _, s := range steadySnap.Services {
		if abs := absFloat(s.WindowDeviationPct); abs > report.MaxWindowDeviationPct {
			report.MaxWindowDeviationPct = abs
			report.WorstWindowService = s.Name
		}
	}

	report.Checks = sim.buildChecks(report)
	report.Pass = true
	for _, c := range report.Checks {
		if !c.Pass {
			report.Pass = false
			break
		}
	}
	return report, nil
}

func (sim *simulator) nextTraceID() uint64 {
	for {
		id := sim.rng.Uint64()
		if id == 0 {
			continue
		}
		if _, ok := sim.seenIDs[id]; ok {
			continue
		}
		sim.seenIDs[id] = struct{}{}
		return id
	}
}

func (sim *simulator) nextSpanID() uint64 {
	sim.spanSeq++
	return sim.spanSeq
}

func (sim *simulator) rootRate(svc ServiceInfo, elapsed time.Duration) float64 {
	rate := svc.RPS
	// Fault window: retries and operator failover add some load.
	if elapsed >= 50*time.Minute && elapsed < 65*time.Minute {
		rate *= 1.35
	}
	// Lunch/morning ticket traffic spike.
	if elapsed >= 105*time.Minute && elapsed < 125*time.Minute {
		rate *= 3
	}
	return rate
}

func (sim *simulator) errorChance(elapsed time.Duration) float64 {
	if elapsed >= 50*time.Minute && elapsed < 65*time.Minute {
		return 0.38
	}
	return 0.004
}

func (sim *simulator) slowChance(elapsed time.Duration) float64 {
	if elapsed >= 150*time.Minute && elapsed < 165*time.Minute {
		return 0.30
	}
	return 0.015
}

func (sim *simulator) makeTrace(root ServiceInfo, base time.Time, elapsed time.Duration) {
	sim.generatedTraces++
	traceID := sim.nextTraceID()

	errorTrace := sim.rng.Float64() < sim.errorChance(elapsed)
	slowTrace := sim.rng.Float64() < sim.slowChance(elapsed)
	vip := sim.rng.Float64() < 0.035
	refund := sim.rng.Float64() < 0.018
	includePayment := refund || sim.rng.Float64() < 0.075

	lateRoll := sim.rng.Float64()
	straggler := lateRoll < 0.035
	trueLate := !straggler && lateRoll < 0.075
	huge := sim.rng.Float64() < 0.018
	useAsync := !straggler && sim.rng.Float64() < 0.18

	var must *mustTrace
	if errorTrace || slowTrace {
		reason := "error"
		if !errorTrace {
			reason = "duration"
		}
		must = &mustTrace{reason: reason, spanCount: -1}
		sim.mustKeep[traceID] = must
	}

	spanCount := 4 + sim.rng.IntN(10)
	if huge {
		spanCount = 70 + sim.rng.IntN(180)
		sim.hugeTraces++
	}
	if errorTrace {
		sim.errorTraces++
	}
	if slowTrace {
		sim.slowTraces++
	}

	// Keep individual inter-fragment gaps below the normal 2s silence window.
	// A slow trace is slow because several calls are slow, not because one
	// multi-second reporting hole can make the aggregator close prematurely.
	durations := make([]time.Duration, spanCount)
	for i := range durations {
		if huge {
			durations[i] = time.Duration(1+sim.rng.IntN(4)) * time.Millisecond
		} else {
			durations[i] = time.Duration(10+sim.rng.IntN(70)) * time.Millisecond
		}
	}
	if slowTrace && spanCount >= 3 {
		slowCalls := 2 + sim.rng.IntN(3)
		for j := 0; j < slowCalls; j++ {
			idx := 1 + sim.rng.IntN(spanCount-1)
			durations[idx] = time.Duration(500+sim.rng.IntN(700)) * time.Millisecond
		}
		// Random slow-call indexes may collide; guarantee the simulated trace
		// really crosses the one-second total-duration policy threshold.
		var sum time.Duration
		for _, d := range durations {
			sum += d
		}
		if sum < 1100*time.Millisecond {
			durations[1] += 750 * time.Millisecond
		}
	}
	if must != nil {
		for _, d := range durations {
			must.expectedDuration += d
		}
	}

	errorIndex := -1
	if errorTrace {
		errorIndex = sim.rng.IntN(spanCount)
	}
	paymentIndex := -1
	if includePayment && spanCount >= 2 {
		paymentIndex = 1 + sim.rng.IntN(spanCount-1)
	}
	asyncIndex := -1
	if useAsync && len(sim.asyncNames) > 0 && spanCount >= 3 {
		asyncIndex = 2 + sim.rng.IntN(spanCount-2)
	}

	cursor := base
	var parent uint64
	var lastArrival time.Time
	var lastEnd time.Time
	for i := 0; i < spanCount; i++ {
		d := durations[i]
		start := cursor
		end := start.Add(d)
		cursor = end

		service := root.Name
		if i > 0 {
			service = sim.downstream[sim.rng.IntN(len(sim.downstream))]
		}
		operation := "Process"
		kind := "sync"
		if i == 0 {
			operation = "ReceiveRequest"
		}
		if i == paymentIndex {
			service = "payment-service"
			operation = "Charge"
			if refund {
				operation = "Refund"
			}
		}
		if i == asyncIndex {
			service = sim.asyncNames[sim.rng.IntN(len(sim.asyncNames))]
			kind = "async"
			operation = "ConsumeMessage"
		} else if op, ok := operationFor(service); ok && operation == "Process" {
			operation = op
		}

		reportDelay := time.Duration(20+sim.rng.IntN(180)) * time.Millisecond
		if kind == "async" {
			// The service's configured silence is longer; the reporting delay
			// itself stays inside that window so normal async work is aggregated.
			reportDelay = time.Duration(100+sim.rng.IntN(900)) * time.Millisecond
		}
		arrival := end.Add(reportDelay)

		span := model.Span{
			TraceID: traceID, SpanID: sim.nextSpanID(), ParentID: parent,
			RootService: root.Name, Service: service, Operation: operation, Kind: kind,
			Start: start, End: end, Arrival: arrival, Error: i == errorIndex,
		}
		if i == 0 {
			span.ParentID = 0
			span.Tags = map[string]string{"resort": "general-slope"}
			if vip {
				span.Tags["ticket.class"] = "vip"
			}
		}
		if span.Error {
			if span.Tags == nil {
				span.Tags = map[string]string{}
			}
			span.Tags["error"] = "true"
		}
		sim.schedule(arrival, span)
		sim.generatedSpans++
		if must != nil {
			must.mainArrivals = append(must.mainArrivals, arrival)
			must.spanCount = spanCount
		}
		if arrival.After(lastArrival) {
			lastArrival = arrival
		}
		if end.After(lastEnd) {
			lastEnd = end
		}
		parent = span.SpanID
	}

	if must != nil {
		must.lastMainArrival = lastArrival
		sort.Slice(must.mainArrivals, func(i, j int) bool { return must.mainArrivals[i].Before(must.mainArrivals[j]) })
		for i := 1; i < len(must.mainArrivals); i++ {
			if g := must.mainArrivals[i].Sub(must.mainArrivals[i-1]); g > must.maxArrivalGap {
				must.maxArrivalGap = g
			}
		}
	}

	if straggler {
		sim.stragglerScheduled++
		delay := sim.pipeConfigDefaultSilence() + time.Duration(300+sim.rng.IntN(700))*time.Millisecond
		arrival := lastArrival.Add(delay)
		sp := model.Span{
			TraceID: traceID, SpanID: sim.nextSpanID(), ParentID: parent,
			RootService: root.Name, Service: "gate-access", Operation: "DelayedAck",
			Kind: "sync", Start: lastEnd.Add(50 * time.Millisecond),
			End: lastEnd.Add(180 * time.Millisecond), Arrival: arrival,
		}
		sim.schedule(arrival, sp)
		sim.generatedSpans++
		if must != nil {
			must.stragglerAt = arrival
		}
	}

	if trueLate {
		sim.lateScheduled++
		delay := time.Duration(25+sim.rng.IntN(215)) * time.Second
		count := 1 + sim.rng.IntN(2)
		lateCursor := lastEnd.Add(time.Second)
		if must != nil {
			must.trueLateAt = lastArrival.Add(delay)
		}
		for i := 0; i < count; i++ {
			arrival := lastArrival.Add(delay + time.Duration(i*100)*time.Millisecond)
			sp := model.Span{
				TraceID: traceID, SpanID: sim.nextSpanID(), ParentID: parent,
				RootService: root.Name, Service: root.Name, Operation: "LateMetric",
				Kind: "async", Start: lateCursor, End: lateCursor.Add(80 * time.Millisecond),
				Arrival: arrival,
			}
			sim.schedule(arrival, sp)
			sim.generatedSpans++
			lateCursor = lateCursor.Add(200 * time.Millisecond)
			parent = sp.SpanID
		}
	}
}

func (sim *simulator) schedule(at time.Time, span model.Span) {
	heap.Push(&sim.heap, scheduledSpan{at: at, span: span})
}

func (sim *simulator) pipeConfigDefaultSilence() time.Duration {
	return 2 * time.Second
}

func operationFor(service string) (string, bool) {
	ops := map[string]string{
		"booking-service":      "CreateBooking",
		"ticket-issuer":        "IssueTicket",
		"gate-access":          "OpenGate",
		"snowmaking-scheduler": "DispatchSnow",
		"snow-gun-control":     "StartGun",
		"lift-monitor":         "RideScan",
		"weather-station":      "ReadWeather",
		"patrol-incident":      "CreateIncident",
		"pos-service":          "Purchase",
		"ski-school":           "BookLesson",
		"notification-worker":  "SendMessage",
		"iot-mqtt-gateway":     "Telemetry",
	}
	op, ok := ops[service]
	return op, ok
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func (sim *simulator) buildChecks(r *Report) []Check {
	checks := []Check{}
	checks = append(checks, Check{
		Name:   "错误和超耗时链路零漏采",
		Pass:   len(r.MissedMustKeep) == 0,
		Detail: fmt.Sprintf("必须保留 %d 条，漏采 %d 条", r.MustKeep, len(r.MissedMustKeep)),
	})
	checks = append(checks, Check{
		Name: "保留/决策批次片段齐全",
		Pass: len(r.SinkReport.Mismatches) == 0,
		Detail: fmt.Sprintf("决策 %d，批次 %d/%d 片段，不一致 %d",
			r.SinkReport.Decisions, r.SinkReport.RawBatches, r.SinkReport.RawBatchSpans, len(r.SinkReport.Mismatches)),
	})
	checks = append(checks, Check{
		Name: "各服务累计保留速率偏差",
		Pass: r.MaxAbsDeviationPct <= r.Config.MaxDeviation*100,
		Detail: fmt.Sprintf("最大绝对偏差 %.2f%% (%s)，允许 %.0f%%",
			r.MaxAbsDeviationPct, r.WorstService, r.Config.MaxDeviation*100),
	})
	checks = append(checks, Check{
		Name:   "静默到期后顺延至少发生",
		Pass:   r.Snapshot.Extensions > 0,
		Detail: fmt.Sprintf("顺延 %d 次，模拟边界片段 %d", r.Snapshot.Extensions, r.StragglerScheduled),
	})
	checks = append(checks, Check{
		Name: "迟到片段覆盖缓存和淘汰路径",
		Pass: r.Snapshot.Late.Total > 0 && r.Snapshot.Late.CacheEvicted > 0 &&
			(r.Snapshot.Late.CachedKeep+r.Snapshot.Late.CachedDrop) > 0 &&
			(r.Snapshot.Late.IndependentKeep+r.Snapshot.Late.IndependentDrop) > 0,
		Detail: fmt.Sprintf("迟到 %d，缓存命中 keep/drop=%d/%d，淘汰后独立 keep/drop=%d/%d",
			r.Snapshot.Late.Total, r.Snapshot.Late.CachedKeep, r.Snapshot.Late.CachedDrop,
			r.Snapshot.Late.IndependentKeep, r.Snapshot.Late.IndependentDrop),
	})
	checks = append(checks, Check{
		Name: "超大链路分批送出",
		Pass: r.Snapshot.Batches.Early > 0,
		Detail: fmt.Sprintf("超大链路 %d，提前批次 %d，批次片段 %d",
			r.HugeTraces, r.Snapshot.Batches.Early, r.Snapshot.Batches.Spans),
	})
	checks = append(checks, Check{
		Name: "缓冲估算内存未超过配置上限",
		Pass: r.Snapshot.Buffered.PeakBytes <= r.Snapshot.Buffered.CapacityBytes,
		Detail: fmt.Sprintf("峰值 %d bytes，上限 %d bytes",
			r.Snapshot.Buffered.PeakBytes, r.Snapshot.Buffered.CapacityBytes),
	})
	return checks
}
