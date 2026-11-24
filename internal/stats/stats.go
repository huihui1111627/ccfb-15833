package stats

import (
	"runtime"
	"sort"
	"sync"
	"time"

	"skiresort/tail-sampling/internal/model"
)

const MaxLateRecords = 80

type WindowRates struct {
	Arrival   float64
	Kept      float64
	Mandatory float64
	Eligible  float64
	ProbHit   float64
}

type window struct {
	buckets []int64
	stamps  []int64
}

func newWindow(seconds int) *window {
	return &window{buckets: make([]int64, seconds), stamps: make([]int64, seconds)}
}

func (w *window) add(now time.Time, n int64) {
	if n == 0 {
		return
	}
	sec := now.Unix()
	size := int64(len(w.buckets))
	idx := sec % size
	if w.stamps[idx] != sec {
		w.stamps[idx] = sec
		w.buckets[idx] = 0
	}
	w.buckets[idx] += n
}

func (w *window) count(now time.Time, span time.Duration) int64 {
	seconds := int64(span / time.Second)
	if seconds <= 0 || seconds > int64(len(w.buckets)) {
		seconds = int64(len(w.buckets))
	}
	cur := now.Unix()
	var total int64
	for i := int64(0); i < seconds; i++ {
		sec := cur - i
		idx := sec % int64(len(w.buckets))
		// Go modulo of negative values is negative; simulated epochs are positive.
		if idx < 0 {
			idx += int64(len(w.buckets))
		}
		if w.stamps[idx] == sec {
			total += w.buckets[idx]
		}
	}
	return total
}

type serviceStats struct {
	Name string

	firstSeen time.Time
	lastNow   time.Time

	arrivalWindow   *window
	keptWindow      *window
	mandatoryWindow *window
	eligibleWindow  *window
	probHitWindow   *window

	Arrivals       int64
	Decisions      int64
	Kept           int64
	Dropped        int64
	MandatoryKept  int64
	Eligible       int64
	ProbabilityHit int64
	ForcedOldest   int64
}

type PolicyStat struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
	Hits     int64  `json:"hits"`
}

type LateCounts struct {
	Total           int64              `json:"total"`
	CachedKeep      int64              `json:"cached_keep"`
	CachedDrop      int64              `json:"cached_drop"`
	IndependentKeep int64              `json:"independent_keep"`
	IndependentDrop int64              `json:"independent_drop"`
	CacheEvicted    int64              `json:"cache_evicted"`
	Recent          []model.LateRecord `json:"recent"`
}

type Snapshot struct {
	GeneratedAt   time.Time          `json:"generated_at"`
	FirstEventAt  time.Time          `json:"first_event_at"`
	LastEventAt   time.Time          `json:"last_event_at"`
	UptimeSeconds float64            `json:"uptime_seconds"`
	Buffered      BufferSnapshot     `json:"buffered"`
	Decisions     DecisionSnapshot   `json:"decisions"`
	Policies      []PolicyStat       `json:"policies"`
	Services      []ServiceSnapshot  `json:"services"`
	Late          LateCounts         `json:"late"`
	Extensions    int64              `json:"extensions"`
	Batches       BatchSnapshot      `json:"batches"`
	Controller    ControllerSnapshot `json:"controller"`
}

type BufferSnapshot struct {
	Traces         int     `json:"traces"`
	EstimatedBytes int64   `json:"estimated_bytes"`
	CapacityBytes  int64   `json:"capacity_bytes"`
	PeakBytes      int64   `json:"peak_bytes"`
	PeakTraces     int     `json:"peak_traces"`
	GoHeapAlloc    uint64  `json:"go_heap_alloc"`
	GoHeapSys      uint64  `json:"go_heap_sys"`
	GoSys          uint64  `json:"go_sys"`
	Goroutines     int     `json:"goroutines"`
	MemoryLimitPct float64 `json:"memory_limit_pct"`
}

type DecisionSnapshot struct {
	Keep         int64   `json:"keep"`
	Drop         int64   `json:"drop"`
	Total        int64   `json:"total"`
	KeepRate     float64 `json:"keep_rate"`
	ForcedOldest int64   `json:"forced_oldest"`
}

type BatchSnapshot struct {
	Batches int64 `json:"batches"`
	Spans   int64 `json:"spans"`
	Early   int64 `json:"early"`
}

type ServiceSnapshot struct {
	Name               string  `json:"name"`
	TargetRPS          float64 `json:"target_rps"`
	Probability        float64 `json:"probability"`
	ActualRPS          float64 `json:"actual_rps"`
	WindowRPS          float64 `json:"window_rps"`
	ArrivalRPS         float64 `json:"arrival_rps"`
	MandatoryRPS       float64 `json:"mandatory_rps"`
	EligibleRPS        float64 `json:"eligible_rps"`
	Kept               int64   `json:"kept"`
	Dropped            int64   `json:"dropped"`
	Arrivals           int64   `json:"arrivals"`
	DeviationPct       float64 `json:"deviation_pct"`
	WindowDeviationPct float64 `json:"window_deviation_pct"`
}

type ControllerSnapshot struct {
	WindowSeconds int       `json:"window_seconds"`
	LastAdjustAt  time.Time `json:"last_adjust_at"`
	Adjustments   int64     `json:"adjustments"`
}

type Stats struct {
	mu sync.RWMutex

	windowSeconds int
	policies      map[string]*PolicyStat
	services      map[string]*serviceStats
	targets       map[string]float64
	probabilities map[string]float64

	bufferedTraces int
	bufferBytes    int64
	bufferCap      int64
	peakBytes      int64
	peakTraces     int

	keep, drop     int64
	forcedOldest   int64
	extensions     int64
	batches        int64
	batchSpans     int64
	earlyBatches   int64
	adjustments    int64
	lastAdjustAt   time.Time
	windowDuration time.Duration

	late       LateCounts
	lateRecent []model.LateRecord

	firstEventAt time.Time
	lastEventAt  time.Time
}

func New(windowSeconds int, bufferCap int64, policies []PolicyStat) *Stats {
	if windowSeconds <= 0 {
		windowSeconds = 180
	}
	s := &Stats{
		windowSeconds:  windowSeconds,
		windowDuration: time.Duration(windowSeconds) * time.Second,
		policies:       make(map[string]*PolicyStat),
		services:       make(map[string]*serviceStats),
		targets:        make(map[string]float64),
		probabilities:  make(map[string]float64),
		bufferCap:      bufferCap,
	}
	for _, p := range policies {
		pc := p
		s.policies[pc.ID] = &pc
	}
	s.late.Recent = []model.LateRecord{}
	return s
}

func (s *Stats) ensureServiceLocked(name string, now time.Time) *serviceStats {
	if name == "" {
		name = "unknown"
	}
	svc, ok := s.services[name]
	if !ok {
		svc = &serviceStats{
			Name: name, firstSeen: now, lastNow: now,
			arrivalWindow:   newWindow(s.windowSeconds),
			keptWindow:      newWindow(s.windowSeconds),
			mandatoryWindow: newWindow(s.windowSeconds),
			eligibleWindow:  newWindow(s.windowSeconds),
			probHitWindow:   newWindow(s.windowSeconds),
		}
		s.services[name] = svc
	}
	if s.firstEventAt.IsZero() || now.Before(s.firstEventAt) {
		s.firstEventAt = now
	}
	if now.After(s.lastEventAt) {
		s.lastEventAt = now
	}
	return svc
}

func (s *Stats) SetTarget(name string, target float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.targets[name] = target
	if _, ok := s.probabilities[name]; !ok {
		s.probabilities[name] = 0.2
	}
}

func (s *Stats) SetProbability(name string, p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probabilities[name] = p
}

func (s *Stats) ControllerAdjusted(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adjustments++
	s.lastAdjustAt = now
	if s.firstEventAt.IsZero() {
		s.firstEventAt = now
	}
	if now.After(s.lastEventAt) {
		s.lastEventAt = now
	}
}

func (s *Stats) RecordArrival(name string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc := s.ensureServiceLocked(name, now)
	svc.Arrivals++
	svc.arrivalWindow.add(now, 1)
	svc.lastNow = now
}

func (s *Stats) RecordBuffer(traces int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bufferedTraces = traces
	s.bufferBytes = bytes
	if bytes > s.peakBytes {
		s.peakBytes = bytes
	}
	if traces > s.peakTraces {
		s.peakTraces = traces
	}
}

func (s *Stats) RecordBatch(spanCount int, early bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches++
	s.batchSpans += int64(spanCount)
	if early {
		s.earlyBatches++
	}
}

func (s *Stats) RecordExtension() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extensions++
}

func (s *Stats) RecordDecision(d model.Decision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := d.DecidedAt
	svc := s.ensureServiceLocked(d.RootService, now)
	svc.Decisions++
	svc.lastNow = now
	if d.Keep {
		s.keep++
		svc.Kept++
		svc.keptWindow.add(now, 1)
	} else {
		s.drop++
		svc.Dropped++
	}
	if d.MandatoryKeep {
		svc.MandatoryKept++
		svc.mandatoryWindow.add(now, 1)
	} else {
		svc.Eligible++
		svc.eligibleWindow.add(now, 1)
	}
	// ProbabilityHit is inferred from matched policy list.
	for _, id := range d.MatchedPolicies {
		if p, ok := s.policies[id]; ok {
			p.Hits++
		}
		if id == "probability" {
			svc.ProbabilityHit++
			svc.probHitWindow.add(now, 1)
		}
	}
	if d.ForcedOldest {
		s.forcedOldest++
		svc.ForcedOldest++
	}
}

func (s *Stats) RecordLate(r model.LateRecord, evicted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.late.Total++
	if evicted {
		s.late.CacheEvicted++
	}
	switch r.Mode {
	case "cached_keep":
		s.late.CachedKeep++
	case "cached_drop":
		s.late.CachedDrop++
	case "independent_keep":
		s.late.IndependentKeep++
	case "independent_drop":
		s.late.IndependentDrop++
	}
	s.lateRecent = append([]model.LateRecord{r}, s.lateRecent...)
	if len(s.lateRecent) > MaxLateRecords {
		s.lateRecent = s.lateRecent[:MaxLateRecords]
	}
	s.late.Recent = append([]model.LateRecord(nil), s.lateRecent...)
	if !r.At.IsZero() && r.At.After(s.lastEventAt) {
		s.lastEventAt = r.At
	}
}

// WindowRates returns per-service rates over the configured recent window.
func (s *Stats) WindowRates(now time.Time) map[string]WindowRates {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]WindowRates, len(s.services))
	for name, svc := range s.services {
		den := s.windowDenominatorLocked(svc, now)
		out[name] = WindowRates{
			Arrival:   float64(svc.arrivalWindow.count(now, s.windowDuration)) / den,
			Kept:      float64(svc.keptWindow.count(now, s.windowDuration)) / den,
			Mandatory: float64(svc.mandatoryWindow.count(now, s.windowDuration)) / den,
			Eligible:  float64(svc.eligibleWindow.count(now, s.windowDuration)) / den,
			ProbHit:   float64(svc.probHitWindow.count(now, s.windowDuration)) / den,
		}
	}
	return out
}

func (s *Stats) windowDenominatorLocked(svc *serviceStats, now time.Time) float64 {
	age := now.Sub(svc.firstSeen).Seconds()
	if age < 1 {
		age = 1
	}
	w := s.windowDuration.Seconds()
	if age < w {
		return age
	}
	return w
}

// CumulativeRates returns retained traces/sec over the observed event interval.
func (s *Stats) CumulativeRates() map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]float64, len(s.services))
	for name, svc := range s.services {
		end := svc.lastNow
		if end.IsZero() {
			end = s.lastEventAt
		}
		secs := end.Sub(svc.firstSeen).Seconds()
		if secs <= 0 {
			secs = 1
		}
		out[name] = float64(svc.Kept) / secs
	}
	return out
}

// BudgetCorrections returns excess retained traces per correction second:
// (kept - target*age) / horizon. Positive means the service is over its
// cumulative budget and the controller should temporarily sample below the
// recent-window feed-forward ratio to repay the excess.
func (s *Stats) BudgetCorrections(now time.Time, horizon time.Duration) map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	horizonSeconds := horizon.Seconds()
	if horizonSeconds <= 0 {
		horizonSeconds = 1
	}
	out := make(map[string]float64, len(s.services))
	for name, svc := range s.services {
		target := s.targets[name]
		if target <= 0 {
			continue
		}
		end := now
		if end.IsZero() {
			end = svc.lastNow
		}
		age := end.Sub(svc.firstSeen).Seconds()
		if age <= 0 {
			continue
		}
		excess := float64(svc.Kept) - target*age
		out[name] = excess / horizonSeconds
	}
	return out
}

func (s *Stats) Snapshot() Snapshot {
	return s.SnapshotAt(time.Now())
}

func (s *Stats) SnapshotAt(now time.Time) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	policies := make([]PolicyStat, 0, len(s.policies))
	for _, p := range s.policies {
		policies = append(policies, *p)
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].Priority < policies[j].Priority })

	names := make([]string, 0, len(s.services))
	for name := range s.services {
		names = append(names, name)
	}
	sort.Strings(names)

	services := make([]ServiceSnapshot, 0, len(names))
	cumulative := map[string]float64{}
	for name, svc := range s.services {
		secs := svc.lastNow.Sub(svc.firstSeen).Seconds()
		if secs <= 0 {
			secs = 1
		}
		cumulative[name] = float64(svc.Kept) / secs
	}
	for _, name := range names {
		svc := s.services[name]
		target := s.targets[name]
		p := s.probabilities[name]
		actual := cumulative[name]
		den := s.windowDenominatorLocked(svc, now)
		windowActual := float64(svc.keptWindow.count(now, s.windowDuration)) / den
		arrival := float64(svc.arrivalWindow.count(now, s.windowDuration)) / den
		mandatory := float64(svc.mandatoryWindow.count(now, s.windowDuration)) / den
		eligible := float64(svc.eligibleWindow.count(now, s.windowDuration)) / den
		deviation := 0.0
		if target > 0 {
			deviation = (actual - target) / target * 100
		}
		windowDeviation := 0.0
		if target > 0 {
			windowDeviation = (windowActual - target) / target * 100
		}
		services = append(services, ServiceSnapshot{
			Name: name, TargetRPS: target, Probability: p,
			ActualRPS: actual, WindowRPS: windowActual,
			ArrivalRPS: arrival, MandatoryRPS: mandatory, EligibleRPS: eligible,
			Kept: svc.Kept, Dropped: svc.Dropped, Arrivals: svc.Arrivals,
			DeviationPct: deviation, WindowDeviationPct: windowDeviation,
		})
	}

	total := s.keep + s.drop
	keepRate := 0.0
	if total > 0 {
		keepRate = float64(s.keep) / float64(total)
	}
	uptime := 0.0
	if !s.firstEventAt.IsZero() {
		end := s.lastEventAt
		if now.After(end) {
			end = now
		}
		uptime = end.Sub(s.firstEventAt).Seconds()
	}
	late := s.late
	late.Recent = append([]model.LateRecord(nil), s.lateRecent...)
	return Snapshot{
		GeneratedAt: now, FirstEventAt: s.firstEventAt, LastEventAt: s.lastEventAt,
		UptimeSeconds: uptime,
		Buffered: BufferSnapshot{
			Traces: s.bufferedTraces, EstimatedBytes: s.bufferBytes, CapacityBytes: s.bufferCap,
			PeakBytes: s.peakBytes, PeakTraces: s.peakTraces,
			GoHeapAlloc: ms.HeapAlloc, GoHeapSys: ms.HeapSys, GoSys: ms.Sys,
			Goroutines: runtime.NumGoroutine(),
			MemoryLimitPct: func() float64 {
				if s.bufferCap <= 0 {
					return 0
				}
				return float64(s.bufferBytes) / float64(s.bufferCap) * 100
			}(),
		},
		Decisions: DecisionSnapshot{
			Keep: s.keep, Drop: s.drop, Total: total, KeepRate: keepRate, ForcedOldest: s.forcedOldest,
		},
		Policies: policies, Services: services, Late: late,
		Extensions: s.extensions,
		Batches:    BatchSnapshot{Batches: s.batches, Spans: s.batchSpans, Early: s.earlyBatches},
		Controller: ControllerSnapshot{
			WindowSeconds: s.windowSeconds, LastAdjustAt: s.lastAdjustAt, Adjustments: s.adjustments,
		},
	}
}
