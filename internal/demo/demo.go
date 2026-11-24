package demo

import (
	"context"
	"math/rand/v2"
	"time"

	"skiresort/tail-sampling/internal/model"
	"skiresort/tail-sampling/internal/pipeline"
	"skiresort/tail-sampling/internal/sim"
)

// Runner emits simulated resort traffic against a live pipeline using the
// wall clock. It is the in-process "reported services" side of the demo.
type Runner struct {
	pipe     *pipeline.Pipeline
	services []sim.ServiceInfo
	seed     uint64
}

func New(pipe *pipeline.Pipeline, seed uint64) *Runner {
	return &Runner{pipe: pipe, services: sim.Services(), seed: seed}
}

func (r *Runner) Start(ctx context.Context) {
	for i, svc := range r.services {
		svc := svc
		seed := r.seed + uint64(i)*1_000_003
		go r.runService(ctx, svc, rand.New(rand.NewPCG(seed, seed^0x55aa55aa)))
	}
}

func (r *Runner) runService(ctx context.Context, svc sim.ServiceInfo, rng *rand.Rand) {
	for {
		cycle := time.Now().Unix() % 150
		rate := svc.RPS
		switch {
		case cycle >= 25 && cycle < 45:
			rate *= 1.4 // fault/error window
		case cycle >= 70 && cycle < 100:
			rate *= 3 // traffic surge
		}
		wait := time.Duration(rng.ExpFloat64() / rate * float64(time.Second))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
			r.emitTrace(svc, rng)
		}
	}
}

func (r *Runner) emitTrace(root sim.ServiceInfo, rng *rand.Rand) {
	now := time.Now()
	cycle := now.Unix() % 150
	errorChance := 0.006
	slowChance := 0.02
	if cycle >= 25 && cycle < 45 {
		errorChance = 0.42
	}
	if cycle >= 110 && cycle < 130 {
		slowChance = 0.32
	}

	traceID := rng.Uint64()
	for traceID == 0 {
		traceID = rng.Uint64()
	}
	errorTrace := rng.Float64() < errorChance
	slowTrace := rng.Float64() < slowChance
	vip := rng.Float64() < 0.05
	huge := rng.Float64() < 0.04
	n := 4 + rng.IntN(9)
	if huge {
		n = 75 + rng.IntN(120)
	}

	services := r.services
	var parent uint64
	spanSeq := uint64(0)
	errorIndex := -1
	if errorTrace {
		errorIndex = rng.IntN(n)
	}
	paymentIndex := -1
	if rng.Float64() < 0.12 && n >= 2 {
		paymentIndex = 1 + rng.IntN(n-1)
	}

	var lastEnd time.Time
	for i := 0; i < n; i++ {
		d := time.Duration(10+rng.IntN(70)) * time.Millisecond
		if slowTrace && i > 0 && rng.Float64() < 0.45 {
			d = time.Duration(500+rng.IntN(700)) * time.Millisecond
		}
		if huge {
			d = time.Duration(2+rng.IntN(8)) * time.Millisecond
		}
		start := now.Add(time.Duration(i*40) * time.Millisecond)
		end := start.Add(d)
		lastEnd = end

		service := root.Name
		if i > 0 {
			service = services[rng.IntN(len(services))].Name
		}
		op := "Process"
		kind := "sync"
		if i == 0 {
			op = "ReceiveRequest"
		}
		if i == paymentIndex {
			service = "payment-service"
			if rng.Float64() < 0.2 {
				op = "Refund"
			} else {
				op = "Charge"
			}
		}
		if root.Async && i == n-1 {
			kind = "async"
			op = "ConsumeMessage"
		}

		spanSeq++
		spanID := traceID + spanSeq // demo-only nonzero deterministic-per-trace ids
		span := model.Span{
			TraceID: traceID, SpanID: spanID, ParentID: parent,
			RootService: root.Name, Service: service, Operation: op, Kind: kind,
			Start: start, End: end, Error: i == errorIndex,
		}
		if i == 0 && vip {
			span.Tags = map[string]string{"ticket.class": "vip"}
		}
		if span.Error {
			if span.Tags == nil {
				span.Tags = map[string]string{}
			}
			span.Tags["error"] = "true"
		}
		parent = spanID

		delay := time.Duration(20+rng.IntN(180)) * time.Millisecond
		if kind == "async" {
			delay = time.Duration(150+rng.IntN(900)) * time.Millisecond
		}
		r.schedule(delay, span)
	}

	// Borderline fragment: usually exercises the one allowed extension.
	if rng.Float64() < 0.08 {
		sp := model.Span{
			TraceID: traceID, SpanID: traceID + 10_000 + uint64(rng.IntN(1000)),
			ParentID: parent, RootService: root.Name, Service: "gate-access",
			Operation: "DelayedAck", Start: lastEnd, End: lastEnd.Add(80 * time.Millisecond),
		}
		r.schedule(2200*time.Millisecond+time.Duration(rng.IntN(700))*time.Millisecond, sp)
	}

	// True late fragments: some hit the decision cache, some arrive after LRU
	// eviction and are independently judged.
	if rng.Float64() < 0.10 {
		lateDelay := time.Duration(8+rng.IntN(55)) * time.Second
		sp := model.Span{
			TraceID: traceID, SpanID: traceID + 20_000 + uint64(rng.IntN(1000)),
			ParentID: parent, RootService: root.Name, Service: root.Name,
			Operation: "LateMetric", Kind: "async",
			Start: lastEnd.Add(time.Second), End: lastEnd.Add(1100 * time.Millisecond),
		}
		r.schedule(lateDelay, sp)
	}
}

func (r *Runner) schedule(delay time.Duration, span model.Span) {
	time.AfterFunc(delay, func() {
		span.Arrival = time.Now()
		r.pipe.Ingest(span)
	})
}
