package controller

import (
	"sync"
	"time"

	"skiresort/tail-sampling/internal/stats"
)

// Controller adjusts each root service's adaptive probability from recent
// traffic and mandatory retain rate.
type Controller struct {
	mu sync.Mutex

	targets  map[string]float64
	probs    map[string]float64
	initial  float64
	last     time.Time
	interval time.Duration
}

func New(interval time.Duration, initialProbability float64, targets map[string]float64) *Controller {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	c := &Controller{
		targets:  make(map[string]float64),
		probs:    make(map[string]float64),
		initial:  initialProbability,
		interval: interval,
	}
	for svc, target := range targets {
		c.targets[svc] = target
		c.probs[svc] = initialProbability
	}
	return c
}

func (c *Controller) SetTarget(service string, target float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[service] = target
	if _, ok := c.probs[service]; !ok {
		c.probs[service] = 0.2
	}
}

func (c *Controller) Target(service string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.targets[service]
}

func (c *Controller) Probability(service string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.probs[service]; ok {
		return p
	}
	// Unknown services still get a conservative default rather than all-or-nothing.
	return 0.1
}

func (c *Controller) Probabilities() map[string]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]float64, len(c.probs))
	for k, v := range c.probs {
		out[k] = v
	}
	return out
}

func (c *Controller) MaybeAdjust(now time.Time, rates map[string]stats.WindowRates, corrections map[string]float64) bool {
	c.mu.Lock()
	if !c.last.IsZero() && now.Sub(c.last) < c.interval {
		c.mu.Unlock()
		return false
	}
	c.last = now
	c.mu.Unlock()

	c.AdjustWithBudget(rates, corrections)
	return true
}

func (c *Controller) Adjust(rates map[string]stats.WindowRates) {
	c.AdjustWithBudget(rates, nil)
}

// AdjustWithBudget combines the recent feed-forward ratio with a cumulative
// budget correction. correction[svc] is excess retained traces per repayment
// second; see Stats.BudgetCorrections.
func (c *Controller) AdjustWithBudget(rates map[string]stats.WindowRates, correction map[string]float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for svc, target := range c.targets {
		r := rates[svc]
		desired := target
		if correction != nil {
			desired -= correction[svc]
		}

		var raw float64
		switch {
		case target <= 0:
			raw = 0
		case r.Eligible >= 0.25:
			// target - correction = mandatoryRate + p * eligibleRate
			raw = (desired - r.Mandatory) / r.Eligible
		default:
			// Too few recent eligible decisions for a stable ratio. Freeze the
			// previous probability instead of amplifying a tiny denominator.
			continue
		}
		raw = clamp(raw, 0, 1)

		old := c.probs[svc]
		if old == 0 {
			old = c.initial
		}
		if raw <= 0 {
			// Stop mandatory-policy overshoot from accumulating further.
			c.probs[svc] = 0
		} else {
			// Light smoothing suppresses window-boundary oscillation.
			c.probs[svc] = clamp(0.75*raw+0.25*old, 0, 1)
		}
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
