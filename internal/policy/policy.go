package policy

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"skiresort/tail-sampling/internal/model"
	"skiresort/tail-sampling/internal/sampling"
)

// TraceView is the policy-relevant projection of an aggregated trace. Raw
// spans may already have been sent in batches and removed from memory.
type TraceView struct {
	TraceID       uint64
	RootService   string
	SpanCount     int
	Error         bool
	TotalDuration time.Duration
	Services      map[string]struct{}
	Operations    map[string]struct{}
	Tags          map[string]string
}

func NewTraceView(id uint64) TraceView {
	return TraceView{
		TraceID:    id,
		Services:   make(map[string]struct{}),
		Operations: make(map[string]struct{}),
		Tags:       make(map[string]string),
	}
}

func (v *TraceView) AddSpan(s model.Span) {
	v.SpanCount++
	if s.Error {
		v.Error = true
	}
	if v.RootService == "" {
		v.RootService = s.RootService
	}
	if v.RootService == "" && s.ParentID == 0 {
		v.RootService = s.Service
	}
	v.Services[s.Service] = struct{}{}
	if s.Operation != "" {
		v.Operations[s.Operation] = struct{}{}
	}
	for k, val := range s.Tags {
		// First non-empty value is enough for the configured equality rules.
		if _, exists := v.Tags[k]; !exists {
			v.Tags[k] = val
		}
	}
}

func (v TraceView) HasService(service string) bool {
	_, ok := v.Services[service]
	return ok
}

func (v TraceView) HasOperation(operation string) bool {
	_, ok := v.Operations[operation]
	return ok
}

type Match struct {
	PolicyID   string
	PolicyName string
	Priority   int
	Reason     string
}

type Result struct {
	Keep           bool
	MandatoryKeep  bool
	Primary        Match
	Matches        []Match
	Probability    float64
	ProbabilityHit bool
	Bucket         uint64
	BucketLimit    uint64
}

type Policy interface {
	ID() string
	Name() string
	Priority() int
	Match(v TraceView) (bool, string)
}

type ErrorPolicy struct{ PriorityValue int }

func (p ErrorPolicy) ID() string    { return "error" }
func (p ErrorPolicy) Name() string  { return "出错链路" }
func (p ErrorPolicy) Priority() int { return p.PriorityValue }
func (p ErrorPolicy) Match(v TraceView) (bool, string) {
	if v.Error {
		return true, "链路中存在错误片段"
	}
	return false, ""
}

type DurationPolicy struct {
	ThresholdValue time.Duration
	PriorityValue  int
}

func (p DurationPolicy) ID() string    { return "duration" }
func (p DurationPolicy) Name() string  { return "总耗时阈值" }
func (p DurationPolicy) Priority() int { return p.PriorityValue }
func (p DurationPolicy) Match(v TraceView) (bool, string) {
	if v.TotalDuration >= p.ThresholdValue {
		return true, fmt.Sprintf("总耗时 %s ≥ %s", v.TotalDuration.Round(time.Millisecond), p.ThresholdValue)
	}
	return false, ""
}

type ServiceOperationPolicy struct {
	IDValue       string
	NameValue     string
	Service       string
	Operation     string
	PriorityValue int
}

func (p ServiceOperationPolicy) ID() string    { return p.IDValue }
func (p ServiceOperationPolicy) Name() string  { return p.NameValue }
func (p ServiceOperationPolicy) Priority() int { return p.PriorityValue }
func (p ServiceOperationPolicy) Match(v TraceView) (bool, string) {
	if p.Service != "" && v.HasService(p.Service) {
		return true, "命中服务 " + p.Service
	}
	if p.Operation != "" && v.HasOperation(p.Operation) {
		return true, "命中接口 " + p.Operation
	}
	return false, ""
}

type TagPolicy struct {
	Key, Value    string
	PriorityValue int
}

func (p TagPolicy) ID() string    { return "tag" }
func (p TagPolicy) Name() string  { return "标签取值" }
func (p TagPolicy) Priority() int { return p.PriorityValue }
func (p TagPolicy) Match(v TraceView) (bool, string) {
	if val, ok := v.Tags[p.Key]; ok && val == p.Value {
		return true, fmt.Sprintf("标签 %s=%s", p.Key, p.Value)
	}
	return false, ""
}

// ProbabilityProvider returns the current adaptive threshold for a root service.
type ProbabilityProvider func(rootService string) float64

type Engine struct {
	policies       []Policy
	probabilityFor ProbabilityProvider
}

func NewEngine(probabilityFor ProbabilityProvider, policies ...Policy) *Engine {
	sort.Slice(policies, func(i, j int) bool { return policies[i].Priority() < policies[j].Priority() })
	return &Engine{policies: policies, probabilityFor: probabilityFor}
}

func DefaultPolicies(durationThreshold time.Duration) []Policy {
	return []Policy{
		ErrorPolicy{PriorityValue: 10},
		DurationPolicy{ThresholdValue: durationThreshold, PriorityValue: 20},
		ServiceOperationPolicy{
			IDValue:       "service_operation",
			NameValue:     "重点服务/接口",
			Service:       "payment-service",
			Operation:     "Refund",
			PriorityValue: 30,
		},
		TagPolicy{Key: "ticket.class", Value: "vip", PriorityValue: 40},
	}
}

// Evaluate runs all deterministic policies and the adaptive probability policy.
// Every matching policy is returned for per-policy hit statistics. Any
// deterministic match is a mandatory retain; otherwise probability decides.
func (e *Engine) Evaluate(v TraceView) Result {
	matches := make([]Match, 0, len(e.policies)+1)
	for _, p := range e.policies {
		if hit, reason := p.Match(v); hit {
			matches = append(matches, Match{
				PolicyID: p.ID(), PolicyName: p.Name(), Priority: p.Priority(), Reason: reason,
			})
		}
	}

	root := v.RootService
	if root == "" {
		root = "unknown"
	}
	probability := 0.0
	if e.probabilityFor != nil {
		probability = e.probabilityFor(root)
	}
	pHit, bucket, limit := sampling.Hit(v.TraceID, probability)
	if pHit {
		matches = append(matches, Match{
			PolicyID:   "probability",
			PolicyName: "自适应概率",
			Priority:   100,
			Reason:     fmt.Sprintf("低位桶 %d < 阈值 %d (%.4f)", bucket, limit, probability),
		})
	}

	sort.SliceStable(matches, func(i, j int) bool { return matches[i].Priority < matches[j].Priority })
	res := Result{
		Matches: matches, Probability: probability, ProbabilityHit: pHit,
		Bucket: bucket, BucketLimit: limit,
	}
	if len(matches) > 0 {
		res.Primary = matches[0]
	}
	for _, m := range matches {
		if m.PolicyID != "probability" {
			res.MandatoryKeep = true
			break
		}
	}
	res.Keep = res.MandatoryKeep || pHit
	if len(matches) > 0 {
		res.Primary = matches[0]
	}
	return res
}

// ExplainPolicies is used by diagnostics/tests.
func PolicyIDs(policies []Policy) []string {
	out := make([]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, p.ID())
	}
	return out
}

func JoinPolicyIDs(matches []Match) string {
	ids := make([]string, 0, len(matches))
	for _, m := range matches {
		ids = append(ids, m.PolicyID)
	}
	return strings.Join(ids, ",")
}
