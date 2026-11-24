package model

import "time"

// Span is one call-chain fragment. JSON ids are strings because trace ids
// are uint64 and would lose precision in browser JavaScript numbers.
type Span struct {
	TraceID     uint64            `json:"trace_id,string"`
	SpanID      uint64            `json:"span_id,string"`
	ParentID    uint64            `json:"parent_id,string"`
	RootService string            `json:"root_service"`
	Service     string            `json:"service"`
	Operation   string            `json:"operation"`
	Kind        string            `json:"kind"`
	Start       time.Time         `json:"start"`
	End         time.Time         `json:"end"`
	Arrival     time.Time         `json:"arrival"`
	Error       bool              `json:"error"`
	Tags        map[string]string `json:"tags,omitempty"`
}

func (s Span) Duration() time.Duration {
	if s.End.After(s.Start) {
		return s.End.Sub(s.Start)
	}
	return 0
}

// Batch is a group of fragments sent before the whole trace is complete.
// Early batches are provisional; a Decision later commits or discards them.
type Batch struct {
	TraceID     uint64    `json:"trace_id,string"`
	BatchNo     int       `json:"batch_no"`
	Final       bool      `json:"final"`
	Provisional bool      `json:"provisional"`
	SpanCount   int       `json:"span_count"`
	Spans       []Span    `json:"spans,omitempty"`
	SentAt      time.Time `json:"sent_at"`
}

// Decision is the tail-sampling result for a trace.
type Decision struct {
	TraceID         uint64        `json:"trace_id,string"`
	Keep            bool          `json:"keep"`
	MandatoryKeep   bool          `json:"mandatory_keep"`
	Reason          string        `json:"reason"`
	PolicyID        string        `json:"policy_id"`
	MatchedPolicies []string      `json:"matched_policies"`
	RootService     string        `json:"root_service"`
	Probability     float64       `json:"probability"`
	Bucket          uint64        `json:"bucket"`
	BucketLimit     uint64        `json:"bucket_limit"`
	SpanCount       int           `json:"span_count"`
	BatchCount      int           `json:"batch_count"`
	ChecksumSum     uint64        `json:"checksum_sum"`
	ChecksumXOR     uint64        `json:"checksum_xor"`
	TotalDuration   time.Duration `json:"total_duration"`
	Error           bool          `json:"error"`
	DecidedAt       time.Time     `json:"decided_at"`
	ForcedOldest    bool          `json:"forced_oldest"`
	ExtensionsUsed  int           `json:"extensions_used"`
}

// LateRecord describes handling of a span arriving after a decision.
type LateRecord struct {
	At           time.Time `json:"at"`
	TraceID      uint64    `json:"trace_id,string"`
	SpanID       uint64    `json:"span_id,string"`
	Service      string    `json:"service"`
	RootService  string    `json:"root_service"`
	Mode         string    `json:"mode"` // cached_keep, cached_drop, independent_keep, independent_drop
	Reason       string    `json:"reason"`
	PolicyID     string    `json:"policy_id"`
	DecisionKeep bool      `json:"decision_keep"`
	Probability  float64   `json:"probability"`
	Bucket       uint64    `json:"bucket"`
	BucketLimit  uint64    `json:"bucket_limit"`
}
