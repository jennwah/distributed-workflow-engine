// Package retry provides configurable retry policies with exponential backoff.
package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy defines parameters for retry behavior with exponential backoff and jitter.
type Policy struct {
	MaxRetries  int           `json:"max_retries"`
	BaseDelay   time.Duration `json:"base_delay"`
	MaxDelay    time.Duration `json:"max_delay"`
	Multiplier  float64       `json:"multiplier"`
	JitterRatio float64       `json:"jitter_ratio"` // 0.0 to 1.0
}

// DefaultPolicy returns a sensible default retry policy.
func DefaultPolicy() *Policy {
	return &Policy{
		MaxRetries:  3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0.1,
	}
}

// NextDelay computes the delay before the next retry attempt using
// exponential backoff with jitter.
func (p *Policy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return p.BaseDelay
	}

	delay := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1))

	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	// Apply jitter: ±jitterRatio of the delay.
	jitter := delay * p.JitterRatio * (2*rand.Float64() - 1)
	delay += jitter

	if delay < 0 {
		delay = float64(p.BaseDelay)
	}

	return time.Duration(delay)
}

// ShouldRetry reports whether another attempt should be made.
func (p *Policy) ShouldRetry(attempt int) bool {
	return attempt < p.MaxRetries
}
