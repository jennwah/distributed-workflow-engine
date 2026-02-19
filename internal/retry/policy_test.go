package retry

import (
	"testing"
	"time"
)

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	if p.MaxRetries != 3 {
		t.Errorf("expected max_retries 3, got %d", p.MaxRetries)
	}
	if p.BaseDelay != 100*time.Millisecond {
		t.Errorf("expected base_delay 100ms, got %s", p.BaseDelay)
	}
}

func TestNextDelay_ExponentialGrowth(t *testing.T) {
	p := &Policy{
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    10 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0, // Disable jitter for deterministic testing.
	}

	delays := make([]time.Duration, 5)
	for i := 0; i < 5; i++ {
		delays[i] = p.NextDelay(i)
	}

	// With no jitter and multiplier=2, delays should roughly double.
	// Attempt 0: 100ms, Attempt 1: 100ms, Attempt 2: 200ms, etc.
	if delays[0] != 100*time.Millisecond {
		t.Errorf("attempt 0: expected 100ms, got %s", delays[0])
	}
	if delays[1] != 100*time.Millisecond {
		t.Errorf("attempt 1: expected 100ms, got %s", delays[1])
	}
	if delays[2] != 200*time.Millisecond {
		t.Errorf("attempt 2: expected 200ms, got %s", delays[2])
	}
	if delays[3] != 400*time.Millisecond {
		t.Errorf("attempt 3: expected 400ms, got %s", delays[3])
	}
}

func TestNextDelay_MaxDelayCap(t *testing.T) {
	p := &Policy{
		BaseDelay:   1 * time.Second,
		MaxDelay:    5 * time.Second,
		Multiplier:  10.0,
		JitterRatio: 0,
	}

	delay := p.NextDelay(5)
	if delay > 5*time.Second {
		t.Errorf("delay %s exceeds max_delay 5s", delay)
	}
}

func TestNextDelay_WithJitter(t *testing.T) {
	p := &Policy{
		BaseDelay:   1 * time.Second,
		MaxDelay:    60 * time.Second,
		Multiplier:  2.0,
		JitterRatio: 0.1,
	}

	// Run multiple times to verify jitter produces varying results.
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := p.NextDelay(2)
		seen[d] = true
	}

	if len(seen) < 2 {
		t.Error("expected jitter to produce varying delays")
	}
}

func TestShouldRetry(t *testing.T) {
	p := &Policy{MaxRetries: 3}

	tests := []struct {
		attempt int
		want    bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
	}

	for _, tt := range tests {
		got := p.ShouldRetry(tt.attempt)
		if got != tt.want {
			t.Errorf("ShouldRetry(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}
