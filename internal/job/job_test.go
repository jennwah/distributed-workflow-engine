package job

import (
	"encoding/json"
	"testing"
)

func TestNewJob(t *testing.T) {
	payload := json.RawMessage(`{"key":"value"}`)
	j := NewJob("test-type", payload, 3)

	if j.ID.String() == "" {
		t.Error("expected non-empty ID")
	}
	if j.Type != "test-type" {
		t.Errorf("expected type 'test-type', got '%s'", j.Type)
	}
	if j.Status != StatusPending {
		t.Errorf("expected status 'pending', got '%s'", j.Status)
	}
	if j.MaxRetries != 3 {
		t.Errorf("expected max_retries 3, got %d", j.MaxRetries)
	}
	if j.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", j.Attempt)
	}
}

func TestJobTransitions(t *testing.T) {
	tests := []struct {
		name     string
		from     Status
		to       Status
		wantErr  bool
	}{
		{"pending to queued", StatusPending, StatusQueued, false},
		{"queued to running", StatusQueued, StatusRunning, false},
		{"running to completed", StatusRunning, StatusCompleted, false},
		{"running to failed", StatusRunning, StatusFailed, false},
		{"failed to retrying", StatusFailed, StatusRetrying, false},
		{"failed to dead_letter", StatusFailed, StatusDeadLetter, false},
		{"retrying to queued", StatusRetrying, StatusQueued, false},

		// Invalid transitions.
		{"pending to completed", StatusPending, StatusCompleted, true},
		{"pending to running", StatusPending, StatusRunning, true},
		{"completed to running", StatusCompleted, StatusRunning, true},
		{"queued to completed", StatusQueued, StatusCompleted, true},
		{"dead_letter to running", StatusDeadLetter, StatusRunning, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{Status: tt.from}
			err := j.TransitionTo(tt.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("TransitionTo(%s -> %s) error = %v, wantErr %v", tt.from, tt.to, err, tt.wantErr)
			}
			if err == nil && j.Status != tt.to {
				t.Errorf("expected status %s, got %s", tt.to, j.Status)
			}
		})
	}
}

func TestMarkRunning(t *testing.T) {
	j := &Job{Status: StatusQueued, Attempt: 0}
	if err := j.MarkRunning("worker-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if j.Status != StatusRunning {
		t.Errorf("expected status running, got %s", j.Status)
	}
	if j.WorkerID != "worker-1" {
		t.Errorf("expected worker_id 'worker-1', got '%s'", j.WorkerID)
	}
	if j.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", j.Attempt)
	}
	if j.StartedAt == nil {
		t.Error("expected started_at to be set")
	}
}

func TestMarkCompleted(t *testing.T) {
	j := &Job{Status: StatusRunning}
	if err := j.MarkCompleted(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if j.Status != StatusCompleted {
		t.Errorf("expected status completed, got %s", j.Status)
	}
	if j.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
}

func TestCanRetry(t *testing.T) {
	j := &Job{Attempt: 2, MaxRetries: 3}
	if !j.CanRetry() {
		t.Error("expected CanRetry to return true when attempt < max_retries")
	}

	j.Attempt = 3
	if j.CanRetry() {
		t.Error("expected CanRetry to return false when attempt >= max_retries")
	}
}

func TestMarkFailed(t *testing.T) {
	j := &Job{Status: StatusRunning}
	if err := j.MarkFailed("something broke"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if j.Status != StatusFailed {
		t.Errorf("expected status failed, got %s", j.Status)
	}
	if j.LastError != "something broke" {
		t.Errorf("expected error message 'something broke', got '%s'", j.LastError)
	}
}

func TestSendToDeadLetter(t *testing.T) {
	j := &Job{Status: StatusFailed}
	if err := j.SendToDeadLetter(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if j.Status != StatusDeadLetter {
		t.Errorf("expected status dead_letter, got %s", j.Status)
	}
}
