package workflow

import (
	"encoding/json"
	"testing"
)

func TestNewWorkflow(t *testing.T) {
	steps := []Step{
		{Name: "step-1", Type: "default", Payload: json.RawMessage(`{}`), MaxRetries: 3},
		{Name: "step-2", Type: "compute", Payload: json.RawMessage(`{}`), MaxRetries: 2},
	}

	w, err := NewWorkflow("test-workflow", steps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if w.Name != "test-workflow" {
		t.Errorf("expected name 'test-workflow', got '%s'", w.Name)
	}
	if w.Status != StatusPending {
		t.Errorf("expected status pending, got %s", w.Status)
	}
	if w.CurrentStep != 0 {
		t.Errorf("expected current_step 0, got %d", w.CurrentStep)
	}
	if len(w.Steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(w.Steps))
	}
}

func TestNewWorkflow_NoSteps(t *testing.T) {
	_, err := NewWorkflow("empty", nil)
	if err == nil {
		t.Error("expected error for empty steps")
	}
}

func TestWorkflow_Advance(t *testing.T) {
	steps := []Step{
		{Name: "step-1", Type: "default"},
		{Name: "step-2", Type: "default"},
		{Name: "step-3", Type: "default"},
	}

	w, _ := NewWorkflow("test", steps)
	w.Status = StatusRunning

	// Advance through each step.
	if done := w.Advance(); done {
		t.Error("expected not done after first advance")
	}
	if w.CurrentStep != 1 {
		t.Errorf("expected step 1, got %d", w.CurrentStep)
	}

	if done := w.Advance(); done {
		t.Error("expected not done after second advance")
	}
	if w.CurrentStep != 2 {
		t.Errorf("expected step 2, got %d", w.CurrentStep)
	}

	if done := w.Advance(); !done {
		t.Error("expected done after final advance")
	}
	if w.Status != StatusCompleted {
		t.Errorf("expected status completed, got %s", w.Status)
	}
	if w.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
}

func TestWorkflow_MarkRunning(t *testing.T) {
	w, _ := NewWorkflow("test", []Step{{Name: "s1", Type: "default"}})

	if err := w.MarkRunning(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Status != StatusRunning {
		t.Errorf("expected status running, got %s", w.Status)
	}

	// Cannot start a running workflow again.
	if err := w.MarkRunning(); err == nil {
		t.Error("expected error when starting an already running workflow")
	}
}

func TestWorkflow_MarkFailed(t *testing.T) {
	w, _ := NewWorkflow("test", []Step{{Name: "s1", Type: "default"}})
	w.Status = StatusRunning

	w.MarkFailed()
	if w.Status != StatusFailed {
		t.Errorf("expected status failed, got %s", w.Status)
	}
}

func TestWorkflow_CurrentStepDef(t *testing.T) {
	steps := []Step{
		{Name: "extract", Type: "transform"},
		{Name: "load", Type: "compute"},
	}
	w, _ := NewWorkflow("test", steps)

	step, err := w.CurrentStepDef()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if step.Name != "extract" {
		t.Errorf("expected step 'extract', got '%s'", step.Name)
	}

	w.CurrentStep = 2
	_, err = w.CurrentStepDef()
	if err == nil {
		t.Error("expected error when no more steps")
	}
}
