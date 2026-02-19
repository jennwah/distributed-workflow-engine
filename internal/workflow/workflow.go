// Package workflow defines multi-step workflow orchestration.
package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a workflow.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Step defines a single step within a workflow.
type Step struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int             `json:"max_retries"`
}

// Workflow represents a multi-step, sequentially-executed pipeline.
type Workflow struct {
	ID          uuid.UUID  `json:"id" db:"id"`
	Name        string     `json:"name" db:"name"`
	Status      Status     `json:"status" db:"status"`
	CurrentStep int        `json:"current_step" db:"current_step"`
	Steps       []Step     `json:"steps" db:"steps"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty" db:"completed_at"`
}

// NewWorkflow creates a new workflow with the given name and steps.
func NewWorkflow(name string, steps []Step) (*Workflow, error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("workflow must have at least one step")
	}

	now := time.Now().UTC()
	return &Workflow{
		ID:          uuid.New(),
		Name:        name,
		Status:      StatusPending,
		CurrentStep: 0,
		Steps:       steps,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

// Advance moves the workflow to the next step. If all steps are done,
// the workflow transitions to completed.
func (w *Workflow) Advance() (done bool) {
	w.CurrentStep++
	w.UpdatedAt = time.Now().UTC()

	if w.CurrentStep >= len(w.Steps) {
		w.Status = StatusCompleted
		now := time.Now().UTC()
		w.CompletedAt = &now
		return true
	}
	return false
}

// MarkRunning transitions the workflow to running.
func (w *Workflow) MarkRunning() error {
	if w.Status != StatusPending {
		return fmt.Errorf("cannot start workflow in status %s", w.Status)
	}
	w.Status = StatusRunning
	w.UpdatedAt = time.Now().UTC()
	return nil
}

// MarkFailed transitions the workflow to failed.
func (w *Workflow) MarkFailed() {
	w.Status = StatusFailed
	w.UpdatedAt = time.Now().UTC()
}

// CurrentStepDef returns the definition of the current step.
func (w *Workflow) CurrentStepDef() (*Step, error) {
	if w.CurrentStep >= len(w.Steps) {
		return nil, fmt.Errorf("no more steps to execute")
	}
	return &w.Steps[w.CurrentStep], nil
}
