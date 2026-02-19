// Package job defines the core job domain model and state machine.
package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a job in its lifecycle.
type Status string

const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusRunning    Status = "running"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusRetrying   Status = "retrying"
	StatusDeadLetter Status = "dead_letter"
)

// validTransitions defines the allowed state machine transitions.
var validTransitions = map[Status][]Status{
	StatusPending:    {StatusQueued},
	StatusQueued:     {StatusRunning, StatusDeadLetter},
	StatusRunning:    {StatusCompleted, StatusFailed},
	StatusFailed:     {StatusRetrying, StatusDeadLetter},
	StatusRetrying:   {StatusQueued},
	StatusCompleted:  {},
	StatusDeadLetter: {},
}

// Job represents a unit of work to be executed by a worker.
type Job struct {
	ID             uuid.UUID       `json:"id" db:"id"`
	IdempotencyKey string          `json:"idempotency_key" db:"idempotency_key"`
	WorkflowID     *uuid.UUID      `json:"workflow_id,omitempty" db:"workflow_id"`
	StepIndex      int             `json:"step_index" db:"step_index"`
	Type           string          `json:"type" db:"type"`
	Payload        json.RawMessage `json:"payload" db:"payload"`
	Status         Status          `json:"status" db:"status"`
	Attempt        int             `json:"attempt" db:"attempt"`
	MaxRetries     int             `json:"max_retries" db:"max_retries"`
	LastError      string          `json:"last_error,omitempty" db:"last_error"`
	WorkerID       string          `json:"worker_id,omitempty" db:"worker_id"`
	CreatedAt      time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at" db:"updated_at"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty" db:"scheduled_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty" db:"started_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty" db:"completed_at"`
}

// NewJob creates a new job with the given type and payload.
func NewJob(jobType string, payload json.RawMessage, maxRetries int) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:             uuid.New(),
		IdempotencyKey: uuid.New().String(),
		Type:           jobType,
		Payload:        payload,
		Status:         StatusPending,
		Attempt:        0,
		MaxRetries:     maxRetries,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// TransitionTo validates and performs a state transition.
func (j *Job) TransitionTo(newStatus Status) error {
	allowed, ok := validTransitions[j.Status]
	if !ok {
		return fmt.Errorf("unknown current status: %s", j.Status)
	}

	for _, s := range allowed {
		if s == newStatus {
			j.Status = newStatus
			j.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	return fmt.Errorf("invalid transition from %s to %s", j.Status, newStatus)
}

// CanRetry reports whether the job has remaining retry attempts.
func (j *Job) CanRetry() bool {
	return j.Attempt < j.MaxRetries
}

// MarkRunning transitions the job to running and records the worker.
func (j *Job) MarkRunning(workerID string) error {
	if err := j.TransitionTo(StatusRunning); err != nil {
		return err
	}
	now := time.Now().UTC()
	j.WorkerID = workerID
	j.StartedAt = &now
	j.Attempt++
	return nil
}

// MarkCompleted transitions the job to completed.
func (j *Job) MarkCompleted() error {
	if err := j.TransitionTo(StatusCompleted); err != nil {
		return err
	}
	now := time.Now().UTC()
	j.CompletedAt = &now
	return nil
}

// MarkFailed transitions the job to failed with an error message.
func (j *Job) MarkFailed(errMsg string) error {
	if err := j.TransitionTo(StatusFailed); err != nil {
		return err
	}
	j.LastError = errMsg
	return nil
}

// SendToDeadLetter transitions the job to the dead letter queue.
func (j *Job) SendToDeadLetter() error {
	return j.TransitionTo(StatusDeadLetter)
}
