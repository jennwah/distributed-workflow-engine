// Package queue defines the job queue interface and Redis implementation.
package queue

import (
	"context"

	"github.com/leejennwah/workflow-engine/internal/job"
)

// Queue defines the interface for job queue operations.
type Queue interface {
	// Enqueue pushes a job onto the queue for processing.
	Enqueue(ctx context.Context, j *job.Job) error

	// Dequeue blocks until a job is available, then returns it.
	// Returns nil if the context is cancelled.
	Dequeue(ctx context.Context) (*job.Job, error)

	// EnqueueDeadLetter moves a job to the dead letter queue.
	EnqueueDeadLetter(ctx context.Context, j *job.Job) error

	// Depth returns the current number of jobs in the queue.
	Depth(ctx context.Context) (int64, error)

	// DeadLetterDepth returns the number of jobs in the dead letter queue.
	DeadLetterDepth(ctx context.Context) (int64, error)
}
