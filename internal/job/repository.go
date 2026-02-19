package job

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines the persistence interface for jobs.
type Repository interface {
	// Create persists a new job. If a job with the same idempotency key
	// already exists, it returns the existing job without modification.
	Create(ctx context.Context, j *Job) error

	// GetByID retrieves a job by its unique identifier.
	GetByID(ctx context.Context, id uuid.UUID) (*Job, error)

	// GetByIdempotencyKey retrieves a job by its idempotency key.
	GetByIdempotencyKey(ctx context.Context, key string) (*Job, error)

	// Update persists changes to an existing job.
	Update(ctx context.Context, j *Job) error

	// ListByStatus returns jobs matching the given status, with pagination.
	ListByStatus(ctx context.Context, status Status, limit, offset int) ([]*Job, error)

	// ListByWorkflowID returns all jobs belonging to a workflow.
	ListByWorkflowID(ctx context.Context, workflowID uuid.UUID) ([]*Job, error)

	// CountByStatus returns the count of jobs in each status.
	CountByStatus(ctx context.Context) (map[Status]int64, error)

	// AcquireStaleJobs reclaims jobs that have been running for longer than
	// the given timeout, resetting them for re-processing. Returns the
	// number of jobs reclaimed.
	AcquireStaleJobs(ctx context.Context, timeoutSeconds int) (int64, error)
}
