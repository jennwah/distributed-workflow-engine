// Package storage provides Postgres-backed implementations of domain repositories.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
	"github.com/leejennwah/workflow-engine/internal/workflow"
)

// PostgresJobRepository implements job.Repository using PostgreSQL.
type PostgresJobRepository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgresJobRepository creates a new Postgres-backed job repository.
func NewPostgresJobRepository(pool *pgxpool.Pool, logger *zap.Logger) *PostgresJobRepository {
	return &PostgresJobRepository{pool: pool, logger: logger}
}

// Create inserts a new job. If a job with the same idempotency key exists,
// the insert is skipped (ON CONFLICT DO NOTHING).
func (r *PostgresJobRepository) Create(ctx context.Context, j *job.Job) error {
	query := `
		INSERT INTO jobs (id, idempotency_key, workflow_id, step_index, type, payload, status,
			attempt, max_retries, last_error, worker_id, created_at, updated_at, scheduled_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (idempotency_key) DO NOTHING`

	_, err := r.pool.Exec(ctx, query,
		j.ID, j.IdempotencyKey, j.WorkflowID, j.StepIndex, j.Type, j.Payload, j.Status,
		j.Attempt, j.MaxRetries, j.LastError, j.WorkerID, j.CreatedAt, j.UpdatedAt,
		j.ScheduledAt, j.StartedAt, j.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// GetByID retrieves a job by its UUID.
func (r *PostgresJobRepository) GetByID(ctx context.Context, id uuid.UUID) (*job.Job, error) {
	query := `
		SELECT id, idempotency_key, workflow_id, step_index, type, payload, status,
			attempt, max_retries, last_error, worker_id, created_at, updated_at,
			scheduled_at, started_at, completed_at
		FROM jobs WHERE id = $1`

	j := &job.Job{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&j.ID, &j.IdempotencyKey, &j.WorkflowID, &j.StepIndex, &j.Type, &j.Payload, &j.Status,
		&j.Attempt, &j.MaxRetries, &j.LastError, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt,
		&j.ScheduledAt, &j.StartedAt, &j.CompletedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", id)
		}
		return nil, fmt.Errorf("query job: %w", err)
	}
	return j, nil
}

// GetByIdempotencyKey retrieves a job by its idempotency key.
func (r *PostgresJobRepository) GetByIdempotencyKey(ctx context.Context, key string) (*job.Job, error) {
	query := `
		SELECT id, idempotency_key, workflow_id, step_index, type, payload, status,
			attempt, max_retries, last_error, worker_id, created_at, updated_at,
			scheduled_at, started_at, completed_at
		FROM jobs WHERE idempotency_key = $1`

	j := &job.Job{}
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&j.ID, &j.IdempotencyKey, &j.WorkflowID, &j.StepIndex, &j.Type, &j.Payload, &j.Status,
		&j.Attempt, &j.MaxRetries, &j.LastError, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt,
		&j.ScheduledAt, &j.StartedAt, &j.CompletedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("job not found by key: %s", key)
		}
		return nil, fmt.Errorf("query job: %w", err)
	}
	return j, nil
}

// Update persists changes to an existing job.
func (r *PostgresJobRepository) Update(ctx context.Context, j *job.Job) error {
	query := `
		UPDATE jobs SET
			status = $2, attempt = $3, last_error = $4, worker_id = $5,
			updated_at = $6, scheduled_at = $7, started_at = $8, completed_at = $9
		WHERE id = $1`

	_, err := r.pool.Exec(ctx, query,
		j.ID, j.Status, j.Attempt, j.LastError, j.WorkerID,
		j.UpdatedAt, j.ScheduledAt, j.StartedAt, j.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	return nil
}

// ListByStatus returns jobs in the given status with pagination.
func (r *PostgresJobRepository) ListByStatus(ctx context.Context, status job.Status, limit, offset int) ([]*job.Job, error) {
	query := `
		SELECT id, idempotency_key, workflow_id, step_index, type, payload, status,
			attempt, max_retries, last_error, worker_id, created_at, updated_at,
			scheduled_at, started_at, completed_at
		FROM jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`

	rows, err := r.pool.Query(ctx, query, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	return scanJobs(rows)
}

// ListByWorkflowID returns all jobs belonging to a workflow.
func (r *PostgresJobRepository) ListByWorkflowID(ctx context.Context, workflowID uuid.UUID) ([]*job.Job, error) {
	query := `
		SELECT id, idempotency_key, workflow_id, step_index, type, payload, status,
			attempt, max_retries, last_error, worker_id, created_at, updated_at,
			scheduled_at, started_at, completed_at
		FROM jobs WHERE workflow_id = $1 ORDER BY step_index`

	rows, err := r.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	return scanJobs(rows)
}

// CountByStatus returns the count of jobs grouped by status.
func (r *PostgresJobRepository) CountByStatus(ctx context.Context) (map[job.Status]int64, error) {
	query := `SELECT status, COUNT(*) FROM jobs GROUP BY status`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[job.Status]int64)
	for rows.Next() {
		var status job.Status
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan count: %w", err)
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

// AcquireStaleJobs reclaims jobs stuck in "running" state beyond the given timeout.
func (r *PostgresJobRepository) AcquireStaleJobs(ctx context.Context, timeoutSeconds int) (int64, error) {
	query := `
		UPDATE jobs SET status = 'queued', worker_id = '', updated_at = $1
		WHERE status = 'running' AND updated_at < NOW() - INTERVAL '1 second' * $2`

	tag, err := r.pool.Exec(ctx, query, time.Now().UTC(), timeoutSeconds)
	if err != nil {
		return 0, fmt.Errorf("reclaim stale jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func scanJobs(rows pgx.Rows) ([]*job.Job, error) {
	var jobs []*job.Job
	for rows.Next() {
		j := &job.Job{}
		if err := rows.Scan(
			&j.ID, &j.IdempotencyKey, &j.WorkflowID, &j.StepIndex, &j.Type, &j.Payload, &j.Status,
			&j.Attempt, &j.MaxRetries, &j.LastError, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt,
			&j.ScheduledAt, &j.StartedAt, &j.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// PostgresWorkflowRepository implements workflow.Repository using PostgreSQL.
type PostgresWorkflowRepository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgresWorkflowRepository creates a new Postgres-backed workflow repository.
func NewPostgresWorkflowRepository(pool *pgxpool.Pool, logger *zap.Logger) *PostgresWorkflowRepository {
	return &PostgresWorkflowRepository{pool: pool, logger: logger}
}

// Create inserts a new workflow.
func (r *PostgresWorkflowRepository) Create(ctx context.Context, w *workflow.Workflow) error {
	stepsJSON, err := json.Marshal(w.Steps)
	if err != nil {
		return fmt.Errorf("marshal steps: %w", err)
	}

	query := `
		INSERT INTO workflows (id, name, status, current_step, steps, created_at, updated_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err = r.pool.Exec(ctx, query,
		w.ID, w.Name, w.Status, w.CurrentStep, stepsJSON, w.CreatedAt, w.UpdatedAt, w.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert workflow: %w", err)
	}
	return nil
}

// GetByID retrieves a workflow by its UUID.
func (r *PostgresWorkflowRepository) GetByID(ctx context.Context, id uuid.UUID) (*workflow.Workflow, error) {
	query := `
		SELECT id, name, status, current_step, steps, created_at, updated_at, completed_at
		FROM workflows WHERE id = $1`

	w := &workflow.Workflow{}
	var stepsJSON []byte
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&w.ID, &w.Name, &w.Status, &w.CurrentStep, &stepsJSON, &w.CreatedAt, &w.UpdatedAt, &w.CompletedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("workflow not found: %s", id)
		}
		return nil, fmt.Errorf("query workflow: %w", err)
	}

	if err := json.Unmarshal(stepsJSON, &w.Steps); err != nil {
		return nil, fmt.Errorf("unmarshal steps: %w", err)
	}
	return w, nil
}

// Update persists changes to an existing workflow.
func (r *PostgresWorkflowRepository) Update(ctx context.Context, w *workflow.Workflow) error {
	stepsJSON, err := json.Marshal(w.Steps)
	if err != nil {
		return fmt.Errorf("marshal steps: %w", err)
	}

	query := `
		UPDATE workflows SET
			status = $2, current_step = $3, steps = $4, updated_at = $5, completed_at = $6
		WHERE id = $1`

	_, err = r.pool.Exec(ctx, query,
		w.ID, w.Status, w.CurrentStep, stepsJSON, w.UpdatedAt, w.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	return nil
}

// ListByStatus returns workflows matching the given status.
func (r *PostgresWorkflowRepository) ListByStatus(ctx context.Context, status workflow.Status, limit, offset int) ([]*workflow.Workflow, error) {
	query := `
		SELECT id, name, status, current_step, steps, created_at, updated_at, completed_at
		FROM workflows WHERE status = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`

	rows, err := r.pool.Query(ctx, query, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query workflows: %w", err)
	}
	defer rows.Close()

	var workflows []*workflow.Workflow
	for rows.Next() {
		w := &workflow.Workflow{}
		var stepsJSON []byte
		if err := rows.Scan(
			&w.ID, &w.Name, &w.Status, &w.CurrentStep, &stepsJSON, &w.CreatedAt, &w.UpdatedAt, &w.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		if err := json.Unmarshal(stepsJSON, &w.Steps); err != nil {
			return nil, fmt.Errorf("unmarshal steps: %w", err)
		}
		workflows = append(workflows, w)
	}
	return workflows, rows.Err()
}
