// Package scheduler implements the worker job execution loop with heartbeats
// and stale job recovery.
package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
	"github.com/leejennwah/workflow-engine/internal/metrics"
	"github.com/leejennwah/workflow-engine/internal/queue"
	"github.com/leejennwah/workflow-engine/internal/retry"
	"github.com/leejennwah/workflow-engine/internal/workflow"
)

var tracer = otel.Tracer("workflow-engine/scheduler")

// TaskHandler is a function that executes job logic. It receives the job
// and returns an error if the execution fails.
type TaskHandler func(ctx context.Context, j *job.Job) error

// Scheduler pulls jobs from the queue and dispatches them to handlers.
type Scheduler struct {
	workerID       string
	jobRepo        job.Repository
	queue          queue.Queue
	workflowEngine *workflow.Engine
	retryPolicy    *retry.Policy
	metrics        *metrics.Metrics
	handlers       map[string]TaskHandler
	logger         *zap.Logger
	staleTimeout   int // seconds
}

// Config holds scheduler configuration.
type Config struct {
	StaleTimeoutSec  int
	HeartbeatSec     int
	MetricIntervalMs int
}

// DefaultConfig returns sensible scheduler defaults.
func DefaultConfig() Config {
	return Config{
		StaleTimeoutSec:  120,
		HeartbeatSec:     30,
		MetricIntervalMs: 5000,
	}
}

// New creates a new scheduler instance.
func New(
	jobRepo job.Repository,
	q queue.Queue,
	we *workflow.Engine,
	rp *retry.Policy,
	m *metrics.Metrics,
	logger *zap.Logger,
	cfg Config,
) *Scheduler {
	return &Scheduler{
		workerID:       fmt.Sprintf("worker-%s", uuid.New().String()[:8]),
		jobRepo:        jobRepo,
		queue:          q,
		workflowEngine: we,
		retryPolicy:    rp,
		metrics:        m,
		handlers:       make(map[string]TaskHandler),
		logger:         logger,
		staleTimeout:   cfg.StaleTimeoutSec,
	}
}

// RegisterHandler registers a task handler for a given job type.
func (s *Scheduler) RegisterHandler(jobType string, handler TaskHandler) {
	s.handlers[jobType] = handler
}

// Run starts the scheduler loop. It blocks until the context is cancelled.
func (s *Scheduler) Run(ctx context.Context, cfg Config) error {
	s.logger.Info("scheduler started", zap.String("worker_id", s.workerID))

	// Start background goroutines for heartbeat and metrics.
	go s.recoverStaleJobs(ctx, time.Duration(cfg.HeartbeatSec)*time.Second)
	go s.updateMetrics(ctx, time.Duration(cfg.MetricIntervalMs)*time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler shutting down", zap.String("worker_id", s.workerID))
			return nil
		default:
			if err := s.processNext(ctx); err != nil {
				s.logger.Error("process error", zap.Error(err))
			}
		}
	}
}

// processNext dequeues and executes a single job.
func (s *Scheduler) processNext(ctx context.Context) error {
	j, err := s.queue.Dequeue(ctx)
	if err != nil {
		return fmt.Errorf("dequeue: %w", err)
	}
	if j == nil {
		return nil // Timeout, no job available.
	}

	ctx, span := tracer.Start(ctx, "job.execute",
		trace.WithAttributes(
			attribute.String("job.id", j.ID.String()),
			attribute.String("job.type", j.Type),
			attribute.Int("job.attempt", j.Attempt),
		),
	)
	defer span.End()

	s.metrics.WorkerBusy.WithLabelValues(s.workerID).Set(1)
	defer s.metrics.WorkerBusy.WithLabelValues(s.workerID).Set(0)

	// Transition to running.
	if err := j.MarkRunning(s.workerID); err != nil {
		return fmt.Errorf("mark running: %w", err)
	}
	if err := s.jobRepo.Update(ctx, j); err != nil {
		return fmt.Errorf("update job running: %w", err)
	}

	// Execute the task handler.
	handler, ok := s.handlers[j.Type]
	if !ok {
		s.logger.Error("no handler registered", zap.String("type", j.Type))
		return s.failJob(ctx, j, fmt.Sprintf("no handler for type: %s", j.Type))
	}

	if err := handler(ctx, j); err != nil {
		return s.handleFailure(ctx, j, err)
	}

	return s.completeJob(ctx, j)
}

// completeJob marks a job as completed and advances the workflow if applicable.
func (s *Scheduler) completeJob(ctx context.Context, j *job.Job) error {
	if err := j.MarkCompleted(); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := s.jobRepo.Update(ctx, j); err != nil {
		return fmt.Errorf("update job completed: %w", err)
	}

	s.metrics.JobsSuccessTotal.Inc()
	if j.StartedAt != nil {
		s.metrics.JobLatency.Observe(time.Since(*j.StartedAt).Seconds())
	}

	// Advance workflow if this job is part of one.
	if j.WorkflowID != nil {
		if err := s.workflowEngine.OnStepCompleted(ctx, *j.WorkflowID, j.StepIndex); err != nil {
			s.logger.Error("advance workflow failed",
				zap.String("workflow_id", j.WorkflowID.String()),
				zap.Error(err),
			)
		}
	}

	s.logger.Info("job completed",
		zap.String("job_id", j.ID.String()),
		zap.String("type", j.Type),
	)
	return nil
}

// handleFailure decides whether to retry or dead-letter a failed job.
func (s *Scheduler) handleFailure(ctx context.Context, j *job.Job, execErr error) error {
	errMsg := execErr.Error()

	if err := j.MarkFailed(errMsg); err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	if err := s.jobRepo.Update(ctx, j); err != nil {
		return fmt.Errorf("update job failed: %w", err)
	}

	if j.CanRetry() {
		delay := s.retryPolicy.NextDelay(j.Attempt)
		s.logger.Info("retrying job",
			zap.String("job_id", j.ID.String()),
			zap.Int("attempt", j.Attempt),
			zap.Duration("delay", delay),
		)

		if err := j.TransitionTo(job.StatusRetrying); err != nil {
			return fmt.Errorf("transition to retrying: %w", err)
		}
		if err := s.jobRepo.Update(ctx, j); err != nil {
			return fmt.Errorf("update job retrying: %w", err)
		}

		time.Sleep(delay)

		if err := j.TransitionTo(job.StatusQueued); err != nil {
			return fmt.Errorf("transition to queued: %w", err)
		}
		if err := s.jobRepo.Update(ctx, j); err != nil {
			return fmt.Errorf("update job requeued: %w", err)
		}
		return s.queue.Enqueue(ctx, j)
	}

	return s.failJob(ctx, j, errMsg)
}

// failJob permanently fails a job and sends it to the dead letter queue.
func (s *Scheduler) failJob(ctx context.Context, j *job.Job, errMsg string) error {
	// The job may already be in failed status from handleFailure.
	if j.Status == job.StatusFailed {
		if err := j.SendToDeadLetter(); err != nil {
			return fmt.Errorf("send to dead letter: %w", err)
		}
	}

	if err := s.jobRepo.Update(ctx, j); err != nil {
		return fmt.Errorf("update job dead letter: %w", err)
	}
	if err := s.queue.EnqueueDeadLetter(ctx, j); err != nil {
		return fmt.Errorf("enqueue dead letter: %w", err)
	}

	s.metrics.JobsFailedTotal.Inc()

	// Fail the workflow if applicable.
	if j.WorkflowID != nil {
		if err := s.workflowEngine.OnStepFailed(ctx, *j.WorkflowID); err != nil {
			s.logger.Error("fail workflow step error", zap.Error(err))
		}
	}

	s.logger.Error("job permanently failed",
		zap.String("job_id", j.ID.String()),
		zap.String("error", errMsg),
	)
	return nil
}

// recoverStaleJobs periodically reclaims jobs stuck in running state.
func (s *Scheduler) recoverStaleJobs(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := s.jobRepo.AcquireStaleJobs(ctx, s.staleTimeout)
			if err != nil {
				s.logger.Error("recover stale jobs failed", zap.Error(err))
				continue
			}
			if count > 0 {
				s.logger.Info("recovered stale jobs", zap.Int64("count", count))
			}
		}
	}
}

// updateMetrics periodically updates gauge metrics from the queue.
func (s *Scheduler) updateMetrics(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := s.queue.Depth(ctx)
			if err == nil {
				s.metrics.QueueDepth.Set(float64(depth))
			}
			dlDepth, err := s.queue.DeadLetterDepth(ctx)
			if err == nil {
				s.metrics.DeadLetterDepth.Set(float64(dlDepth))
			}
		}
	}
}
