package workflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
	"github.com/leejennwah/workflow-engine/internal/metrics"
	"github.com/leejennwah/workflow-engine/internal/queue"
)

var tracer = otel.Tracer("workflow-engine/workflow")

// Engine orchestrates multi-step workflow execution by creating jobs
// for each step and advancing the workflow upon step completion.
type Engine struct {
	workflowRepo Repository
	jobRepo      job.Repository
	queue        queue.Queue
	metrics      *metrics.Metrics
	logger       *zap.Logger
}

// NewEngine creates a new workflow engine.
func NewEngine(wr Repository, jr job.Repository, q queue.Queue, m *metrics.Metrics, logger *zap.Logger) *Engine {
	return &Engine{
		workflowRepo: wr,
		jobRepo:      jr,
		queue:        q,
		metrics:      m,
		logger:       logger,
	}
}

// StartWorkflow initializes and begins executing a workflow. It creates
// the workflow record, transitions it to running, and enqueues the first step.
func (e *Engine) StartWorkflow(ctx context.Context, name string, steps []Step) (*Workflow, error) {
	ctx, span := tracer.Start(ctx, "workflow.start",
		trace.WithAttributes(attribute.String("workflow.name", name)),
	)
	defer span.End()

	w, err := NewWorkflow(name, steps)
	if err != nil {
		return nil, fmt.Errorf("create workflow: %w", err)
	}

	if err := e.workflowRepo.Create(ctx, w); err != nil {
		return nil, fmt.Errorf("persist workflow: %w", err)
	}

	if err := w.MarkRunning(); err != nil {
		return nil, fmt.Errorf("transition workflow: %w", err)
	}

	if err := e.workflowRepo.Update(ctx, w); err != nil {
		return nil, fmt.Errorf("update workflow: %w", err)
	}

	if err := e.enqueueCurrentStep(ctx, w); err != nil {
		return nil, fmt.Errorf("enqueue first step: %w", err)
	}

	e.logger.Info("workflow started",
		zap.String("workflow_id", w.ID.String()),
		zap.String("name", name),
		zap.Int("total_steps", len(steps)),
	)

	return w, nil
}

// OnStepCompleted is called when a job belonging to a workflow finishes
// successfully. It advances the workflow and enqueues the next step.
func (e *Engine) OnStepCompleted(ctx context.Context, workflowID uuid.UUID, stepIndex int) error {
	ctx, span := tracer.Start(ctx, "workflow.step_completed",
		trace.WithAttributes(
			attribute.String("workflow.id", workflowID.String()),
			attribute.Int("workflow.step_index", stepIndex),
		),
	)
	defer span.End()

	w, err := e.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}

	if w.Status != StatusRunning {
		return fmt.Errorf("workflow %s is not running (status: %s)", w.ID, w.Status)
	}

	if stepIndex != w.CurrentStep {
		e.logger.Warn("step index mismatch, ignoring",
			zap.Int("expected", w.CurrentStep),
			zap.Int("received", stepIndex),
		)
		return nil
	}

	done := w.Advance()
	if err := e.workflowRepo.Update(ctx, w); err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}

	if done {
		e.logger.Info("workflow completed",
			zap.String("workflow_id", w.ID.String()),
		)
		return nil
	}

	return e.enqueueCurrentStep(ctx, w)
}

// OnStepFailed is called when a workflow step permanently fails (exhausted retries).
func (e *Engine) OnStepFailed(ctx context.Context, workflowID uuid.UUID) error {
	w, err := e.workflowRepo.GetByID(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}

	w.MarkFailed()
	if err := e.workflowRepo.Update(ctx, w); err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}

	e.logger.Error("workflow failed",
		zap.String("workflow_id", w.ID.String()),
		zap.Int("failed_step", w.CurrentStep),
	)
	return nil
}

// enqueueCurrentStep creates a job for the current workflow step and pushes it to the queue.
func (e *Engine) enqueueCurrentStep(ctx context.Context, w *Workflow) error {
	step, err := w.CurrentStepDef()
	if err != nil {
		return err
	}

	payload, err := json.Marshal(step.Payload)
	if err != nil {
		return fmt.Errorf("marshal step payload: %w", err)
	}

	j := job.NewJob(step.Type, payload, step.MaxRetries)
	j.WorkflowID = &w.ID
	j.StepIndex = w.CurrentStep

	// Transition to queued in memory before persisting — single INSERT.
	if err := j.TransitionTo(job.StatusQueued); err != nil {
		return fmt.Errorf("transition job to queued: %w", err)
	}

	if err := e.jobRepo.Create(ctx, j); err != nil {
		return fmt.Errorf("persist job: %w", err)
	}

	if err := e.queue.Enqueue(ctx, j); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	e.metrics.JobsTotal.WithLabelValues(step.Type).Inc()

	e.logger.Info("enqueued workflow step",
		zap.String("workflow_id", w.ID.String()),
		zap.Int("step", w.CurrentStep),
		zap.String("step_name", step.Name),
	)

	return nil
}
