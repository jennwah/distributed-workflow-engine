package workflow

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines the persistence interface for workflows.
type Repository interface {
	// Create persists a new workflow.
	Create(ctx context.Context, w *Workflow) error

	// GetByID retrieves a workflow by its unique identifier.
	GetByID(ctx context.Context, id uuid.UUID) (*Workflow, error)

	// Update persists changes to an existing workflow.
	Update(ctx context.Context, w *Workflow) error

	// ListByStatus returns workflows matching the given status.
	ListByStatus(ctx context.Context, status Status, limit, offset int) ([]*Workflow, error)
}
