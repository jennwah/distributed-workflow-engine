// Package client provides a Go SDK for interacting with the workflow engine API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Client communicates with the workflow engine API server.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// New creates a new workflow engine client.
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// EnqueueJobRequest is the request body for creating a job.
type EnqueueJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     int             `json:"max_retries"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// JobResponse is the API response for a job.
type JobResponse struct {
	ID             uuid.UUID       `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxRetries     int             `json:"max_retries"`
	CreatedAt      time.Time       `json:"created_at"`
}

// WorkflowStep defines a step in a workflow creation request.
type WorkflowStep struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int             `json:"max_retries"`
}

// StartWorkflowRequest is the request body for starting a workflow.
type StartWorkflowRequest struct {
	Name  string         `json:"name"`
	Steps []WorkflowStep `json:"steps"`
}

// WorkflowResponse is the API response for a workflow.
type WorkflowResponse struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	CurrentStep int       `json:"current_step"`
	TotalSteps  int       `json:"total_steps"`
	CreatedAt   time.Time `json:"created_at"`
}

// EnqueueJob submits a new job to the workflow engine.
func (c *Client) EnqueueJob(ctx context.Context, req *EnqueueJobRequest) (*JobResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var jobResp JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &jobResp, nil
}

// GetJob retrieves a job by its ID.
func (c *Client) GetJob(ctx context.Context, id uuid.UUID) (*JobResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/jobs/%s", c.baseURL, id), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var jobResp JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &jobResp, nil
}

// StartWorkflow starts a new multi-step workflow.
func (c *Client) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*WorkflowResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/workflows", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var wfResp WorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&wfResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &wfResp, nil
}

// GetWorkflow retrieves a workflow by its ID.
func (c *Client) GetWorkflow(ctx context.Context, id uuid.UUID) (*WorkflowResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v1/workflows/%s", c.baseURL, id), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	var wfResp WorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&wfResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &wfResp, nil
}
