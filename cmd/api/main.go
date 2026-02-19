// Command api starts the HTTP API server for the workflow engine.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
	"github.com/leejennwah/workflow-engine/internal/metrics"
	"github.com/leejennwah/workflow-engine/internal/queue"
	"github.com/leejennwah/workflow-engine/internal/storage"
	"github.com/leejennwah/workflow-engine/internal/tracing"
	"github.com/leejennwah/workflow-engine/internal/workflow"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize tracing.
	otlpEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	shutdownTracer, err := tracing.Init(ctx, "workflow-api", otlpEndpoint)
	if err != nil {
		logger.Warn("tracing init failed, continuing without tracing", zap.Error(err))
	} else {
		defer shutdownTracer(ctx)
	}

	// Connect to Postgres with tuned connection pool for high-throughput ingestion.
	dsn := getEnv("DATABASE_URL", "postgres://workflow:workflow@localhost:5432/workflow?sslmode=disable")
	pgConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Fatal("parse postgres config", zap.Error(err))
	}
	pgConfig.MaxConns = 50
	pgConfig.MinConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		logger.Fatal("connect to postgres", zap.Error(err))
	}
	defer pool.Close()

	// Connect to Redis with connection pool sized for concurrency.
	redisAddr := getEnv("REDIS_URL", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     100,
		MinIdleConns: 20,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("connect to redis", zap.Error(err))
	}
	defer rdb.Close()

	// Initialize repositories and services.
	jobRepo := storage.NewPostgresJobRepository(pool, logger)
	workflowRepo := storage.NewPostgresWorkflowRepository(pool, logger)
	q := queue.NewRedisQueue(rdb, logger)
	m := metrics.New()
	wfEngine := workflow.NewEngine(workflowRepo, jobRepo, q, m, logger)

	handler := &apiHandler{
		jobRepo:        jobRepo,
		workflowRepo:   workflowRepo,
		queue:          q,
		workflowEngine: wfEngine,
		metrics:        m,
		logger:         logger,
	}

	// Set up routes.
	r := mux.NewRouter()
	r.HandleFunc("/health", handler.health).Methods("GET")
	r.HandleFunc("/api/v1/jobs", handler.enqueueJob).Methods("POST")
	r.HandleFunc("/api/v1/jobs/{id}", handler.getJob).Methods("GET")
	r.HandleFunc("/api/v1/jobs", handler.listJobs).Methods("GET")
	r.HandleFunc("/api/v1/workflows", handler.startWorkflow).Methods("POST")
	r.HandleFunc("/api/v1/workflows/{id}", handler.getWorkflow).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())

	addr := getEnv("API_ADDR", ":8080")
	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("api server starting", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server failed", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down api server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("server shutdown failed", zap.Error(err))
	}
}

type apiHandler struct {
	jobRepo        job.Repository
	workflowRepo   workflow.Repository
	queue          queue.Queue
	workflowEngine *workflow.Engine
	metrics        *metrics.Metrics
	logger         *zap.Logger
}

func (h *apiHandler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type enqueueRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     int             `json:"max_retries"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

func (h *apiHandler) enqueueJob(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, `{"error":"type is required"}`, http.StatusBadRequest)
		return
	}

	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}

	j := job.NewJob(req.Type, req.Payload, req.MaxRetries)
	if req.IdempotencyKey != "" {
		j.IdempotencyKey = req.IdempotencyKey
	}

	// Transition to queued in memory before persisting — single INSERT instead
	// of INSERT + UPDATE, cutting Postgres round trips in half under load.
	if err := j.TransitionTo(job.StatusQueued); err != nil {
		h.logger.Error("transition to queued failed", zap.Error(err))
		http.Error(w, `{"error":"failed to queue job"}`, http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	if err := h.jobRepo.Create(ctx, j); err != nil {
		h.logger.Error("create job failed", zap.Error(err))
		http.Error(w, `{"error":"failed to create job"}`, http.StatusInternalServerError)
		return
	}

	if err := h.queue.Enqueue(ctx, j); err != nil {
		h.logger.Error("enqueue failed", zap.Error(err))
		http.Error(w, `{"error":"failed to enqueue job"}`, http.StatusInternalServerError)
		return
	}

	h.metrics.JobsTotal.WithLabelValues(j.Type).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(j)
}

func (h *apiHandler) getJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := uuid.Parse(vars["id"])
	if err != nil {
		http.Error(w, `{"error":"invalid job id"}`, http.StatusBadRequest)
		return
	}

	j, err := h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (h *apiHandler) listJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "pending"
	}

	jobs, err := h.jobRepo.ListByStatus(r.Context(), job.Status(status), 100, 0)
	if err != nil {
		h.logger.Error("list jobs failed", zap.Error(err))
		http.Error(w, `{"error":"failed to list jobs"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

type startWorkflowRequest struct {
	Name  string          `json:"name"`
	Steps []workflow.Step `json:"steps"`
}

func (h *apiHandler) startWorkflow(w http.ResponseWriter, r *http.Request) {
	var req startWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusBadRequest)
		return
	}

	if req.Name == "" || len(req.Steps) == 0 {
		http.Error(w, `{"error":"name and steps are required"}`, http.StatusBadRequest)
		return
	}

	wf, err := h.workflowEngine.StartWorkflow(r.Context(), req.Name, req.Steps)
	if err != nil {
		h.logger.Error("start workflow failed", zap.Error(err))
		http.Error(w, `{"error":"failed to start workflow"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":           wf.ID,
		"name":         wf.Name,
		"status":       wf.Status,
		"current_step": wf.CurrentStep,
		"total_steps":  len(wf.Steps),
		"created_at":   wf.CreatedAt,
	})
}

func (h *apiHandler) getWorkflow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := uuid.Parse(vars["id"])
	if err != nil {
		http.Error(w, `{"error":"invalid workflow id"}`, http.StatusBadRequest)
		return
	}

	wf, err := h.workflowRepo.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":           wf.ID,
		"name":         wf.Name,
		"status":       wf.Status,
		"current_step": wf.CurrentStep,
		"total_steps":  len(wf.Steps),
		"steps":        wf.Steps,
		"created_at":   wf.CreatedAt,
		"updated_at":   wf.UpdatedAt,
		"completed_at": wf.CompletedAt,
	})
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
