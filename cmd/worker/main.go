// Command worker starts the job processing worker.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
	"github.com/leejennwah/workflow-engine/internal/metrics"
	"github.com/leejennwah/workflow-engine/internal/queue"
	"github.com/leejennwah/workflow-engine/internal/retry"
	"github.com/leejennwah/workflow-engine/internal/scheduler"
	"github.com/leejennwah/workflow-engine/internal/storage"
	"github.com/leejennwah/workflow-engine/internal/tracing"
	"github.com/leejennwah/workflow-engine/internal/workflow"

	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize tracing.
	otlpEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	shutdownTracer, err := tracing.Init(ctx, "workflow-worker", otlpEndpoint)
	if err != nil {
		logger.Warn("tracing init failed, continuing without tracing", zap.Error(err))
	} else {
		defer shutdownTracer(ctx)
	}

	// Connect to Postgres with tuned pool for worker operations.
	dsn := getEnv("DATABASE_URL", "postgres://workflow:workflow@localhost:5432/workflow?sslmode=disable")
	pgConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logger.Fatal("parse postgres config", zap.Error(err))
	}
	pgConfig.MaxConns = 20
	pgConfig.MinConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		logger.Fatal("connect to postgres", zap.Error(err))
	}
	defer pool.Close()

	// Connect to Redis with tuned pool.
	redisAddr := getEnv("REDIS_URL", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     50,
		MinIdleConns: 10,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("connect to redis", zap.Error(err))
	}
	defer rdb.Close()

	// Initialize dependencies.
	jobRepo := storage.NewPostgresJobRepository(pool, logger)
	workflowRepo := storage.NewPostgresWorkflowRepository(pool, logger)
	q := queue.NewRedisQueue(rdb, logger)
	m := metrics.New()
	retryPolicy := retry.DefaultPolicy()
	wfEngine := workflow.NewEngine(workflowRepo, jobRepo, q, m, logger)

	cfg := scheduler.DefaultConfig()
	sched := scheduler.New(jobRepo, q, wfEngine, retryPolicy, m, logger, cfg)

	// Register task handlers.
	sched.RegisterHandler("default", defaultHandler(logger))
	sched.RegisterHandler("compute", computeHandler(logger))
	sched.RegisterHandler("notify", notifyHandler(logger))
	sched.RegisterHandler("transform", transformHandler(logger))
	sched.RegisterHandler("flaky", flakyHandler(logger))

	// Expose metrics endpoint for Prometheus scraping.
	metricsAddr := getEnv("METRICS_ADDR", ":9091")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		logger.Info("metrics server starting", zap.String("addr", metricsAddr))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			logger.Error("metrics server failed", zap.Error(err))
		}
	}()

	// Run the scheduler.
	if err := sched.Run(ctx, cfg); err != nil {
		logger.Fatal("scheduler error", zap.Error(err))
	}
}

// defaultHandler simulates a generic task with random duration.
func defaultHandler(logger *zap.Logger) scheduler.TaskHandler {
	return func(ctx context.Context, j *job.Job) error {
		logger.Info("executing default task",
			zap.String("job_id", j.ID.String()),
			zap.String("type", j.Type),
		)
		// Simulate work with 1-50ms latency.
		time.Sleep(time.Duration(1+rand.Intn(50)) * time.Millisecond)
		return nil
	}
}

// computeHandler simulates a CPU-intensive computation.
func computeHandler(logger *zap.Logger) scheduler.TaskHandler {
	return func(ctx context.Context, j *job.Job) error {
		logger.Info("executing compute task", zap.String("job_id", j.ID.String()))

		var payload struct {
			Iterations int `json:"iterations"`
		}
		if err := json.Unmarshal(j.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		if payload.Iterations <= 0 {
			payload.Iterations = 1000
		}

		// Simulate computation.
		result := 0.0
		for i := 0; i < payload.Iterations; i++ {
			result += float64(i) * 0.001
		}

		time.Sleep(time.Duration(5+rand.Intn(20)) * time.Millisecond)
		return nil
	}
}

// notifyHandler simulates sending a notification.
func notifyHandler(logger *zap.Logger) scheduler.TaskHandler {
	return func(ctx context.Context, j *job.Job) error {
		logger.Info("executing notify task", zap.String("job_id", j.ID.String()))
		time.Sleep(time.Duration(2+rand.Intn(10)) * time.Millisecond)
		return nil
	}
}

// transformHandler simulates a data transformation.
func transformHandler(logger *zap.Logger) scheduler.TaskHandler {
	return func(ctx context.Context, j *job.Job) error {
		logger.Info("executing transform task", zap.String("job_id", j.ID.String()))
		time.Sleep(time.Duration(3+rand.Intn(15)) * time.Millisecond)
		return nil
	}
}

// flakyHandler simulates an unreliable external service. It reads a
// failure_rate from the payload (0.0-1.0) and fails that percentage of
// executions. Jobs that exhaust retries end up in the dead letter queue,
// demonstrating the engine's retry + DLQ pipeline under load.
func flakyHandler(logger *zap.Logger) scheduler.TaskHandler {
	return func(ctx context.Context, j *job.Job) error {
		var payload struct {
			FailureRate float64 `json:"failure_rate"`
		}
		if err := json.Unmarshal(j.Payload, &payload); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}
		if payload.FailureRate <= 0 {
			payload.FailureRate = 0.5
		}

		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

		if rand.Float64() < payload.FailureRate {
			logger.Warn("flaky task failed",
				zap.String("job_id", j.ID.String()),
				zap.Int("attempt", j.Attempt),
				zap.Float64("failure_rate", payload.FailureRate),
			)
			return fmt.Errorf("simulated transient failure (attempt %d/%d)", j.Attempt, j.MaxRetries)
		}

		logger.Info("flaky task succeeded",
			zap.String("job_id", j.ID.String()),
			zap.Int("attempt", j.Attempt),
		)
		return nil
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
