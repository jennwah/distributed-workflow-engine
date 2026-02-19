package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/leejennwah/workflow-engine/internal/job"
)

const (
	jobQueueKey        = "workflow:jobs:pending"
	deadLetterQueueKey = "workflow:jobs:dead_letter"
	dequeueTimeout     = 5 * time.Second
)

// RedisQueue implements Queue using Redis lists with BRPOP for blocking dequeue.
type RedisQueue struct {
	client *redis.Client
	logger *zap.Logger
}

// NewRedisQueue creates a new Redis-backed job queue.
func NewRedisQueue(client *redis.Client, logger *zap.Logger) *RedisQueue {
	return &RedisQueue{
		client: client,
		logger: logger,
	}
}

// Enqueue pushes a job onto the pending queue using LPUSH.
func (q *RedisQueue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	if err := q.client.LPush(ctx, jobQueueKey, data).Err(); err != nil {
		return fmt.Errorf("lpush job: %w", err)
	}

	return nil
}

// Dequeue blocks until a job is available using BRPOP.
// Returns nil without error if the context is cancelled.
func (q *RedisQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	result, err := q.client.BRPop(ctx, dequeueTimeout, jobQueueKey).Result()
	if err != nil {
		if err == redis.Nil || ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("brpop: %w", err)
	}

	if len(result) < 2 {
		return nil, nil
	}

	var j job.Job
	if err := json.Unmarshal([]byte(result[1]), &j); err != nil {
		q.logger.Error("failed to unmarshal job from queue",
			zap.Error(err),
			zap.String("data", result[1]),
		)
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &j, nil
}

// EnqueueDeadLetter moves a job to the dead letter queue.
func (q *RedisQueue) EnqueueDeadLetter(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	if err := q.client.LPush(ctx, deadLetterQueueKey, data).Err(); err != nil {
		return fmt.Errorf("lpush dead letter: %w", err)
	}

	return nil
}

// Depth returns the number of jobs in the pending queue.
func (q *RedisQueue) Depth(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, jobQueueKey).Result()
}

// DeadLetterDepth returns the number of jobs in the dead letter queue.
func (q *RedisQueue) DeadLetterDepth(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, deadLetterQueueKey).Result()
}
