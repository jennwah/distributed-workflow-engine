// Script load_test pushes a high volume of jobs to benchmark throughput.
// It sends a mix of reliable and flaky jobs to exercise the full retry +
// dead letter queue pipeline under sustained load.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultJobCount    = 500000
	defaultConcurrency = 200
	maxHTTPRetries     = 3
)

func main() {
	apiURL := getEnv("API_URL", "http://localhost:8080")
	jobCount := defaultJobCount
	concurrency := defaultConcurrency

	fmt.Printf("=== Workflow Engine Load Test ===\n")
	fmt.Printf("Target:      %s\n", apiURL)
	fmt.Printf("Total Jobs:  %d\n", jobCount)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Job Mix:     80%% reliable, 20%% flaky (50%% failure rate)\n\n")

	ctx := context.Background()
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			MaxConnsPerHost:     concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var (
		enqueueSuccess int64
		enqueueFail    int64
		httpRetries    int64
		flakyCount     int64
		reliableCount  int64
		wg             sync.WaitGroup
		sem            = make(chan struct{}, concurrency)
	)

	// Job type distribution: 80% reliable, 20% flaky.
	reliableTypes := []string{"default", "compute", "notify", "transform"}

	start := time.Now()

	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			var jobType string
			var payload map[string]any

			if idx%5 == 0 {
				// 20% of jobs are flaky — these will fail ~50% of the time on
				// each attempt. With max_retries=2, roughly 12.5% of flaky jobs
				// will exhaust retries and land in the dead letter queue.
				jobType = "flaky"
				payload = map[string]any{
					"failure_rate": 0.5,
					"index":        idx,
				}
				atomic.AddInt64(&flakyCount, 1)
			} else {
				jobType = reliableTypes[idx%len(reliableTypes)]
				payload = map[string]any{
					"load_test":  true,
					"index":      idx,
					"iterations": 100,
				}
				atomic.AddInt64(&reliableCount, 1)
			}

			payloadBytes, _ := json.Marshal(payload)
			reqBody, _ := json.Marshal(map[string]any{
				"type":        jobType,
				"payload":     json.RawMessage(payloadBytes),
				"max_retries": 2,
			})

			// Retry HTTP request on transient server errors.
			var lastErr error
			for attempt := 0; attempt <= maxHTTPRetries; attempt++ {
				if attempt > 0 {
					atomic.AddInt64(&httpRetries, 1)
					time.Sleep(time.Duration(attempt*50) * time.Millisecond)
				}

				req, err := http.NewRequestWithContext(ctx, "POST", apiURL+"/api/v1/jobs", bytes.NewReader(reqBody))
				if err != nil {
					lastErr = err
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					lastErr = err
					continue
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusCreated {
					atomic.AddInt64(&enqueueSuccess, 1)
					lastErr = nil
					break
				}

				if resp.StatusCode >= 500 {
					lastErr = fmt.Errorf("status %d", resp.StatusCode)
					continue
				}

				lastErr = fmt.Errorf("status %d", resp.StatusCode)
				break
			}

			if lastErr != nil {
				atomic.AddInt64(&enqueueFail, 1)
			}

			// Print progress every 50k jobs.
			count := atomic.LoadInt64(&enqueueSuccess) + atomic.LoadInt64(&enqueueFail)
			if count%50000 == 0 {
				elapsed := time.Since(start)
				rate := float64(count) / elapsed.Seconds() * 60
				fmt.Printf("  Progress: %d/%d jobs enqueued (%.0f jobs/min)\n", count, jobCount, rate)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== Enqueue Results ===\n")
	fmt.Printf("Duration:       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Enqueued:       %d / %d\n", enqueueSuccess, jobCount)
	fmt.Printf("Enqueue Fails:  %d\n", enqueueFail)
	fmt.Printf("HTTP Retries:   %d\n", httpRetries)
	fmt.Printf("Throughput:     %.0f jobs/min\n", float64(enqueueSuccess)/elapsed.Seconds()*60)
	fmt.Printf("Avg Latency:    %.2fms/job\n", float64(elapsed.Milliseconds())/float64(enqueueSuccess))
	fmt.Printf("\n--- Job Mix ---\n")
	fmt.Printf("Reliable jobs:  %d (should all succeed)\n", reliableCount)
	fmt.Printf("Flaky jobs:     %d (expect ~%.0f in dead letter queue)\n", flakyCount, float64(flakyCount)*0.125)

	fmt.Printf("\n--- What to check in Grafana ---\n")
	fmt.Printf("1. Jobs Throughput:   success rate climbs, then flaky failures appear\n")
	fmt.Printf("2. Queue Depth:       spikes to ~%dk, then drains as workers process\n", jobCount/1000)
	fmt.Printf("3. Dead Letter Queue: climbs as flaky jobs exhaust retries\n")
	fmt.Printf("4. Workers Busy:      all 3 workers saturated during processing\n")
	fmt.Printf("5. Job Latency:       p99 stays bounded even under failure load\n")

	if enqueueFail > 0 {
		fmt.Printf("\nWARNING: %d jobs failed to enqueue (HTTP-level failures)\n", enqueueFail)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
