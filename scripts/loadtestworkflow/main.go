// Script loadtestworkflow benchmarks multi-step workflow orchestration.
// It creates 50k workflows, each with 3 sequential steps (extract → compute → notify),
// demonstrating the engine's ability to orchestrate durable, multi-step pipelines at scale.
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
	defaultWorkflowCount = 50000
	defaultConcurrency   = 150
	maxHTTPRetries       = 3
	stepsPerWorkflow     = 3
)

func main() {
	apiURL := getEnv("API_URL", "http://localhost:8080")
	workflowCount := defaultWorkflowCount
	concurrency := defaultConcurrency
	totalSteps := workflowCount * stepsPerWorkflow

	fmt.Printf("=== Workflow Engine — Workflow Load Test ===\n")
	fmt.Printf("Target:           %s\n", apiURL)
	fmt.Printf("Total Workflows:  %d\n", workflowCount)
	fmt.Printf("Steps/Workflow:   %d\n", stepsPerWorkflow)
	fmt.Printf("Total Steps:      %d\n", totalSteps)
	fmt.Printf("Concurrency:      %d\n\n", concurrency)

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
		wg             sync.WaitGroup
		sem            = make(chan struct{}, concurrency)
	)

	// Each workflow is a 3-step pipeline: extract → compute → notify.
	// This mirrors a real ETL or data processing pipeline.
	makeWorkflowBody := func(idx int) []byte {
		body, _ := json.Marshal(map[string]any{
			"name": fmt.Sprintf("pipeline-%d", idx),
			"steps": []map[string]any{
				{
					"name":        "extract",
					"type":        "transform",
					"payload":     map[string]any{"source": "database", "table": fmt.Sprintf("events_%d", idx%100)},
					"max_retries": 2,
				},
				{
					"name":        "compute",
					"type":        "compute",
					"payload":     map[string]any{"iterations": 500, "pipeline_id": idx},
					"max_retries": 2,
				},
				{
					"name":        "notify",
					"type":        "notify",
					"payload":     map[string]any{"channel": "slack", "message": fmt.Sprintf("pipeline-%d complete", idx)},
					"max_retries": 1,
				},
			},
		})
		return body
	}

	start := time.Now()

	for i := 0; i < workflowCount; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			reqBody := makeWorkflowBody(idx)

			var lastErr error
			for attempt := 0; attempt <= maxHTTPRetries; attempt++ {
				if attempt > 0 {
					atomic.AddInt64(&httpRetries, 1)
					time.Sleep(time.Duration(attempt*50) * time.Millisecond)
				}

				req, err := http.NewRequestWithContext(ctx, "POST", apiURL+"/api/v1/workflows", bytes.NewReader(reqBody))
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

			count := atomic.LoadInt64(&enqueueSuccess) + atomic.LoadInt64(&enqueueFail)
			if count%5000 == 0 {
				elapsed := time.Since(start)
				rate := float64(count) / elapsed.Seconds() * 60
				fmt.Printf("  Progress: %d/%d workflows submitted (%.0f wf/min)\n", count, workflowCount, rate)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== Submission Results ===\n")
	fmt.Printf("Duration:          %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Workflows Created: %d / %d\n", enqueueSuccess, workflowCount)
	fmt.Printf("Submit Failures:   %d\n", enqueueFail)
	fmt.Printf("HTTP Retries:      %d\n", httpRetries)
	fmt.Printf("Submit Rate:       %.0f workflows/min\n", float64(enqueueSuccess)/elapsed.Seconds()*60)
	fmt.Printf("Steps Enqueued:    %d (first step of each workflow)\n", enqueueSuccess)
	fmt.Printf("Total Steps:       %d (will be created as each step completes)\n", enqueueSuccess*stepsPerWorkflow)

	fmt.Printf("\n--- What to watch in Grafana ---\n")
	fmt.Printf("1. Queue Depth:      spikes as step-1 jobs land, drains, then spikes\n")
	fmt.Printf("                     again as step-2 and step-3 jobs are enqueued\n")
	fmt.Printf("2. Jobs Throughput:  sustained processing across all 3 waves of steps\n")
	fmt.Printf("3. Workers Busy:     all workers saturated through the entire pipeline\n")
	fmt.Printf("4. Job Latency:      may increase slightly on later steps (pipeline depth)\n")
	fmt.Printf("5. Jobs by Type:     transform → compute → notify waves visible over time\n")

	if enqueueFail > 0 {
		fmt.Printf("\nWARNING: %d workflows failed to submit\n", enqueueFail)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
