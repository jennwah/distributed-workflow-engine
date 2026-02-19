// Script seed_jobs pushes sample jobs and workflows to the API for development.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/leejennwah/workflow-engine/pkg/client"
)

func main() {
	apiURL := getEnv("API_URL", "http://localhost:8080")
	c := client.New(apiURL)
	ctx := context.Background()

	// Seed individual jobs.
	jobTypes := []string{"default", "compute", "notify", "transform"}
	for i := 0; i < 20; i++ {
		jobType := jobTypes[i%len(jobTypes)]
		payload, _ := json.Marshal(map[string]interface{}{
			"seed":       true,
			"index":      i,
			"iterations": 1000,
		})

		resp, err := c.EnqueueJob(ctx, &client.EnqueueJobRequest{
			Type:       jobType,
			Payload:    payload,
			MaxRetries: 3,
		})
		if err != nil {
			log.Printf("failed to enqueue job %d: %v", i, err)
			continue
		}
		fmt.Printf("enqueued job %s (type=%s)\n", resp.ID, jobType)
	}

	// Seed a multi-step workflow.
	wfResp, err := c.StartWorkflow(ctx, &client.StartWorkflowRequest{
		Name: "data-pipeline",
		Steps: []client.WorkflowStep{
			{
				Name:       "extract",
				Type:       "transform",
				Payload:    json.RawMessage(`{"action":"extract","source":"database"}`),
				MaxRetries: 3,
			},
			{
				Name:       "compute",
				Type:       "compute",
				Payload:    json.RawMessage(`{"iterations":5000}`),
				MaxRetries: 2,
			},
			{
				Name:       "notify",
				Type:       "notify",
				Payload:    json.RawMessage(`{"channel":"slack","message":"pipeline complete"}`),
				MaxRetries: 1,
			},
		},
	})
	if err != nil {
		log.Fatalf("failed to start workflow: %v", err)
	}
	fmt.Printf("started workflow %s (name=data-pipeline)\n", wfResp.ID)

	fmt.Println("\nseed complete: 20 jobs + 1 workflow")
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
