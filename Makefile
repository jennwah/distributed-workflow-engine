.PHONY: up down build test load-test load-test-workflow seed migrate lint fmt clean

# Start all services via Docker Compose.
up:
	docker compose -f deployments/docker-compose.yml up --build -d

# Stop and remove all containers.
down:
	docker compose -f deployments/docker-compose.yml down -v

# Build Go binaries locally.
build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/migrate ./cmd/migrate

# Run unit tests.
test:
	go test -v -race -count=1 ./internal/...

# Run integration tests (requires running services).
test-integration:
	go test -v -race -tags=integration -count=1 ./...

# Run the jobs load test (500k jobs, mix of reliable + flaky).
load-test:
	go run ./scripts/loadtest/main.go

# Run the workflow load test (50k workflows × 3 steps = 150k total steps).
load-test-workflow:
	go run ./scripts/loadtestworkflow/main.go

# Seed the database with sample data.
seed:
	go run ./scripts/seed/main.go

# Apply database migrations.
migrate:
	go run ./cmd/migrate/main.go

# Format code.
fmt:
	go fmt ./...

# Lint code.
lint:
	golangci-lint run ./...

# Remove build artifacts.
clean:
	rm -rf bin/
