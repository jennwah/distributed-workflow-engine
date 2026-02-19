// Command migrate applies database migrations to PostgreSQL.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := getEnv("DATABASE_URL", "postgres://workflow:workflow@localhost:5432/workflow?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer pool.Close()

	migration, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		log.Fatalf("read migration file: %v", err)
	}

	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		log.Fatalf("apply migration: %v", err)
	}

	fmt.Println("migrations applied successfully")
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
