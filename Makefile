ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: run test lint migrate sync-backfill sync-backfill-recompute recompute-analytics db-up db-down

run:
	go run ./cmd/server

lint:
	golangci-lint run

migrate:
	go run ./cmd/server -migrate-only

sync-backfill:
	go run ./cmd/server -sync-backfill-only

sync-backfill-recompute:
	go run ./cmd/server -sync-backfill-recompute-only

recompute-analytics:
	go run ./cmd/server -recompute-analytics-only

db-up:
	docker compose up -d postgres

db-down:
	docker compose down
