ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: run test lint migrate db-up db-down

run:
	go run ./cmd/server

lint:
	golangci-lint run

migrate:
	go run ./cmd/server -migrate-only

db-up:
	docker compose up -d postgres

db-down:
	docker compose down
