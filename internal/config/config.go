package config

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

const (
	defaultPort         = "8080"
	defaultDatabaseURL  = "postgres://postgres:postgres@localhost:5432/mutual_fund_analytics?sslmode=disable"
	defaultLogLevel     = "info"
	defaultSyncSchedule = "@every 1h"
)

type Config struct {
	Port         string
	DatabaseURL  string
	LogLevel     string
	SyncSchedule string
}

func Load() Config {
	// Best-effort load for local development; environment variables still take precedence.
	_ = godotenv.Load()

	return Config{
		Port:         getEnv("PORT", defaultPort),
		DatabaseURL:  getEnv("DATABASE_URL", defaultDatabaseURL),
		LogLevel:     getEnv("LOG_LEVEL", defaultLogLevel),
		SyncSchedule: getEnv("SYNC_SCHEDULE", defaultSyncSchedule),
	}
}

func getEnv(key, fallback string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return fallback
	}

	return trimmed
}
