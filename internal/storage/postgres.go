package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type RetryConfig struct {
	Attempts     int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func NewPostgresPoolWithRetry(ctx context.Context, databaseURL string, retry RetryConfig, logger *slog.Logger) (*pgxpool.Pool, error) {
	if retry.Attempts <= 0 {
		retry.Attempts = 1
	}
	if retry.InitialDelay <= 0 {
		retry.InitialDelay = time.Second
	}
	if retry.MaxDelay <= 0 {
		retry.MaxDelay = 5 * time.Second
	}

	delay := retry.InitialDelay
	var lastErr error

	for attempt := 1; attempt <= retry.Attempts; attempt++ {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				logger.Info("connected to PostgreSQL", "attempt", attempt)
				return pool, nil
			}

			pool.Close()
		}

		lastErr = err
		if attempt == retry.Attempts {
			break
		}

		logger.Warn("postgres not ready yet; retrying", "attempt", attempt, "error", err, "next_delay", delay.String())

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		delay *= 2
		if delay > retry.MaxDelay {
			delay = retry.MaxDelay
		}
	}

	return nil, fmt.Errorf("failed to connect to PostgreSQL after %d attempts: %w", retry.Attempts, lastErr)
}
