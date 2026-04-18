package main

import (
	"context"
	"flag"
	"log/slog"
	stdhttp "net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mutual-fund-analytics/internal/config"
	httptransport "mutual-fund-analytics/internal/http"
	"mutual-fund-analytics/internal/storage"
)

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "run database migrations and exit")
	flag.Parse()

	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := storage.NewPostgresPoolWithRetry(ctx, cfg.DatabaseURL, storage.RetryConfig{
		Attempts:     10,
		InitialDelay: time.Second,
		MaxDelay:     5 * time.Second,
	}, logger)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := storage.RunMigrations(cfg.DatabaseURL, "migrations", logger); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}

	if *migrateOnly {
		logger.Info("migrations completed")
		return
	}

	fundRepo := storage.NewFundRepository(pool)
	analyticsRepo := storage.NewAnalyticsRepository(pool)
	api := httptransport.NewAPI(fundRepo, analyticsRepo)

	server := &stdhttp.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httptransport.NewRouter(api),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("starting HTTP server", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != stdhttp.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			cancel()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}

	logger.Info("server stopped")
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
