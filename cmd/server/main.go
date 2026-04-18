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

	"mutual-fund-analytics/internal/analytics"
	"mutual-fund-analytics/internal/config"
	"mutual-fund-analytics/internal/domain"
	httptransport "mutual-fund-analytics/internal/http"
	"mutual-fund-analytics/internal/limiter"
	"mutual-fund-analytics/internal/mfapi"
	"mutual-fund-analytics/internal/storage"
	"mutual-fund-analytics/internal/syncer"
)

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "run database migrations and exit")
	recomputeAnalyticsOnly := flag.Bool("recompute-analytics-only", false, "recompute analytics snapshots and exit")
	syncBackfillOnly := flag.Bool("sync-backfill-only", false, "discover tracked schemes and run backfill sync, then exit")
	syncBackfillRecomputeOnly := flag.Bool("sync-backfill-recompute-only", false, "discover tracked schemes, run backfill sync, recompute analytics snapshots, then exit")
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

	oneShotModes := 0
	if *recomputeAnalyticsOnly {
		oneShotModes++
	}
	if *syncBackfillOnly {
		oneShotModes++
	}
	if *syncBackfillRecomputeOnly {
		oneShotModes++
	}

	if oneShotModes > 1 {
		logger.Error("only one one-shot mode can be used at a time")
		os.Exit(1)
	}

	fundRepo := storage.NewFundRepository(pool)
	navRepo := storage.NewNavRepository(pool)
	analyticsRepo := storage.NewAnalyticsRepository(pool)
	syncRepo := storage.NewSyncRepository(pool)
	apiLimiter := limiter.NewPersistentLimiter(pool)
	mfClient, err := mfapi.NewClient(nil, apiLimiter)
	if err != nil {
		logger.Error("failed to create mfapi client", "error", err)
		os.Exit(1)
	}
	orchestrator := syncer.NewOrchestrator(pool, fundRepo, navRepo, syncRepo, mfClient)

	if *recomputeAnalyticsOnly {
		runAnalyticsRecompute(ctx, logger, fundRepo, navRepo, analyticsRepo)
		return
	}

	if *syncBackfillOnly || *syncBackfillRecomputeOnly {
		discovery := syncer.NewDiscoveryService(mfClient, fundRepo)
		discoveryResult, err := discovery.DiscoverAndTrack(ctx)
		if err != nil {
			logger.Error("fund discovery failed", "error", err)
			os.Exit(1)
		}

		logger.Info("fund discovery completed",
			"total_discovered_schemes", discoveryResult.TotalDiscoveredSchemes,
			"tracked_schemes", len(discoveryResult.TrackedSchemes),
		)

		syncResult, err := orchestrator.RunBackfill(ctx, "manual-cli")
		if err != nil {
			logger.Error("backfill sync failed", "error", err)
			os.Exit(1)
		}

		logger.Info("backfill sync completed",
			"run_id", syncResult.RunID,
			"status", syncResult.FinalStatus,
			"total_funds", syncResult.TotalFunds,
			"processed_funds", syncResult.ProcessedFunds,
			"failed_funds", syncResult.FailedFunds,
			"inserted_nav_rows", syncResult.InsertedNAVRows,
		)

		if *syncBackfillRecomputeOnly {
			runAnalyticsRecompute(ctx, logger, fundRepo, navRepo, analyticsRepo)
		}

		return
	}

	api := httptransport.NewAPI(fundRepo, analyticsRepo)
	controlPlane := syncer.NewControlPlane(ctx, orchestrator, syncRepo, logger)
	api.SetSyncController(controlPlane)

	enabled, interval, err := syncer.ParseScheduleInterval(cfg.SyncSchedule)
	if err != nil {
		logger.Error("invalid sync schedule", "schedule", cfg.SyncSchedule, "error", err)
		os.Exit(1)
	}
	if enabled {
		scheduler := syncer.NewScheduler(controlPlane, logger, interval)
		scheduler.Start(ctx)
	} else {
		logger.Info("sync scheduler disabled", "schedule", cfg.SyncSchedule)
	}

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

	if err := controlPlane.Shutdown(shutdownCtx); err != nil {
		logger.Error("sync control plane shutdown failed", "error", err)
	}

	logger.Info("server stopped")
}

func runAnalyticsRecompute(ctx context.Context, logger *slog.Logger, fundRepo domain.FundRepository, navRepo domain.NavRepository, analyticsRepo domain.AnalyticsRepository) {
	engine := analytics.NewEngine(fundRepo, navRepo, analyticsRepo)
	result, err := engine.RecomputeAll(ctx)
	if err != nil {
		logger.Error("analytics recompute failed", "error", err)
		os.Exit(1)
	}

	logger.Info("analytics recompute completed",
		"funds_processed", result.FundsProcessed,
		"snapshots_generated", result.SnapshotsGenerated,
		"insufficient_snapshots", result.InsufficientSnapshots,
	)
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
