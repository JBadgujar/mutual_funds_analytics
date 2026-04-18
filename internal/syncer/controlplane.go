package syncer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mutual-fund-analytics/internal/domain"
)

var ErrControlPlaneShuttingDown = errors.New("sync control plane is shutting down")

type ControlPlane struct {
	baseCtx      context.Context
	orchestrator *Orchestrator
	syncs        domain.SyncRepository
	logger       *slog.Logger

	shuttingDown atomic.Bool
	triggerSeq   atomic.Uint64
	wg           sync.WaitGroup
}

func NewControlPlane(baseCtx context.Context, orchestrator *Orchestrator, syncs domain.SyncRepository, logger *slog.Logger) *ControlPlane {
	if logger == nil {
		logger = slog.Default()
	}

	return &ControlPlane{
		baseCtx:      baseCtx,
		orchestrator: orchestrator,
		syncs:        syncs,
		logger:       logger,
	}
}

type runOutcome struct {
	result SyncResult
	err    error
}

func (c *ControlPlane) TriggerIncremental(ctx context.Context, source string) (domain.SyncRun, error) {
	if c.shuttingDown.Load() {
		return domain.SyncRun{}, ErrControlPlaneShuttingDown
	}

	seq := c.triggerSeq.Add(1)
	triggeredBy := fmt.Sprintf("%s:%d:%d", sanitizeTriggerSource(source), time.Now().UTC().UnixNano(), seq)
	outcomeCh := make(chan runOutcome, 1)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		c.logger.Info("sync run started", "mode", string(SyncModeIncremental), "triggered_by", triggeredBy)
		result, err := c.orchestrator.RunIncremental(c.baseCtx, triggeredBy)
		if err != nil {
			if errors.Is(err, ErrSyncAlreadyRunning) {
				c.logger.Warn("sync run rejected", "mode", string(SyncModeIncremental), "triggered_by", triggeredBy, "reason", "already_running")
			} else {
				c.logger.Error("sync run failed", "mode", string(SyncModeIncremental), "triggered_by", triggeredBy, "error", err)
			}
			outcomeCh <- runOutcome{result: result, err: err}
			return
		}

		c.logger.Info("sync run completed",
			"mode", string(result.Mode),
			"triggered_by", triggeredBy,
			"run_id", result.RunID,
			"status", result.FinalStatus,
			"processed_funds", result.ProcessedFunds,
			"failed_funds", result.FailedFunds,
			"inserted_nav_rows", result.InsertedNAVRows,
		)
		outcomeCh <- runOutcome{result: result, err: nil}
	}()

	pollTicker := time.NewTicker(25 * time.Millisecond)
	defer pollTicker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return domain.SyncRun{}, ctx.Err()
		case <-timeout.C:
			run, err := c.syncs.GetLatestRunByTriggeredBy(ctx, triggeredBy)
			if err == nil {
				return run, nil
			}
			if errors.Is(err, sql.ErrNoRows) {
				return domain.SyncRun{}, fmt.Errorf("timed out waiting for sync run to be enqueued")
			}
			return domain.SyncRun{}, err
		case out := <-outcomeCh:
			if out.err != nil {
				return domain.SyncRun{}, out.err
			}

			run, err := c.syncs.GetLatestRunByTriggeredBy(ctx, triggeredBy)
			if err != nil {
				return domain.SyncRun{
					ID:          out.result.RunID,
					Status:      out.result.FinalStatus,
					TriggeredBy: triggeredBy,
				}, nil
			}
			return run, nil
		case <-pollTicker.C:
			run, err := c.syncs.GetLatestRunByTriggeredBy(ctx, triggeredBy)
			if err == nil {
				return run, nil
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return domain.SyncRun{}, err
			}
		}
	}
}

func (c *ControlPlane) GetStatus(ctx context.Context, limit, offset int32) (*domain.SyncRun, *domain.SyncRun, []domain.SyncFundStateView, error) {
	var currentRun *domain.SyncRun
	var lastRun *domain.SyncRun

	active, err := c.syncs.GetActiveRun(ctx)
	if err == nil {
		currentRun = &active
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, fmt.Errorf("get active run: %w", err)
	}

	latest, err := c.syncs.GetLatestRun(ctx)
	if err == nil {
		lastRun = &latest
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil, fmt.Errorf("get latest run: %w", err)
	}

	states, err := c.syncs.ListFundStates(ctx, limit, offset)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list fund sync states: %w", err)
	}

	return currentRun, lastRun, states, nil
}

func (c *ControlPlane) Shutdown(ctx context.Context) error {
	c.shuttingDown.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.wg.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func sanitizeTriggerSource(source string) string {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return "manual-api"
	}

	return trimmed
}
