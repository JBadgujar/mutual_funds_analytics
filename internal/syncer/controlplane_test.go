package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/mfapi"
)

func TestControlPlane_TriggerAndStatus_ReflectsProgress(t *testing.T) {
	ctx, pool, funds, navs, syncs := setupPipelineTestDB(t)

	insertTrackedFunds(t, ctx, funds,
		domain.Fund{SchemeCode: "5001", Name: "ICICI Prudential Mid Cap Fund - Direct Plan - Growth", Category: "Mid Cap Direct Growth", Active: true},
	)

	fetcher := newFakeNAVFetcher(map[string]mfapi.SchemeNavHistory{"5001": sampleHistoryByCode()["1001"]})
	fetcher.delay = 400 * time.Millisecond

	orchestrator := NewOrchestrator(pool, funds, navs, syncs, fetcher)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := NewControlPlane(ctx, orchestrator, syncs, logger)

	run, err := control.TriggerIncremental(context.Background(), "manual-api")
	if err != nil {
		t.Fatalf("trigger incremental: %v", err)
	}
	if run.ID == 0 {
		t.Fatal("expected non-zero run id")
	}
	if run.Status == "" {
		t.Fatal("expected non-empty run status")
	}

	stateVisible := false
	currentVisible := false
	statusTimeout := time.After(2 * time.Second)
	for !stateVisible || !currentVisible {
		select {
		case <-statusTimeout:
			t.Fatalf("timed out waiting for active run/state visibility, currentVisible=%v stateVisible=%v", currentVisible, stateVisible)
		default:
			currentRun, _, states, statusErr := control.GetStatus(context.Background(), 100, 0)
			if statusErr != nil {
				t.Fatalf("get status during run: %v", statusErr)
			}
			if currentRun != nil {
				currentVisible = true
			}
			if len(states) > 0 {
				stateVisible = true
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for run completion")
		default:
			active, last, _, statusErr := control.GetStatus(context.Background(), 100, 0)
			if statusErr != nil {
				t.Fatalf("get status: %v", statusErr)
			}
			if active == nil && last != nil && (last.Status == SyncStatusSuccess || last.Status == SyncStatusPartial || last.Status == SyncStatusFailed) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestControlPlane_ParallelTriggersPrevented(t *testing.T) {
	ctx, pool, funds, navs, syncs := setupPipelineTestDB(t)

	insertTrackedFunds(t, ctx, funds,
		domain.Fund{SchemeCode: "6001", Name: "HDFC Small Cap Fund - Direct Plan - Growth", Category: "Small Cap Direct Growth", Active: true},
	)

	fetcher := newFakeNAVFetcher(map[string]mfapi.SchemeNavHistory{"6001": sampleHistoryByCode()["1001"]})
	fetcher.delay = 300 * time.Millisecond

	orchestrator := NewOrchestrator(pool, funds, navs, syncs, fetcher)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := NewControlPlane(ctx, orchestrator, syncs, logger)

	firstRun, err := control.TriggerIncremental(context.Background(), "manual-api")
	if err != nil {
		t.Fatalf("first trigger failed: %v", err)
	}
	if firstRun.ID == 0 {
		t.Fatal("expected first trigger to return run id")
	}

	_, err = control.TriggerIncremental(context.Background(), "manual-api")
	if !errors.Is(err, ErrSyncAlreadyRunning) {
		t.Fatalf("expected ErrSyncAlreadyRunning on parallel trigger, got: %v", err)
	}
}
