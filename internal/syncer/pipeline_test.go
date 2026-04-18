package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/mfapi"
	"mutual-fund-analytics/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeNAVFetcher struct {
	mu          sync.Mutex
	history     map[string]mfapi.SchemeNavHistory
	delay       time.Duration
	callCounter map[string]int
}

func newFakeNAVFetcher(history map[string]mfapi.SchemeNavHistory) *fakeNAVFetcher {
	return &fakeNAVFetcher{
		history:     history,
		callCounter: make(map[string]int),
	}
}

func (f *fakeNAVFetcher) FetchSchemeNavHistory(ctx context.Context, code string) (mfapi.SchemeNavHistory, error) {
	f.mu.Lock()
	f.callCounter[code]++
	f.mu.Unlock()

	if f.delay > 0 {
		t := time.NewTimer(f.delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return mfapi.SchemeNavHistory{}, ctx.Err()
		case <-t.C:
		}
	}

	history, ok := f.history[code]
	if !ok {
		return mfapi.SchemeNavHistory{}, errors.New("unknown scheme code")
	}

	return history, nil
}

func TestBackfill_CrashAndResumeFromCheckpoint(t *testing.T) {
	ctx, pool, funds, navs, syncs := setupPipelineTestDB(t)

	insertTrackedFunds(t, ctx, funds,
		domain.Fund{SchemeCode: "1001", Name: "ICICI Prudential Mid Cap Fund - Direct Plan - Growth", Category: "Mid Cap Direct Growth", Active: true},
		domain.Fund{SchemeCode: "1002", Name: "HDFC Small Cap Fund - Direct Plan - Growth", Category: "Small Cap Direct Growth", Active: true},
		domain.Fund{SchemeCode: "1003", Name: "Axis Midcap Fund - Direct Plan - Growth", Category: "Mid Cap Direct Growth", Active: true},
	)

	fetcher := newFakeNAVFetcher(sampleHistoryByCode())
	orch := NewOrchestrator(pool, funds, navs, syncs, fetcher)

	crashed := false
	orch.beforeFundHook = func(fund domain.Fund) error {
		if fund.SchemeCode == "1002" && !crashed {
			crashed = true
			return errors.New("simulated crash")
		}
		return nil
	}

	firstRun, err := orch.RunBackfill(ctx, "test-crash")
	if err == nil {
		t.Fatal("expected simulated crash error on first run")
	}
	if firstRun.FinalStatus != SyncStatusPartial {
		t.Fatalf("expected first run final status partial, got %s", firstRun.FinalStatus)
	}

	state1001 := mustFundState(t, ctx, pool, "1001")
	state1002 := mustFundState(t, ctx, pool, "1002")
	state1003 := mustFundState(t, ctx, pool, "1003")

	if state1001.Status != "synced" {
		t.Fatalf("expected fund 1001 synced after checkpoint, got %s", state1001.Status)
	}
	if state1002.Status != "pending" {
		t.Fatalf("expected fund 1002 pending after crash, got %s", state1002.Status)
	}
	if state1003.Status != "pending" {
		t.Fatalf("expected fund 1003 pending after crash, got %s", state1003.Status)
	}

	orch.beforeFundHook = nil
	secondRun, err := orch.RunBackfill(ctx, "test-resume")
	if err != nil {
		t.Fatalf("resume backfill failed: %v", err)
	}
	if secondRun.FinalStatus != SyncStatusSuccess {
		t.Fatalf("expected resumed run success, got %s", secondRun.FinalStatus)
	}
	if secondRun.ProcessedFunds != 2 {
		t.Fatalf("expected resumed run to process 2 funds, got %d", secondRun.ProcessedFunds)
	}

	if rows := countNAVRows(t, ctx, pool); rows != 6 {
		t.Fatalf("expected 6 nav rows after resume, got %d", rows)
	}

	runStatuses := syncRunStatuses(t, ctx, pool)
	if len(runStatuses) < 2 {
		t.Fatalf("expected at least two sync runs, got %d", len(runStatuses))
	}
	if runStatuses[0] != SyncStatusPartial || runStatuses[1] != SyncStatusSuccess {
		t.Fatalf("unexpected run lifecycle statuses: %+v", runStatuses)
	}
}

func TestIncremental_DoesNotCreateDuplicateRows(t *testing.T) {
	ctx, pool, funds, navs, syncs := setupPipelineTestDB(t)

	insertTrackedFunds(t, ctx, funds,
		domain.Fund{SchemeCode: "2001", Name: "SBI Midcap Fund - Direct Plan - Growth", Category: "Mid Cap Direct Growth", Active: true},
	)

	history := sampleHistoryByCode()
	fetcher := newFakeNAVFetcher(map[string]mfapi.SchemeNavHistory{"2001": history["1001"]})
	orch := NewOrchestrator(pool, funds, navs, syncs, fetcher)

	first, err := orch.RunIncremental(ctx, "test-incremental-1")
	if err != nil {
		t.Fatalf("first incremental run failed: %v", err)
	}
	if first.InsertedNAVRows != 2 {
		t.Fatalf("expected first incremental inserted 2 rows, got %d", first.InsertedNAVRows)
	}

	second, err := orch.RunIncremental(ctx, "test-incremental-2")
	if err != nil {
		t.Fatalf("second incremental run failed: %v", err)
	}
	if second.InsertedNAVRows != 0 {
		t.Fatalf("expected second incremental inserted 0 rows, got %d", second.InsertedNAVRows)
	}

	if rows := countNAVRows(t, ctx, pool); rows != 2 {
		t.Fatalf("expected exactly 2 nav rows after repeated incremental runs, got %d", rows)
	}
}

func TestParallelTriggers_OnlyOneActiveRun(t *testing.T) {
	ctx, pool, funds, navs, syncs := setupPipelineTestDB(t)

	insertTrackedFunds(t, ctx, funds,
		domain.Fund{SchemeCode: "3001", Name: "Kotak Midcap Fund - Direct Plan - Growth", Category: "Mid Cap Direct Growth", Active: true},
	)

	fetcher := newFakeNAVFetcher(map[string]mfapi.SchemeNavHistory{"3001": sampleHistoryByCode()["1001"]})
	fetcher.delay = 300 * time.Millisecond
	orch := NewOrchestrator(pool, funds, navs, syncs, fetcher)

	type outcome struct {
		result SyncResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)

	runOnce := func(label string) {
		<-start
		res, err := orch.RunBackfill(ctx, label)
		outcomes <- outcome{result: res, err: err}
	}

	go runOnce("parallel-a")
	go runOnce("parallel-b")
	close(start)

	oneSuccess := 0
	oneRejected := 0
	for i := 0; i < 2; i++ {
		out := <-outcomes
		if out.err == nil {
			oneSuccess++
			continue
		}
		if errors.Is(out.err, ErrSyncAlreadyRunning) {
			oneRejected++
			continue
		}
		t.Fatalf("unexpected parallel trigger error: %v", out.err)
	}

	if oneSuccess != 1 || oneRejected != 1 {
		t.Fatalf("expected one success and one rejection, got success=%d rejected=%d", oneSuccess, oneRejected)
	}

	if rows := countSyncRuns(t, ctx, pool); rows != 1 {
		t.Fatalf("expected exactly one sync_runs row, got %d", rows)
	}

	if active := countActiveSyncRuns(t, ctx, pool); active != 0 {
		t.Fatalf("expected zero active sync runs after completion, got %d", active)
	}
}

func setupPipelineTestDB(t *testing.T) (context.Context, *pgxpool.Pool, domain.FundRepository, domain.NavRepository, domain.SyncRepository) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/mutual_fund_analytics?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := storage.RunMigrations(databaseURL, "../../migrations", logger); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	_, err = pool.Exec(ctx, `
		TRUNCATE TABLE
			nav_history,
			sync_fund_state,
			sync_runs,
			funds
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}

	return ctx,
		pool,
		storage.NewFundRepository(pool),
		storage.NewNavRepository(pool),
		storage.NewSyncRepository(pool)
}

func insertTrackedFunds(t *testing.T, ctx context.Context, funds domain.FundRepository, entries ...domain.Fund) {
	t.Helper()
	for _, entry := range entries {
		if _, err := funds.Upsert(ctx, entry); err != nil {
			t.Fatalf("insert tracked fund %s: %v", entry.SchemeCode, err)
		}
	}
}

func sampleHistoryByCode() map[string]mfapi.SchemeNavHistory {
	pointA := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pointB := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

	makeHistory := func(code string) mfapi.SchemeNavHistory {
		return mfapi.SchemeNavHistory{
			SchemeCode: code,
			Data: []mfapi.SchemeNAVPoint{
				{Date: pointA, NAV: 100.11},
				{Date: pointB, NAV: 101.23},
			},
		}
	}

	return map[string]mfapi.SchemeNavHistory{
		"1001": makeHistory("1001"),
		"1002": makeHistory("1002"),
		"1003": makeHistory("1003"),
	}
}

type fundStateView struct {
	Status string
}

func mustFundState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemeCode string) fundStateView {
	t.Helper()

	var out fundStateView
	err := pool.QueryRow(ctx, `
		SELECT s.status
		FROM sync_fund_state s
		JOIN funds f ON f.id = s.fund_id
		WHERE f.scheme_code = $1
	`, schemeCode).Scan(&out.Status)
	if err != nil {
		t.Fatalf("query fund state for %s: %v", schemeCode, err)
	}

	return out
}

func countNAVRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(1) FROM nav_history`).Scan(&count); err != nil {
		t.Fatalf("count nav rows: %v", err)
	}
	return count
}

func countSyncRuns(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(1) FROM sync_runs`).Scan(&count); err != nil {
		t.Fatalf("count sync runs: %v", err)
	}
	return count
}

func countActiveSyncRuns(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(1) FROM sync_runs WHERE status IN ('queued', 'running')`).Scan(&count); err != nil {
		t.Fatalf("count active sync runs: %v", err)
	}
	return count
}

func syncRunStatuses(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(ctx, `SELECT status FROM sync_runs ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query sync run statuses: %v", err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatalf("scan sync run status: %v", err)
		}
		out = append(out, status)
	}
	if rows.Err() != nil {
		t.Fatalf("iterate sync run statuses: %v", rows.Err())
	}

	return out
}
