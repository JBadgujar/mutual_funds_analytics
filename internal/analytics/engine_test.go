package analytics

import (
	"context"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

const integrationTestLockKey int64 = 9917001

func TestComputeWindowSnapshot_DeterministicGrowth(t *testing.T) {
	points := make([]NAVPoint, 0)
	nav := 100.0
	for year := 2012; year <= 2025; year++ {
		points = append(points, NAVPoint{Date: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC), NAV: nav})
		nav *= 1.10
	}

	snapshot := computeWindowSnapshot(points, WindowSpec{Code: "1Y", Years: 1})
	if snapshot.InsufficientData {
		t.Fatal("expected sufficient data for 1Y deterministic growth")
	}

	assertClose(t, snapshot.RollingReturnMin, 0.10, 1e-9, "rolling return min")
	assertClose(t, snapshot.RollingReturnMax, 0.10, 1e-9, "rolling return max")
	assertClose(t, snapshot.RollingReturnMedian, 0.10, 1e-9, "rolling return median")
	assertClose(t, snapshot.CAGRMin, 0.10, 5e-4, "cagr min")
	assertClose(t, snapshot.CAGRMax, 0.10, 5e-4, "cagr max")
	assertClose(t, snapshot.CAGRMedian, 0.10, 5e-4, "cagr median")
	assertClose(t, snapshot.MaxDrawdownDeclinePct, 0, 1e-12, "drawdown decline")
	assertClose(t, snapshot.AnnualizedVolatility, 0, 1e-12, "annualized volatility")
}

func TestComputeWindowSnapshot_TradingDayOnOrBefore(t *testing.T) {
	points := []NAVPoint{
		{Date: time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), NAV: 100},
		{Date: time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC), NAV: 104},
		{Date: time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC), NAV: 110},
	}

	snapshot := computeWindowSnapshot(points, WindowSpec{Code: "1Y", Years: 1})
	if snapshot.StartDate == nil {
		t.Fatal("expected start date to be resolved")
	}

	expectedStart := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	if !snapshot.StartDate.Equal(expectedStart) {
		t.Fatalf("expected start date %s, got %s", expectedStart.Format("2006-01-02"), snapshot.StartDate.Format("2006-01-02"))
	}
}

func TestComputeWindowSnapshot_EdgeCases(t *testing.T) {
	tests := []struct {
		name                 string
		points               []NAVPoint
		window               WindowSpec
		expectInsufficient   bool
		expectDrawdownMinPct float64
	}{
		{
			name: "sparse data insufficient",
			points: []NAVPoint{
				{Date: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), NAV: 100},
				{Date: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC), NAV: 101},
			},
			window:             WindowSpec{Code: "1Y", Years: 1},
			expectInsufficient: true,
		},
		{
			name: "flat nav",
			points: makeMonthlyPoints(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 48, func(i int) float64 {
				return 100
			}),
			window:             WindowSpec{Code: "1Y", Years: 1},
			expectInsufficient: false,
		},
		{
			name: "monotonic rise",
			points: makeMonthlyPoints(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 48, func(i int) float64 {
				return 100 * math.Pow(1.01, float64(i))
			}),
			window:             WindowSpec{Code: "1Y", Years: 1},
			expectInsufficient: false,
		},
		{
			name: "monotonic fall",
			points: makeMonthlyPoints(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 48, func(i int) float64 {
				return 100 * math.Pow(0.99, float64(i))
			}),
			window:               WindowSpec{Code: "1Y", Years: 1},
			expectInsufficient:   false,
			expectDrawdownMinPct: 0.05,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := computeWindowSnapshot(tc.points, tc.window)
			if snapshot.InsufficientData != tc.expectInsufficient {
				t.Fatalf("expected insufficient=%v got=%v", tc.expectInsufficient, snapshot.InsufficientData)
			}

			if tc.expectDrawdownMinPct > 0 && snapshot.MaxDrawdownDeclinePct < tc.expectDrawdownMinPct {
				t.Fatalf("expected drawdown >= %f got %f", tc.expectDrawdownMinPct, snapshot.MaxDrawdownDeclinePct)
			}

			if tc.name == "flat nav" {
				assertClose(t, snapshot.RollingReturnMedian, 0, 1e-12, "flat rolling median")
				assertClose(t, snapshot.CAGRMedian, 0, 1e-12, "flat cagr median")
			}
		})
	}
}

func TestEngine_RecomputeAll_PersistsSnapshotRows(t *testing.T) {
	ctx, pool, funds, navs, analyticsRepo := setupAnalyticsTestDB(t)

	insertedFund, err := funds.Upsert(ctx, domain.Fund{
		SchemeCode: "9001",
		Name:       "ICICI Prudential Mid Cap Fund - Direct Plan - Growth",
		Category:   "Mid Cap Direct Growth",
		Active:     true,
	})
	if err != nil {
		t.Fatalf("insert test fund: %v", err)
	}

	points := makeMonthlyPoints(time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC), 185, func(i int) float64 {
		return 100 * math.Pow(1.007, float64(i))
	})
	for _, point := range points {
		if err := navs.Upsert(ctx, domain.NAVHistory{FundID: insertedFund.ID, NAVDate: point.Date, NAV: point.NAV, Source: "fixture"}); err != nil {
			t.Fatalf("insert nav fixture: %v", err)
		}
	}

	engine := NewEngine(funds, navs, analyticsRepo)
	result, err := engine.RecomputeAll(ctx)
	if err != nil {
		t.Fatalf("recompute all: %v", err)
	}

	if result.FundsProcessed != 1 {
		t.Fatalf("expected funds processed 1, got %d", result.FundsProcessed)
	}
	if result.SnapshotsGenerated != 4 {
		t.Fatalf("expected snapshots generated 4, got %d", result.SnapshotsGenerated)
	}

	var rowCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(1) FROM analytics_snapshot WHERE fund_id = $1`, insertedFund.ID).Scan(&rowCount)
	if err != nil {
		t.Fatalf("count analytics rows: %v", err)
	}
	if rowCount != 4 {
		t.Fatalf("expected 4 analytics snapshot rows, got %d", rowCount)
	}

	rows, err := pool.Query(ctx, `SELECT window_code, insufficient_data FROM analytics_snapshot WHERE fund_id = $1`, insertedFund.ID)
	if err != nil {
		t.Fatalf("query snapshot windows: %v", err)
	}
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var windowCode string
		var insufficient bool
		if err := rows.Scan(&windowCode, &insufficient); err != nil {
			t.Fatalf("scan snapshot row: %v", err)
		}
		seen[windowCode] = true
	}
	if rows.Err() != nil {
		t.Fatalf("iterate snapshot windows: %v", rows.Err())
	}

	for _, expected := range []string{"1Y", "3Y", "5Y", "10Y"} {
		if !seen[expected] {
			t.Fatalf("missing analytics snapshot for window %s", expected)
		}
	}
}

func setupAnalyticsTestDB(t *testing.T) (context.Context, *pgxpool.Pool, domain.FundRepository, domain.NavRepository, domain.AnalyticsRepository) {
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

	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection for test lock: %v", err)
	}
	t.Cleanup(func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = lockConn.Exec(releaseCtx, `SELECT pg_advisory_unlock($1)`, integrationTestLockKey)
		lockConn.Release()
	})

	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, integrationTestLockKey); err != nil {
		t.Fatalf("acquire integration test advisory lock: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := storage.RunMigrations(databaseURL, "../../migrations", logger); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	_, err = pool.Exec(ctx, `
		TRUNCATE TABLE
			analytics_snapshot,
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
		storage.NewAnalyticsRepository(pool)
}

func makeMonthlyPoints(start time.Time, months int, navFn func(i int) float64) []NAVPoint {
	points := make([]NAVPoint, 0, months)
	for i := 0; i < months; i++ {
		points = append(points, NAVPoint{
			Date: start.AddDate(0, i, 0),
			NAV:  navFn(i),
		})
	}
	return points
}

func assertClose(t *testing.T, got, want, tolerance float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s mismatch: got=%f want=%f tolerance=%f", label, got, want, tolerance)
	}
}
