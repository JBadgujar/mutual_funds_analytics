package limiter

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mutual-fund-analytics/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAcquire_RespectsAllLimitsWithPersistentState(t *testing.T) {
	ctx, pool, limiter := setupLimiterTest(t)

	current := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	limiter.clock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	limiter.waitFn = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		current = current.Add(d)
		mu.Unlock()
		return nil
	}

	for i := 0; i < 420; i++ {
		if err := limiter.Acquire(ctx); err != nil {
			t.Fatalf("acquire failed at request %d: %v", i, err)
		}

		state := fetchState(t, ctx, pool, limiter.provider)
		if state.secondCount > SecondLimit {
			t.Fatalf("second limit violated: got %d > %d", state.secondCount, SecondLimit)
		}
		if state.minuteCount > MinuteLimit {
			t.Fatalf("minute limit violated: got %d > %d", state.minuteCount, MinuteLimit)
		}
		if state.hourCount > HourLimit {
			t.Fatalf("hour limit violated: got %d > %d", state.hourCount, HourLimit)
		}
	}
}

func TestAcquireAt_MinuteLimit(t *testing.T) {
	ctx, pool, limiter := setupLimiterTest(t)

	now := time.Date(2026, 1, 1, 10, 12, 10, 0, time.UTC)
	seedState(t, ctx, pool, limiter.provider, stateRow{
		secondBucket: now.Unix(),
		minuteBucket: now.Unix() / 60,
		hourBucket:   now.Unix() / 3600,
		secondCount:  0,
		minuteCount:  49,
		hourCount:    100,
	})

	if waitFor, err := limiter.acquireAt(ctx, now); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	} else if waitFor != 0 {
		t.Fatalf("first acquire should be allowed, wait=%v", waitFor)
	}

	nowPlusSecond := now.Add(time.Second)
	if waitFor, err := limiter.acquireAt(ctx, nowPlusSecond); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	} else if waitFor <= 0 || waitFor > time.Minute {
		t.Fatalf("second acquire should be blocked by minute limit with <=1m wait, got %v", waitFor)
	}
}

func TestAcquireAt_HourLimit(t *testing.T) {
	ctx, pool, limiter := setupLimiterTest(t)

	now := time.Date(2026, 1, 1, 10, 12, 10, 0, time.UTC)
	seedState(t, ctx, pool, limiter.provider, stateRow{
		secondBucket: now.Unix(),
		minuteBucket: now.Unix() / 60,
		hourBucket:   now.Unix() / 3600,
		secondCount:  0,
		minuteCount:  10,
		hourCount:    299,
	})

	if waitFor, err := limiter.acquireAt(ctx, now); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	} else if waitFor != 0 {
		t.Fatalf("first acquire should be allowed, wait=%v", waitFor)
	}

	nowPlusSecond := now.Add(time.Second)
	if waitFor, err := limiter.acquireAt(ctx, nowPlusSecond); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	} else if waitFor < 45*time.Minute {
		t.Fatalf("second acquire should be blocked by hour limit; wait=%v", waitFor)
	}
}

func TestAcquireAt_BoundaryTransitions(t *testing.T) {
	ctx, pool, limiter := setupLimiterTest(t)

	base := time.Date(2026, 1, 1, 10, 59, 59, 0, time.UTC)
	seedState(t, ctx, pool, limiter.provider, stateRow{
		secondBucket: base.Unix(),
		minuteBucket: base.Unix() / 60,
		hourBucket:   base.Unix() / 3600,
		secondCount:  SecondLimit,
		minuteCount:  MinuteLimit,
		hourCount:    HourLimit,
	})

	nextHour := base.Add(time.Second)
	if waitFor, err := limiter.acquireAt(ctx, nextHour); err != nil {
		t.Fatalf("acquire at hour boundary failed: %v", err)
	} else if waitFor != 0 {
		t.Fatalf("expected allowed acquire at boundary, wait=%v", waitFor)
	}

	state := fetchState(t, ctx, pool, limiter.provider)
	if state.secondCount != 1 || state.minuteCount != 1 || state.hourCount != 1 {
		t.Fatalf("expected counters reset and increment to 1 at boundary, got second=%d minute=%d hour=%d", state.secondCount, state.minuteCount, state.hourCount)
	}
}

func TestAcquireAt_ConcurrencySafety(t *testing.T) {
	ctx, pool, limiter := setupLimiterTest(t)
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	const workers = 32
	var allowed int32
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			waitFor, err := limiter.acquireAt(ctx, now)
			if err != nil {
				errCh <- err
				return
			}
			if waitFor == 0 {
				atomic.AddInt32(&allowed, 1)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected concurrency error: %v", err)
	}

	if allowed != 2 {
		t.Fatalf("expected exactly 2 allowed acquires in same second, got %d", allowed)
	}

	state := fetchState(t, ctx, pool, limiter.provider)
	if state.secondCount != 2 {
		t.Fatalf("expected second_count=2 after concurrent acquires, got %d", state.secondCount)
	}
}

func TestAcquireAt_RestartPersistence(t *testing.T) {
	ctx, pool, limiter1 := setupLimiterTest(t)
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	limiter2 := NewPersistentLimiter(pool)
	limiter2.provider = limiter1.provider

	if waitFor, err := limiter1.acquireAt(ctx, now); err != nil || waitFor != 0 {
		t.Fatalf("first acquire failed, wait=%v err=%v", waitFor, err)
	}
	if waitFor, err := limiter1.acquireAt(ctx, now); err != nil || waitFor != 0 {
		t.Fatalf("second acquire failed, wait=%v err=%v", waitFor, err)
	}

	if waitFor, err := limiter2.acquireAt(ctx, now); err != nil {
		t.Fatalf("restart acquire failed: %v", err)
	} else if waitFor <= 0 {
		t.Fatalf("expected blocked acquire after restart due to persisted state, wait=%v", waitFor)
	}
}

func setupLimiterTest(t *testing.T) (context.Context, *pgxpool.Pool, *PersistentLimiter) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	limiter := NewPersistentLimiter(pool)
	limiter.provider = sanitizeProviderName(t.Name())

	_, err = pool.Exec(ctx, `DELETE FROM rate_limit_state WHERE provider = $1`, limiter.provider)
	if err != nil {
		t.Fatalf("cleanup limiter state: %v", err)
	}

	return ctx, pool, limiter
}

func seedState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, provider string, state stateRow) {
	t.Helper()

	_, err := pool.Exec(ctx, `
		INSERT INTO rate_limit_state (
			provider,
			window_start,
			window_seconds,
			request_count,
			second_bucket,
			minute_bucket,
			hour_bucket,
			second_count,
			minute_count,
			hour_count
		)
		VALUES ($1, $2, 3600, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (provider)
		DO UPDATE SET
			window_start = EXCLUDED.window_start,
			window_seconds = EXCLUDED.window_seconds,
			request_count = EXCLUDED.request_count,
			second_bucket = EXCLUDED.second_bucket,
			minute_bucket = EXCLUDED.minute_bucket,
			hour_bucket = EXCLUDED.hour_bucket,
			second_count = EXCLUDED.second_count,
			minute_count = EXCLUDED.minute_count,
			hour_count = EXCLUDED.hour_count,
			updated_at = NOW()
	`,
		provider,
		time.Unix(state.hourBucket*3600, 0).UTC(),
		state.hourCount,
		state.secondBucket,
		state.minuteBucket,
		state.hourBucket,
		state.secondCount,
		state.minuteCount,
		state.hourCount,
	)
	if err != nil {
		t.Fatalf("seed limiter state: %v", err)
	}
}

func fetchState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, provider string) stateRow {
	t.Helper()

	var state stateRow
	err := pool.QueryRow(ctx, `
		SELECT second_bucket, minute_bucket, hour_bucket, second_count, minute_count, hour_count
		FROM rate_limit_state
		WHERE provider = $1
	`, provider).Scan(
		&state.secondBucket,
		&state.minuteBucket,
		&state.hourBucket,
		&state.secondCount,
		&state.minuteCount,
		&state.hourCount,
	)
	if err != nil {
		t.Fatalf("fetch limiter state: %v", err)
	}

	return state
}

func sanitizeProviderName(in string) string {
	replacer := strings.NewReplacer("/", "_", " ", "_", "-", "_")
	return "test_" + replacer.Replace(strings.ToLower(in))
}
