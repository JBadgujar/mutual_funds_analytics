package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	SecondLimit = int32(2)
	MinuteLimit = int32(50)
	HourLimit   = int32(300)

	defaultProvider = "mfapi"
)

type PersistentLimiter struct {
	pool     *pgxpool.Pool
	provider string
	clock    func() time.Time
	waitFn   func(context.Context, time.Duration) error
}

type stateRow struct {
	secondBucket int64
	minuteBucket int64
	hourBucket   int64
	secondCount  int32
	minuteCount  int32
	hourCount    int32
}

func NewPersistentLimiter(pool *pgxpool.Pool) *PersistentLimiter {
	return &PersistentLimiter{
		pool:     pool,
		provider: defaultProvider,
		clock:    time.Now,
		waitFn:   waitWithContext,
	}
}

func (l *PersistentLimiter) Acquire(ctx context.Context) error {
	for {
		now := l.clock().UTC()
		waitFor, err := l.acquireAt(ctx, now)
		if err != nil {
			return err
		}

		if waitFor <= 0 {
			return nil
		}

		if err := l.waitFn(ctx, waitFor); err != nil {
			return err
		}
	}
}

func (l *PersistentLimiter) acquireAt(ctx context.Context, now time.Time) (time.Duration, error) {
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin limiter transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := l.ensureRow(ctx, tx, now); err != nil {
		return 0, err
	}

	state, err := l.lockState(ctx, tx)
	if err != nil {
		return 0, err
	}

	state = rollBuckets(state, now)
	waitFor := calculateWaitDuration(state, now)
	if waitFor <= 0 {
		state.secondCount++
		state.minuteCount++
		state.hourCount++
	}

	if err := l.persistState(ctx, tx, state); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit limiter transaction: %w", err)
	}

	return waitFor, nil
}

func (l *PersistentLimiter) ensureRow(ctx context.Context, tx pgx.Tx, now time.Time) error {
	secondBucket := now.Unix()
	minuteBucket := secondBucket / 60
	hourBucket := secondBucket / 3600

	const q = `
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
		VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, 0)
		ON CONFLICT (provider) DO NOTHING
	`

	_, err := tx.Exec(ctx, q,
		l.provider,
		time.Unix(hourBucket*3600, 0).UTC(),
		int32(3600),
		int32(0),
		secondBucket,
		minuteBucket,
		hourBucket,
	)
	if err != nil {
		return fmt.Errorf("ensure limiter row: %w", err)
	}

	return nil
}

func (l *PersistentLimiter) lockState(ctx context.Context, tx pgx.Tx) (stateRow, error) {
	const q = `
		SELECT second_bucket, minute_bucket, hour_bucket, second_count, minute_count, hour_count
		FROM rate_limit_state
		WHERE provider = $1
		FOR UPDATE
	`

	var state stateRow
	err := tx.QueryRow(ctx, q, l.provider).Scan(
		&state.secondBucket,
		&state.minuteBucket,
		&state.hourBucket,
		&state.secondCount,
		&state.minuteCount,
		&state.hourCount,
	)
	if err != nil {
		return stateRow{}, fmt.Errorf("lock limiter row: %w", err)
	}

	return state, nil
}

func (l *PersistentLimiter) persistState(ctx context.Context, tx pgx.Tx, state stateRow) error {
	const q = `
		UPDATE rate_limit_state
		SET
			window_start = $2,
			window_seconds = $3,
			request_count = $4,
			second_bucket = $5,
			minute_bucket = $6,
			hour_bucket = $7,
			second_count = $8,
			minute_count = $9,
			hour_count = $10,
			updated_at = NOW()
		WHERE provider = $1
	`

	_, err := tx.Exec(ctx, q,
		l.provider,
		time.Unix(state.hourBucket*3600, 0).UTC(),
		int32(3600),
		state.hourCount,
		state.secondBucket,
		state.minuteBucket,
		state.hourBucket,
		state.secondCount,
		state.minuteCount,
		state.hourCount,
	)
	if err != nil {
		return fmt.Errorf("persist limiter state: %w", err)
	}

	return nil
}

func rollBuckets(state stateRow, now time.Time) stateRow {
	secondBucket := now.Unix()
	minuteBucket := secondBucket / 60
	hourBucket := secondBucket / 3600

	if state.secondBucket != secondBucket {
		state.secondBucket = secondBucket
		state.secondCount = 0
	}
	if state.minuteBucket != minuteBucket {
		state.minuteBucket = minuteBucket
		state.minuteCount = 0
	}
	if state.hourBucket != hourBucket {
		state.hourBucket = hourBucket
		state.hourCount = 0
	}

	return state
}

func calculateWaitDuration(state stateRow, now time.Time) time.Duration {
	var waitFor time.Duration

	setMin := func(candidate time.Duration) {
		if candidate <= 0 {
			candidate = time.Millisecond
		}
		if waitFor == 0 || candidate < waitFor {
			waitFor = candidate
		}
	}

	if state.secondCount >= SecondLimit {
		next := now.Truncate(time.Second).Add(time.Second)
		setMin(next.Sub(now))
	}
	if state.minuteCount >= MinuteLimit {
		next := now.Truncate(time.Minute).Add(time.Minute)
		setMin(next.Sub(now))
	}
	if state.hourCount >= HourLimit {
		next := now.Truncate(time.Hour).Add(time.Hour)
		setMin(next.Sub(now))
	}

	return waitFor
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
