package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"mutual-fund-analytics/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type PostgresFundRepository struct {
	db DBTX
}

type PostgresNavRepository struct {
	db DBTX
}

type PostgresAnalyticsRepository struct {
	db DBTX
}

type PostgresSyncRepository struct {
	db DBTX
}

type PostgresRateLimitStateRepository struct {
	db DBTX
}

var _ domain.FundRepository = (*PostgresFundRepository)(nil)
var _ domain.NavRepository = (*PostgresNavRepository)(nil)
var _ domain.AnalyticsRepository = (*PostgresAnalyticsRepository)(nil)
var _ domain.SyncRepository = (*PostgresSyncRepository)(nil)
var _ domain.RateLimitStateRepository = (*PostgresRateLimitStateRepository)(nil)

func NewFundRepository(db DBTX) domain.FundRepository {
	return &PostgresFundRepository{db: db}
}

func NewNavRepository(db DBTX) domain.NavRepository {
	return &PostgresNavRepository{db: db}
}

func NewAnalyticsRepository(db DBTX) domain.AnalyticsRepository {
	return &PostgresAnalyticsRepository{db: db}
}

func NewSyncRepository(db DBTX) domain.SyncRepository {
	return &PostgresSyncRepository{db: db}
}

func NewRateLimitStateRepository(db DBTX) domain.RateLimitStateRepository {
	return &PostgresRateLimitStateRepository{db: db}
}

func (r *PostgresFundRepository) Upsert(ctx context.Context, fund domain.Fund) (domain.Fund, error) {
	const q = `
		INSERT INTO funds (scheme_code, name, category, isin, active)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (scheme_code)
		DO UPDATE SET
			name = EXCLUDED.name,
			category = EXCLUDED.category,
			isin = EXCLUDED.isin,
			active = EXCLUDED.active,
			updated_at = NOW()
		RETURNING id, scheme_code, name, category, isin, active, created_at, updated_at
	`

	row := r.db.QueryRow(ctx, q, fund.SchemeCode, fund.Name, fund.Category, fund.ISIN, fund.Active)
	return scanFund(row)
}

func (r *PostgresFundRepository) GetBySchemeCode(ctx context.Context, schemeCode string) (domain.Fund, error) {
	const q = `
		SELECT id, scheme_code, name, category, isin, active, created_at, updated_at
		FROM funds
		WHERE scheme_code = $1
	`

	row := r.db.QueryRow(ctx, q, schemeCode)
	return scanFund(row)
}

func (r *PostgresFundRepository) ListActive(ctx context.Context, limit, offset int32) ([]domain.Fund, error) {
	if limit <= 0 {
		limit = 100
	}

	const q = `
		SELECT id, scheme_code, name, category, isin, active, created_at, updated_at
		FROM funds
		WHERE active = TRUE
		ORDER BY name ASC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query active funds: %w", err)
	}
	defer rows.Close()

	out := make([]domain.Fund, 0, limit)
	for rows.Next() {
		fund, scanErr := scanFund(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, fund)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate active funds: %w", rows.Err())
	}

	return out, nil
}

func (r *PostgresNavRepository) Upsert(ctx context.Context, nav domain.NAVHistory) error {
	const q = `
		INSERT INTO nav_history (fund_id, nav_date, nav, source)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (fund_id, nav_date)
		DO UPDATE SET
			nav = EXCLUDED.nav,
			source = EXCLUDED.source,
			updated_at = NOW()
	`

	_, err := r.db.Exec(ctx, q, nav.FundID, nav.NAVDate, nav.NAV, nav.Source)
	if err != nil {
		return fmt.Errorf("upsert nav history: %w", err)
	}

	return nil
}

func (r *PostgresNavRepository) GetByDate(ctx context.Context, fundID int64, navDate time.Time) (domain.NAVHistory, error) {
	const q = `
		SELECT fund_id, nav_date, nav, source, created_at, updated_at
		FROM nav_history
		WHERE fund_id = $1 AND nav_date = $2
	`

	row := r.db.QueryRow(ctx, q, fundID, navDate)
	return scanNAVHistory(row)
}

func (r *PostgresNavRepository) GetLatestByFundID(ctx context.Context, fundID int64, limit int32) ([]domain.NAVHistory, error) {
	if limit <= 0 {
		limit = 90
	}

	const q = `
		SELECT fund_id, nav_date, nav, source, created_at, updated_at
		FROM nav_history
		WHERE fund_id = $1
		ORDER BY nav_date DESC
		LIMIT $2
	`

	rows, err := r.db.Query(ctx, q, fundID, limit)
	if err != nil {
		return nil, fmt.Errorf("query nav history: %w", err)
	}
	defer rows.Close()

	out := make([]domain.NAVHistory, 0, limit)
	for rows.Next() {
		nav, scanErr := scanNAVHistory(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, nav)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate nav history: %w", rows.Err())
	}

	return out, nil
}

func (r *PostgresAnalyticsRepository) Upsert(ctx context.Context, snapshot domain.AnalyticsSnapshot) error {
	const q = `
		INSERT INTO analytics_snapshot (
			fund_id,
			as_of_date,
			return_1y,
			return_3y,
			return_5y,
			volatility_1y,
			sharpe_ratio,
			expense_ratio
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (fund_id, as_of_date)
		DO UPDATE SET
			return_1y = EXCLUDED.return_1y,
			return_3y = EXCLUDED.return_3y,
			return_5y = EXCLUDED.return_5y,
			volatility_1y = EXCLUDED.volatility_1y,
			sharpe_ratio = EXCLUDED.sharpe_ratio,
			expense_ratio = EXCLUDED.expense_ratio,
			updated_at = NOW()
	`

	_, err := r.db.Exec(ctx, q,
		snapshot.FundID,
		snapshot.AsOfDate,
		snapshot.Return1Y,
		snapshot.Return3Y,
		snapshot.Return5Y,
		snapshot.Volatility1Y,
		snapshot.SharpeRatio,
		snapshot.ExpenseRatio,
	)
	if err != nil {
		return fmt.Errorf("upsert analytics snapshot: %w", err)
	}

	return nil
}

func (r *PostgresAnalyticsRepository) GetLatestForFund(ctx context.Context, fundID int64) (domain.AnalyticsSnapshot, error) {
	const q = `
		SELECT
			fund_id,
			as_of_date,
			return_1y,
			return_3y,
			return_5y,
			volatility_1y,
			sharpe_ratio,
			expense_ratio,
			created_at,
			updated_at
		FROM analytics_snapshot
		WHERE fund_id = $1
		ORDER BY as_of_date DESC
		LIMIT 1
	`

	row := r.db.QueryRow(ctx, q, fundID)
	return scanAnalyticsSnapshot(row)
}

func (r *PostgresAnalyticsRepository) TopByReturn1Y(ctx context.Context, asOfDate time.Time, limit int32) ([]domain.AnalyticsSnapshot, error) {
	if limit <= 0 {
		limit = 20
	}

	const q = `
		SELECT
			fund_id,
			as_of_date,
			return_1y,
			return_3y,
			return_5y,
			volatility_1y,
			sharpe_ratio,
			expense_ratio,
			created_at,
			updated_at
		FROM analytics_snapshot
		WHERE as_of_date = $1
		ORDER BY return_1y DESC
		LIMIT $2
	`

	rows, err := r.db.Query(ctx, q, asOfDate, limit)
	if err != nil {
		return nil, fmt.Errorf("query top analytics: %w", err)
	}
	defer rows.Close()

	out := make([]domain.AnalyticsSnapshot, 0, limit)
	for rows.Next() {
		snapshot, scanErr := scanAnalyticsSnapshot(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, snapshot)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate top analytics: %w", rows.Err())
	}

	return out, nil
}

func (r *PostgresSyncRepository) StartRun(ctx context.Context, triggeredBy string) (domain.SyncRun, error) {
	const q = `
		INSERT INTO sync_runs (status, triggered_by)
		VALUES ('running', $1)
		RETURNING id, started_at, completed_at, status, triggered_by, records_processed, error_message, updated_at
	`

	row := r.db.QueryRow(ctx, q, triggeredBy)
	return scanSyncRun(row)
}

func (r *PostgresSyncRepository) CompleteRun(ctx context.Context, runID int64, status string, recordsProcessed int32, errorMessage string) error {
	const q = `
		UPDATE sync_runs
		SET
			status = $2,
			records_processed = $3,
			error_message = $4,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`

	_, err := r.db.Exec(ctx, q, runID, status, recordsProcessed, errorMessage)
	if err != nil {
		return fmt.Errorf("complete sync run: %w", err)
	}

	return nil
}

func (r *PostgresSyncRepository) UpsertFundState(ctx context.Context, state domain.SyncFundState) error {
	const q = `
		INSERT INTO sync_fund_state (
			fund_id,
			last_synced_at,
			last_nav_date,
			status,
			retry_count,
			next_retry_at,
			last_error,
			last_run_id,
			consecutive_ok
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (fund_id)
		DO UPDATE SET
			last_synced_at = EXCLUDED.last_synced_at,
			last_nav_date = EXCLUDED.last_nav_date,
			status = EXCLUDED.status,
			retry_count = EXCLUDED.retry_count,
			next_retry_at = EXCLUDED.next_retry_at,
			last_error = EXCLUDED.last_error,
			last_run_id = EXCLUDED.last_run_id,
			consecutive_ok = EXCLUDED.consecutive_ok,
			updated_at = NOW()
	`

	_, err := r.db.Exec(ctx, q,
		state.FundID,
		state.LastSyncedAt,
		state.LastNAVDate,
		state.Status,
		state.RetryCount,
		state.NextRetryAt,
		state.LastError,
		state.LastRunID,
		state.ConsecutiveOK,
	)
	if err != nil {
		return fmt.Errorf("upsert sync fund state: %w", err)
	}

	return nil
}

func (r *PostgresSyncRepository) ListPendingFundStates(ctx context.Context, now time.Time, limit int32) ([]domain.SyncFundState, error) {
	if limit <= 0 {
		limit = 100
	}

	const q = `
		SELECT
			fund_id,
			last_synced_at,
			last_nav_date,
			status,
			retry_count,
			next_retry_at,
			last_error,
			updated_at,
			last_run_id,
			consecutive_ok
		FROM sync_fund_state
		WHERE status IN ('pending', 'failed')
		  AND (next_retry_at IS NULL OR next_retry_at <= $1)
		ORDER BY next_retry_at NULLS FIRST, fund_id ASC
		LIMIT $2
	`

	rows, err := r.db.Query(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending sync fund states: %w", err)
	}
	defer rows.Close()

	out := make([]domain.SyncFundState, 0, limit)
	for rows.Next() {
		state, scanErr := scanSyncFundState(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, state)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate pending sync fund states: %w", rows.Err())
	}

	return out, nil
}

func (r *PostgresRateLimitStateRepository) Upsert(ctx context.Context, state domain.RateLimitState) error {
	const q = `
		INSERT INTO rate_limit_state (provider, window_start, window_seconds, request_count)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (provider)
		DO UPDATE SET
			window_start = EXCLUDED.window_start,
			window_seconds = EXCLUDED.window_seconds,
			request_count = EXCLUDED.request_count,
			updated_at = NOW()
	`

	_, err := r.db.Exec(ctx, q, state.Provider, state.WindowStart, state.WindowSeconds, state.RequestCount)
	if err != nil {
		return fmt.Errorf("upsert rate limit state: %w", err)
	}

	return nil
}

func (r *PostgresRateLimitStateRepository) Get(ctx context.Context, provider string) (domain.RateLimitState, error) {
	const q = `
		SELECT provider, window_start, window_seconds, request_count, updated_at
		FROM rate_limit_state
		WHERE provider = $1
	`

	row := r.db.QueryRow(ctx, q, provider)
	return scanRateLimitState(row)
}

func scanFund(row interface{ Scan(dest ...any) error }) (domain.Fund, error) {
	var out domain.Fund
	if err := row.Scan(
		&out.ID,
		&out.SchemeCode,
		&out.Name,
		&out.Category,
		&out.ISIN,
		&out.Active,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return domain.Fund{}, fmt.Errorf("scan fund: %w", err)
	}

	return out, nil
}

func scanNAVHistory(row interface{ Scan(dest ...any) error }) (domain.NAVHistory, error) {
	var out domain.NAVHistory
	if err := row.Scan(
		&out.FundID,
		&out.NAVDate,
		&out.NAV,
		&out.Source,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return domain.NAVHistory{}, fmt.Errorf("scan nav history: %w", err)
	}

	return out, nil
}

func scanAnalyticsSnapshot(row interface{ Scan(dest ...any) error }) (domain.AnalyticsSnapshot, error) {
	var out domain.AnalyticsSnapshot
	if err := row.Scan(
		&out.FundID,
		&out.AsOfDate,
		&out.Return1Y,
		&out.Return3Y,
		&out.Return5Y,
		&out.Volatility1Y,
		&out.SharpeRatio,
		&out.ExpenseRatio,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return domain.AnalyticsSnapshot{}, fmt.Errorf("scan analytics snapshot: %w", err)
	}

	return out, nil
}

func scanSyncRun(row interface{ Scan(dest ...any) error }) (domain.SyncRun, error) {
	var out domain.SyncRun
	var completedAt sql.NullTime
	if err := row.Scan(
		&out.ID,
		&out.StartedAt,
		&completedAt,
		&out.Status,
		&out.TriggeredBy,
		&out.RecordsProcessed,
		&out.ErrorMessage,
		&out.UpdatedAt,
	); err != nil {
		return domain.SyncRun{}, fmt.Errorf("scan sync run: %w", err)
	}

	if completedAt.Valid {
		value := completedAt.Time
		out.CompletedAt = &value
	}

	return out, nil
}

func scanSyncFundState(row interface{ Scan(dest ...any) error }) (domain.SyncFundState, error) {
	var out domain.SyncFundState
	var lastSyncedAt sql.NullTime
	var lastNAVDate sql.NullTime
	var nextRetryAt sql.NullTime
	var lastRunID sql.NullInt64

	if err := row.Scan(
		&out.FundID,
		&lastSyncedAt,
		&lastNAVDate,
		&out.Status,
		&out.RetryCount,
		&nextRetryAt,
		&out.LastError,
		&out.UpdatedAt,
		&lastRunID,
		&out.ConsecutiveOK,
	); err != nil {
		return domain.SyncFundState{}, fmt.Errorf("scan sync fund state: %w", err)
	}

	if lastSyncedAt.Valid {
		value := lastSyncedAt.Time
		out.LastSyncedAt = &value
	}
	if lastNAVDate.Valid {
		value := lastNAVDate.Time
		out.LastNAVDate = &value
	}
	if nextRetryAt.Valid {
		value := nextRetryAt.Time
		out.NextRetryAt = &value
	}
	if lastRunID.Valid {
		value := lastRunID.Int64
		out.LastRunID = &value
	}

	return out, nil
}

func scanRateLimitState(row interface{ Scan(dest ...any) error }) (domain.RateLimitState, error) {
	var out domain.RateLimitState
	if err := row.Scan(
		&out.Provider,
		&out.WindowStart,
		&out.WindowSeconds,
		&out.RequestCount,
		&out.UpdatedAt,
	); err != nil {
		return domain.RateLimitState{}, fmt.Errorf("scan rate limit state: %w", err)
	}

	return out, nil
}
