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

func (r *PostgresNavRepository) ListByFundID(ctx context.Context, fundID int64) ([]domain.NAVHistory, error) {
	const q = `
		SELECT fund_id, nav_date, nav, source, created_at, updated_at
		FROM nav_history
		WHERE fund_id = $1
		ORDER BY nav_date ASC
	`

	rows, err := r.db.Query(ctx, q, fundID)
	if err != nil {
		return nil, fmt.Errorf("list nav history by fund id: %w", err)
	}
	defer rows.Close()

	out := make([]domain.NAVHistory, 0)
	for rows.Next() {
		nav, scanErr := scanNAVHistory(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, nav)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate nav history by fund id: %w", rows.Err())
	}

	return out, nil
}

func (r *PostgresAnalyticsRepository) Upsert(ctx context.Context, snapshot domain.AnalyticsSnapshot) error {
	const q = `
		INSERT INTO analytics_snapshot (
			fund_id,
			window_code,
			as_of_date,
			start_date,
			end_date,
			total_days,
			nav_data_points,
			insufficient_data,
			rolling_return_min,
			rolling_return_max,
			rolling_return_median,
			rolling_return_p25,
			rolling_return_p75,
			max_drawdown_decline_pct,
			max_drawdown_peak_date,
			max_drawdown_trough_date,
			cagr_min,
			cagr_max,
			cagr_median,
			annualized_volatility
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		ON CONFLICT (fund_id, window_code)
		DO UPDATE SET
			as_of_date = EXCLUDED.as_of_date,
			start_date = EXCLUDED.start_date,
			end_date = EXCLUDED.end_date,
			total_days = EXCLUDED.total_days,
			nav_data_points = EXCLUDED.nav_data_points,
			insufficient_data = EXCLUDED.insufficient_data,
			rolling_return_min = EXCLUDED.rolling_return_min,
			rolling_return_max = EXCLUDED.rolling_return_max,
			rolling_return_median = EXCLUDED.rolling_return_median,
			rolling_return_p25 = EXCLUDED.rolling_return_p25,
			rolling_return_p75 = EXCLUDED.rolling_return_p75,
			max_drawdown_decline_pct = EXCLUDED.max_drawdown_decline_pct,
			max_drawdown_peak_date = EXCLUDED.max_drawdown_peak_date,
			max_drawdown_trough_date = EXCLUDED.max_drawdown_trough_date,
			cagr_min = EXCLUDED.cagr_min,
			cagr_max = EXCLUDED.cagr_max,
			cagr_median = EXCLUDED.cagr_median,
			annualized_volatility = EXCLUDED.annualized_volatility,
			updated_at = NOW()
	`

	_, err := r.db.Exec(ctx, q,
		snapshot.FundID,
		snapshot.WindowCode,
		snapshot.AsOfDate,
		snapshot.StartDate,
		snapshot.EndDate,
		snapshot.TotalDays,
		snapshot.NAVDataPoints,
		snapshot.InsufficientData,
		snapshot.RollingReturnMin,
		snapshot.RollingReturnMax,
		snapshot.RollingReturnMedian,
		snapshot.RollingReturnP25,
		snapshot.RollingReturnP75,
		snapshot.MaxDrawdownDeclinePct,
		snapshot.MaxDrawdownPeakDate,
		snapshot.MaxDrawdownTroughDate,
		snapshot.CAGRMin,
		snapshot.CAGRMax,
		snapshot.CAGRMedian,
		snapshot.AnnualizedVolatility,
	)
	if err != nil {
		return fmt.Errorf("upsert analytics snapshot: %w", err)
	}

	return nil
}

func (r *PostgresAnalyticsRepository) GetByFundAndWindow(ctx context.Context, fundID int64, windowCode string) (domain.AnalyticsSnapshot, error) {
	const q = `
		SELECT
			fund_id,
			window_code,
			as_of_date,
			start_date,
			end_date,
			total_days,
			nav_data_points,
			insufficient_data,
			rolling_return_min,
			rolling_return_max,
			rolling_return_median,
			rolling_return_p25,
			rolling_return_p75,
			max_drawdown_decline_pct,
			max_drawdown_peak_date,
			max_drawdown_trough_date,
			cagr_min,
			cagr_max,
			cagr_median,
			annualized_volatility,
			created_at,
			updated_at
		FROM analytics_snapshot
		WHERE fund_id = $1 AND window_code = $2
	`

	row := r.db.QueryRow(ctx, q, fundID, windowCode)
	return scanAnalyticsSnapshot(row)
}

func (r *PostgresAnalyticsRepository) ListByWindow(ctx context.Context, windowCode string, asOfDate time.Time, limit int32) ([]domain.AnalyticsSnapshot, error) {
	if limit <= 0 {
		limit = 20
	}

	const q = `
		SELECT
			fund_id,
			window_code,
			as_of_date,
			start_date,
			end_date,
			total_days,
			nav_data_points,
			insufficient_data,
			rolling_return_min,
			rolling_return_max,
			rolling_return_median,
			rolling_return_p25,
			rolling_return_p75,
			max_drawdown_decline_pct,
			max_drawdown_peak_date,
			max_drawdown_trough_date,
			cagr_min,
			cagr_max,
			cagr_median,
			annualized_volatility,
			created_at,
			updated_at
		FROM analytics_snapshot
		WHERE window_code = $1 AND as_of_date = $2
		ORDER BY rolling_return_median DESC
		LIMIT $3
	`

	rows, err := r.db.Query(ctx, q, windowCode, asOfDate, limit)
	if err != nil {
		return nil, fmt.Errorf("query analytics by window: %w", err)
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
		return nil, fmt.Errorf("iterate analytics by window: %w", rows.Err())
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
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
	`

	_, err := r.db.Exec(ctx, q,
		state.Provider,
		state.WindowStart,
		state.WindowSeconds,
		state.RequestCount,
		state.SecondBucket,
		state.MinuteBucket,
		state.HourBucket,
		state.SecondCount,
		state.MinuteCount,
		state.HourCount,
	)
	if err != nil {
		return fmt.Errorf("upsert rate limit state: %w", err)
	}

	return nil
}

func (r *PostgresRateLimitStateRepository) Get(ctx context.Context, provider string) (domain.RateLimitState, error) {
	const q = `
		SELECT
			provider,
			window_start,
			window_seconds,
			request_count,
			second_bucket,
			minute_bucket,
			hour_bucket,
			second_count,
			minute_count,
			hour_count,
			updated_at
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
	var startDate sql.NullTime
	var endDate sql.NullTime
	var peakDate sql.NullTime
	var troughDate sql.NullTime
	if err := row.Scan(
		&out.FundID,
		&out.WindowCode,
		&out.AsOfDate,
		&startDate,
		&endDate,
		&out.TotalDays,
		&out.NAVDataPoints,
		&out.InsufficientData,
		&out.RollingReturnMin,
		&out.RollingReturnMax,
		&out.RollingReturnMedian,
		&out.RollingReturnP25,
		&out.RollingReturnP75,
		&out.MaxDrawdownDeclinePct,
		&peakDate,
		&troughDate,
		&out.CAGRMin,
		&out.CAGRMax,
		&out.CAGRMedian,
		&out.AnnualizedVolatility,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		return domain.AnalyticsSnapshot{}, fmt.Errorf("scan analytics snapshot: %w", err)
	}

	if startDate.Valid {
		value := startDate.Time
		out.StartDate = &value
	}
	if endDate.Valid {
		value := endDate.Time
		out.EndDate = &value
	}
	if peakDate.Valid {
		value := peakDate.Time
		out.MaxDrawdownPeakDate = &value
	}
	if troughDate.Valid {
		value := troughDate.Time
		out.MaxDrawdownTroughDate = &value
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
		&out.SecondBucket,
		&out.MinuteBucket,
		&out.HourBucket,
		&out.SecondCount,
		&out.MinuteCount,
		&out.HourCount,
		&out.UpdatedAt,
	); err != nil {
		return domain.RateLimitState{}, fmt.Errorf("scan rate limit state: %w", err)
	}

	return out, nil
}
