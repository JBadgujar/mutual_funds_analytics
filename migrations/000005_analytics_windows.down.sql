DROP INDEX IF EXISTS idx_analytics_snapshot_window_rank;
DROP INDEX IF EXISTS idx_analytics_snapshot_as_of_date;

ALTER TABLE IF EXISTS analytics_snapshot
    DROP CONSTRAINT IF EXISTS analytics_snapshot_pkey;

ALTER TABLE IF EXISTS analytics_snapshot
    ADD CONSTRAINT analytics_snapshot_pkey PRIMARY KEY (fund_id, as_of_date);

ALTER TABLE IF EXISTS analytics_snapshot
    DROP CONSTRAINT IF EXISTS analytics_snapshot_nav_data_points_non_negative,
    DROP CONSTRAINT IF EXISTS analytics_snapshot_total_days_non_negative,
    DROP CONSTRAINT IF EXISTS analytics_snapshot_window_code_check,
    DROP COLUMN IF EXISTS annualized_volatility,
    DROP COLUMN IF EXISTS cagr_median,
    DROP COLUMN IF EXISTS cagr_max,
    DROP COLUMN IF EXISTS cagr_min,
    DROP COLUMN IF EXISTS max_drawdown_trough_date,
    DROP COLUMN IF EXISTS max_drawdown_peak_date,
    DROP COLUMN IF EXISTS max_drawdown_decline_pct,
    DROP COLUMN IF EXISTS rolling_return_p75,
    DROP COLUMN IF EXISTS rolling_return_p25,
    DROP COLUMN IF EXISTS rolling_return_median,
    DROP COLUMN IF EXISTS rolling_return_max,
    DROP COLUMN IF EXISTS rolling_return_min,
    DROP COLUMN IF EXISTS insufficient_data,
    DROP COLUMN IF EXISTS nav_data_points,
    DROP COLUMN IF EXISTS total_days,
    DROP COLUMN IF EXISTS end_date,
    DROP COLUMN IF EXISTS start_date,
    DROP COLUMN IF EXISTS window_code;
