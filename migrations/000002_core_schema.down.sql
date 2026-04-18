DROP INDEX IF EXISTS idx_sync_fund_state_status_next_retry;
DROP INDEX IF EXISTS idx_sync_runs_status_started;
DROP INDEX IF EXISTS idx_analytics_snapshot_as_of_return_3y;
DROP INDEX IF EXISTS idx_analytics_snapshot_as_of_return_1y;
DROP INDEX IF EXISTS idx_nav_history_date;
DROP INDEX IF EXISTS idx_nav_history_fund_date_desc;
DROP INDEX IF EXISTS idx_funds_active_name;

DROP TABLE IF EXISTS rate_limit_state;
DROP TABLE IF EXISTS sync_fund_state;
DROP TABLE IF EXISTS analytics_snapshot;
DROP TABLE IF EXISTS nav_history;
DROP TABLE IF EXISTS funds;

ALTER TABLE IF EXISTS sync_runs
    DROP CONSTRAINT IF EXISTS sync_runs_records_processed_non_negative,
    DROP CONSTRAINT IF EXISTS sync_runs_status_check,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS error_message,
    DROP COLUMN IF EXISTS records_processed,
    DROP COLUMN IF EXISTS triggered_by;
