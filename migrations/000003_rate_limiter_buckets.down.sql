DROP INDEX IF EXISTS idx_rate_limit_state_hour_bucket;

ALTER TABLE IF EXISTS rate_limit_state
    DROP CONSTRAINT IF EXISTS rate_limit_hour_count_range,
    DROP CONSTRAINT IF EXISTS rate_limit_minute_count_range,
    DROP CONSTRAINT IF EXISTS rate_limit_second_count_range,
    DROP COLUMN IF EXISTS hour_count,
    DROP COLUMN IF EXISTS minute_count,
    DROP COLUMN IF EXISTS second_count,
    DROP COLUMN IF EXISTS hour_bucket,
    DROP COLUMN IF EXISTS minute_bucket,
    DROP COLUMN IF EXISTS second_bucket;
