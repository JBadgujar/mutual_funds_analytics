ALTER TABLE rate_limit_state
    ADD COLUMN IF NOT EXISTS second_bucket BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS minute_bucket BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hour_bucket BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS second_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS minute_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hour_count INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'rate_limit_second_count_range'
    ) THEN
        ALTER TABLE rate_limit_state
            ADD CONSTRAINT rate_limit_second_count_range CHECK (second_count BETWEEN 0 AND 2);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'rate_limit_minute_count_range'
    ) THEN
        ALTER TABLE rate_limit_state
            ADD CONSTRAINT rate_limit_minute_count_range CHECK (minute_count BETWEEN 0 AND 50);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'rate_limit_hour_count_range'
    ) THEN
        ALTER TABLE rate_limit_state
            ADD CONSTRAINT rate_limit_hour_count_range CHECK (hour_count BETWEEN 0 AND 300);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_rate_limit_state_hour_bucket ON rate_limit_state (hour_bucket);
