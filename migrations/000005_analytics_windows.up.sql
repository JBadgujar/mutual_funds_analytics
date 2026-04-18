ALTER TABLE analytics_snapshot
    ADD COLUMN IF NOT EXISTS window_code TEXT NOT NULL DEFAULT '1Y',
    ADD COLUMN IF NOT EXISTS start_date DATE,
    ADD COLUMN IF NOT EXISTS end_date DATE,
    ADD COLUMN IF NOT EXISTS total_days INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS nav_data_points INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS insufficient_data BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS rolling_return_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rolling_return_max DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rolling_return_median DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rolling_return_p25 DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rolling_return_p75 DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_drawdown_decline_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_drawdown_peak_date DATE,
    ADD COLUMN IF NOT EXISTS max_drawdown_trough_date DATE,
    ADD COLUMN IF NOT EXISTS cagr_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_max DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_median DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS annualized_volatility DOUBLE PRECISION NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'analytics_snapshot_window_code_check'
    ) THEN
        ALTER TABLE analytics_snapshot
            ADD CONSTRAINT analytics_snapshot_window_code_check
            CHECK (window_code IN ('1Y', '3Y', '5Y', '10Y'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'analytics_snapshot_total_days_non_negative'
    ) THEN
        ALTER TABLE analytics_snapshot
            ADD CONSTRAINT analytics_snapshot_total_days_non_negative CHECK (total_days >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'analytics_snapshot_nav_data_points_non_negative'
    ) THEN
        ALTER TABLE analytics_snapshot
            ADD CONSTRAINT analytics_snapshot_nav_data_points_non_negative CHECK (nav_data_points >= 0);
    END IF;
END $$;

WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (PARTITION BY fund_id, window_code ORDER BY as_of_date DESC, updated_at DESC, created_at DESC) AS rn
    FROM analytics_snapshot
)
DELETE FROM analytics_snapshot a
USING ranked r
WHERE a.ctid = r.ctid AND r.rn > 1;

ALTER TABLE analytics_snapshot
    DROP CONSTRAINT IF EXISTS analytics_snapshot_pkey;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'analytics_snapshot_pkey'
          AND conrelid = 'analytics_snapshot'::regclass
    ) THEN
        ALTER TABLE analytics_snapshot
            ADD CONSTRAINT analytics_snapshot_pkey PRIMARY KEY (fund_id, window_code);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_as_of_date
ON analytics_snapshot (as_of_date DESC);

CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_window_rank
ON analytics_snapshot (window_code, as_of_date DESC, rolling_return_median DESC)
WHERE insufficient_data = FALSE;
