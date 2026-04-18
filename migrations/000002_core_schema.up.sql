CREATE TABLE IF NOT EXISTS funds (
    id BIGSERIAL PRIMARY KEY,
    scheme_code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    isin TEXT NOT NULL DEFAULT '',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT funds_scheme_code_not_blank CHECK (BTRIM(scheme_code) <> ''),
    CONSTRAINT funds_name_not_blank CHECK (BTRIM(name) <> '')
);

CREATE TABLE IF NOT EXISTS nav_history (
    fund_id BIGINT NOT NULL REFERENCES funds(id) ON DELETE CASCADE,
    nav_date DATE NOT NULL,
    nav DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL DEFAULT 'mfapi',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fund_id, nav_date),
    CONSTRAINT nav_positive CHECK (nav > 0)
);

CREATE TABLE IF NOT EXISTS analytics_snapshot (
    fund_id BIGINT NOT NULL REFERENCES funds(id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    return_1y DOUBLE PRECISION NOT NULL DEFAULT 0,
    return_3y DOUBLE PRECISION NOT NULL DEFAULT 0,
    return_5y DOUBLE PRECISION NOT NULL DEFAULT 0,
    volatility_1y DOUBLE PRECISION NOT NULL DEFAULT 0,
    sharpe_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    expense_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fund_id, as_of_date),
    CONSTRAINT analytics_expense_ratio_non_negative CHECK (expense_ratio >= 0)
);

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS triggered_by TEXT NOT NULL DEFAULT 'scheduler',
    ADD COLUMN IF NOT EXISTS records_processed INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS error_message TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'sync_runs_status_check'
    ) THEN
        ALTER TABLE sync_runs
            ADD CONSTRAINT sync_runs_status_check CHECK (status IN ('running', 'success', 'failed', 'partial'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'sync_runs_records_processed_non_negative'
    ) THEN
        ALTER TABLE sync_runs
            ADD CONSTRAINT sync_runs_records_processed_non_negative CHECK (records_processed >= 0);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS sync_fund_state (
    fund_id BIGINT PRIMARY KEY REFERENCES funds(id) ON DELETE CASCADE,
    last_synced_at TIMESTAMPTZ,
    last_nav_date DATE,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
    consecutive_ok INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT sync_fund_state_status_check CHECK (status IN ('pending', 'synced', 'failed', 'paused')),
    CONSTRAINT sync_fund_state_retry_count_non_negative CHECK (retry_count >= 0),
    CONSTRAINT sync_fund_state_consecutive_ok_non_negative CHECK (consecutive_ok >= 0)
);

CREATE TABLE IF NOT EXISTS rate_limit_state (
    provider TEXT PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_seconds INTEGER NOT NULL,
    request_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT rate_limit_provider_not_blank CHECK (BTRIM(provider) <> ''),
    CONSTRAINT rate_limit_window_seconds_positive CHECK (window_seconds > 0),
    CONSTRAINT rate_limit_request_count_non_negative CHECK (request_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_funds_active_name ON funds (active, name);
CREATE INDEX IF NOT EXISTS idx_nav_history_fund_date_desc ON nav_history (fund_id, nav_date DESC);
CREATE INDEX IF NOT EXISTS idx_nav_history_date ON nav_history (nav_date DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_as_of_return_1y ON analytics_snapshot (as_of_date DESC, return_1y DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_as_of_return_3y ON analytics_snapshot (as_of_date DESC, return_3y DESC);
CREATE INDEX IF NOT EXISTS idx_sync_runs_status_started ON sync_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_fund_state_status_next_retry ON sync_fund_state (status, next_retry_at NULLS FIRST);
