CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_window_median_rank
    ON analytics_snapshot (window_code, rolling_return_median DESC)
    WHERE insufficient_data = FALSE;

CREATE INDEX IF NOT EXISTS idx_analytics_snapshot_window_drawdown_rank
    ON analytics_snapshot (window_code, max_drawdown_decline_pct ASC)
    WHERE insufficient_data = FALSE;
