DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'sync_runs_status_check'
    ) THEN
        ALTER TABLE sync_runs
            DROP CONSTRAINT sync_runs_status_check;
    END IF;

    ALTER TABLE sync_runs
        ADD CONSTRAINT sync_runs_status_check CHECK (status IN ('queued', 'running', 'success', 'failed', 'partial'));
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_runs_active_singleton
ON sync_runs ((1))
WHERE status IN ('queued', 'running');
