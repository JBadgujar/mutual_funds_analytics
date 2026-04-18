package syncer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/mfapi"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	syncAdvisoryLockKey int64 = 90241017
	fetchBatchLimit           = int32(500)
)

const (
	SyncStatusQueued  = "queued"
	SyncStatusRunning = "running"
	SyncStatusSuccess = "success"
	SyncStatusFailed  = "failed"
	SyncStatusPartial = "partial"
)

type SyncMode string

const (
	SyncModeBackfill    SyncMode = "backfill"
	SyncModeIncremental SyncMode = "incremental"
)

var ErrSyncAlreadyRunning = errors.New("sync already running")

type SchemeNAVFetcher interface {
	FetchSchemeNavHistory(ctx context.Context, code string) (mfapi.SchemeNavHistory, error)
}

type Orchestrator struct {
	pool    *pgxpool.Pool
	funds   domain.FundRepository
	navs    domain.NavRepository
	syncs   domain.SyncRepository
	fetcher SchemeNAVFetcher
	nowFn   func() time.Time

	// beforeFundHook is a test seam used to simulate crashes deterministically.
	beforeFundHook func(domain.Fund) error
}

type SyncResult struct {
	RunID            int64
	Mode             SyncMode
	TotalFunds       int
	ProcessedFunds   int
	FailedFunds      int
	InsertedNAVRows  int
	FinalStatus      string
	LifecycleTrace   []string
	LastErrorMessage string
}

func NewOrchestrator(
	pool *pgxpool.Pool,
	funds domain.FundRepository,
	navs domain.NavRepository,
	syncs domain.SyncRepository,
	fetcher SchemeNAVFetcher,
) *Orchestrator {
	return &Orchestrator{
		pool:    pool,
		funds:   funds,
		navs:    navs,
		syncs:   syncs,
		fetcher: fetcher,
		nowFn:   time.Now,
	}
}

func (o *Orchestrator) RunBackfill(ctx context.Context, triggeredBy string) (SyncResult, error) {
	return o.run(ctx, SyncModeBackfill, triggeredBy)
}

func (o *Orchestrator) RunIncremental(ctx context.Context, triggeredBy string) (SyncResult, error) {
	return o.run(ctx, SyncModeIncremental, triggeredBy)
}

func (o *Orchestrator) run(ctx context.Context, mode SyncMode, triggeredBy string) (result SyncResult, runErr error) {
	lockConn, err := o.pool.Acquire(ctx)
	if err != nil {
		return SyncResult{}, fmt.Errorf("acquire db connection for sync lock: %w", err)
	}
	defer lockConn.Release()

	lockAcquired, err := o.tryAcquireLock(ctx, lockConn)
	if err != nil {
		return SyncResult{}, err
	}
	if !lockAcquired {
		return SyncResult{}, ErrSyncAlreadyRunning
	}
	defer o.releaseLock(lockConn)

	if err := o.recoverAbandonedActiveRuns(ctx); err != nil {
		return SyncResult{}, err
	}

	runID, err := o.enqueueRun(ctx, triggeredBy)
	if err != nil {
		return SyncResult{}, err
	}

	result = SyncResult{
		RunID:          runID,
		Mode:           mode,
		LifecycleTrace: []string{SyncStatusQueued},
	}

	if err := o.markRunRunning(ctx, runID); err != nil {
		return result, err
	}
	result.LifecycleTrace = append(result.LifecycleTrace, SyncStatusRunning)

	defer func() {
		finalStatus, errorMessage := finalizeRunStatus(result, runErr)
		result.FinalStatus = finalStatus
		result.LastErrorMessage = errorMessage

		if completeErr := o.syncs.CompleteRun(ctx, runID, finalStatus, int32(result.ProcessedFunds), errorMessage); completeErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("complete sync run: %w", completeErr)
			}
		}
		result.LifecycleTrace = append(result.LifecycleTrace, finalStatus)
	}()

	activeFunds, err := o.listActiveFunds(ctx)
	if err != nil {
		return result, err
	}
	if len(activeFunds) == 0 {
		return result, nil
	}

	fundByID := make(map[int64]domain.Fund, len(activeFunds))
	activeIDs := make([]int64, 0, len(activeFunds))
	for _, fund := range activeFunds {
		fundByID[fund.ID] = fund
		activeIDs = append(activeIDs, fund.ID)
	}

	if err := o.queueFundsForMode(ctx, mode, runID, activeIDs); err != nil {
		return result, err
	}

	pendingStates, err := o.syncs.ListPendingFundStates(ctx, o.nowFn().UTC(), int32(len(activeFunds)*2+1))
	if err != nil {
		return result, fmt.Errorf("list pending fund states: %w", err)
	}

	result.TotalFunds = len(pendingStates)

	for _, state := range pendingStates {
		fund, ok := fundByID[state.FundID]
		if !ok {
			continue
		}

		if o.beforeFundHook != nil {
			if err := o.beforeFundHook(fund); err != nil {
				return result, err
			}
		}

		inserted, latestDate, err := o.syncSingleFund(ctx, mode, fund)
		now := o.nowFn().UTC()
		if err != nil {
			result.FailedFunds++
			retry := state.RetryCount + 1
			nextRetry := now.Add(backoffForRetry(retry))
			if upsertErr := o.syncs.UpsertFundState(ctx, domain.SyncFundState{
				FundID:      fund.ID,
				Status:      "failed",
				RetryCount:  retry,
				NextRetryAt: &nextRetry,
				LastError:   safeError(err),
				LastRunID:   &runID,
			}); upsertErr != nil {
				return result, fmt.Errorf("mark fund failed (%s): %w", fund.SchemeCode, upsertErr)
			}
			continue
		}

		result.ProcessedFunds++
		result.InsertedNAVRows += inserted

		consecutiveOK := state.ConsecutiveOK + 1
		if upsertErr := o.syncs.UpsertFundState(ctx, domain.SyncFundState{
			FundID:        fund.ID,
			LastSyncedAt:  &now,
			LastNAVDate:   latestDate,
			Status:        "synced",
			RetryCount:    0,
			NextRetryAt:   nil,
			LastError:     "",
			LastRunID:     &runID,
			ConsecutiveOK: consecutiveOK,
		}); upsertErr != nil {
			return result, fmt.Errorf("checkpoint synced fund (%s): %w", fund.SchemeCode, upsertErr)
		}
	}

	return result, nil
}

func (o *Orchestrator) tryAcquireLock(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	var ok bool
	err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, syncAdvisoryLockKey).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("acquire advisory lock: %w", err)
	}

	return ok, nil
}

func (o *Orchestrator) releaseLock(conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, syncAdvisoryLockKey)
}

func (o *Orchestrator) recoverAbandonedActiveRuns(ctx context.Context) error {
	const q = `
		UPDATE sync_runs
		SET
			status = 'partial',
			completed_at = NOW(),
			error_message = CASE
				WHEN BTRIM(error_message) = '' THEN 'recovered unfinished run on restart'
				ELSE error_message
			END,
			updated_at = NOW()
		WHERE status IN ('queued', 'running')
	`

	_, err := o.pool.Exec(ctx, q)
	if err != nil {
		return fmt.Errorf("recover abandoned active runs: %w", err)
	}

	return nil
}

func (o *Orchestrator) enqueueRun(ctx context.Context, triggeredBy string) (int64, error) {
	const q = `
		INSERT INTO sync_runs (status, triggered_by)
		VALUES ('queued', $1)
		RETURNING id
	`

	var runID int64
	err := o.pool.QueryRow(ctx, q, strings.TrimSpace(triggeredBy)).Scan(&runID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return 0, ErrSyncAlreadyRunning
		}
		return 0, fmt.Errorf("enqueue sync run: %w", err)
	}

	return runID, nil
}

func (o *Orchestrator) markRunRunning(ctx context.Context, runID int64) error {
	const q = `
		UPDATE sync_runs
		SET status = 'running', updated_at = NOW()
		WHERE id = $1
	`

	_, err := o.pool.Exec(ctx, q, runID)
	if err != nil {
		return fmt.Errorf("mark run running: %w", err)
	}

	return nil
}

func (o *Orchestrator) queueFundsForMode(ctx context.Context, mode SyncMode, runID int64, activeIDs []int64) error {
	if len(activeIDs) == 0 {
		return nil
	}

	pendingBefore, err := o.countPendingFailedForActive(ctx, activeIDs)
	if err != nil {
		return err
	}

	for _, fundID := range activeIDs {
		const insertState = `
			INSERT INTO sync_fund_state (fund_id, status, retry_count, last_error, last_run_id)
			VALUES ($1, 'pending', 0, '', $2)
			ON CONFLICT (fund_id) DO NOTHING
		`

		if _, err := o.pool.Exec(ctx, insertState, fundID, runID); err != nil {
			return fmt.Errorf("ensure sync_fund_state row for fund %d: %w", fundID, err)
		}
	}

	switch mode {
	case SyncModeBackfill:
		if pendingBefore > 0 {
			const q = `
				UPDATE sync_fund_state
				SET last_run_id = $1, updated_at = NOW()
				WHERE fund_id = ANY($2)
				  AND status IN ('pending', 'failed')
			`
			if _, err := o.pool.Exec(ctx, q, runID, activeIDs); err != nil {
				return fmt.Errorf("stamp pending backfill states: %w", err)
			}
			return nil
		}

		const q = `
			UPDATE sync_fund_state
			SET
				status = 'pending',
				next_retry_at = NULL,
				last_run_id = $1,
				updated_at = NOW()
			WHERE fund_id = ANY($2)
			  AND (
				last_nav_date IS NULL
				OR status IN ('pending', 'failed')
			  )
		`
		if _, err := o.pool.Exec(ctx, q, runID, activeIDs); err != nil {
			return fmt.Errorf("queue backfill states: %w", err)
		}

	case SyncModeIncremental:
		if pendingBefore > 0 {
			const q = `
				UPDATE sync_fund_state
				SET last_run_id = $1, updated_at = NOW()
				WHERE fund_id = ANY($2)
				  AND status IN ('pending', 'failed')
			`
			if _, err := o.pool.Exec(ctx, q, runID, activeIDs); err != nil {
				return fmt.Errorf("stamp pending incremental states: %w", err)
			}
			return nil
		}

		const q = `
			UPDATE sync_fund_state
			SET
				status = 'pending',
				next_retry_at = NULL,
				last_run_id = $1,
				updated_at = NOW()
			WHERE fund_id = ANY($2)
		`
		if _, err := o.pool.Exec(ctx, q, runID, activeIDs); err != nil {
			return fmt.Errorf("queue incremental states: %w", err)
		}

	default:
		return fmt.Errorf("unsupported sync mode: %s", mode)
	}

	return nil
}

func (o *Orchestrator) countPendingFailedForActive(ctx context.Context, activeIDs []int64) (int, error) {
	const q = `
		SELECT COUNT(1)
		FROM sync_fund_state
		WHERE fund_id = ANY($1)
		  AND status IN ('pending', 'failed')
	`

	var count int
	err := o.pool.QueryRow(ctx, q, activeIDs).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count pending/failed fund states: %w", err)
	}

	return count, nil
}

func (o *Orchestrator) syncSingleFund(ctx context.Context, mode SyncMode, fund domain.Fund) (int, *time.Time, error) {
	history, err := o.fetcher.FetchSchemeNavHistory(ctx, fund.SchemeCode)
	if err != nil {
		return 0, nil, fmt.Errorf("fetch nav history for %s: %w", fund.SchemeCode, err)
	}

	var latestSeen *time.Time
	if mode == SyncModeIncremental {
		existing, err := o.navs.GetLatestByFundID(ctx, fund.ID, 1)
		if err != nil {
			return 0, nil, fmt.Errorf("get latest nav for %s: %w", fund.SchemeCode, err)
		}
		if len(existing) > 0 {
			latest := existing[0].NAVDate.UTC()
			latestSeen = &latest
		}
	}

	inserted := 0
	var maxDate *time.Time

	for _, point := range history.Data {
		navDate := point.Date.UTC()
		if latestSeen != nil && !navDate.After(*latestSeen) {
			continue
		}

		if err := o.navs.Upsert(ctx, domain.NAVHistory{
			FundID:  fund.ID,
			NAVDate: navDate,
			NAV:     point.NAV,
			Source:  "mfapi",
		}); err != nil {
			return inserted, maxDate, fmt.Errorf("upsert nav row for %s on %s: %w", fund.SchemeCode, navDate.Format("2006-01-02"), err)
		}

		inserted++
		if maxDate == nil || navDate.After(*maxDate) {
			value := navDate
			maxDate = &value
		}
	}

	if maxDate == nil && latestSeen != nil {
		copyValue := *latestSeen
		maxDate = &copyValue
	}

	return inserted, maxDate, nil
}

func (o *Orchestrator) listActiveFunds(ctx context.Context) ([]domain.Fund, error) {
	all := make([]domain.Fund, 0)
	offset := int32(0)

	for {
		chunk, err := o.funds.ListActive(ctx, fetchBatchLimit, offset)
		if err != nil {
			return nil, fmt.Errorf("list active funds at offset %d: %w", offset, err)
		}
		if len(chunk) == 0 {
			break
		}

		all = append(all, chunk...)
		offset += int32(len(chunk))
	}

	return all, nil
}

func backoffForRetry(retryCount int32) time.Duration {
	if retryCount <= 0 {
		retryCount = 1
	}

	backoff := time.Minute
	for i := int32(1); i < retryCount; i++ {
		backoff *= 2
		if backoff >= time.Hour {
			return time.Hour
		}
	}

	if backoff > time.Hour {
		return time.Hour
	}

	return backoff
}

func safeError(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.TrimSpace(err.Error())
	if len(msg) > 2048 {
		return msg[:2048]
	}

	return msg
}

func finalizeRunStatus(result SyncResult, runErr error) (status string, errorMessage string) {
	if runErr != nil {
		if result.ProcessedFunds > 0 || result.FailedFunds > 0 {
			return SyncStatusPartial, safeError(runErr)
		}
		return SyncStatusFailed, safeError(runErr)
	}

	if result.FailedFunds > 0 {
		return SyncStatusPartial, fmt.Sprintf("%d fund(s) failed during sync", result.FailedFunds)
	}

	return SyncStatusSuccess, ""
}
