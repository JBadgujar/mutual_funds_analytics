package domain

import (
	"context"
	"time"
)

type FundRepository interface {
	Upsert(ctx context.Context, fund Fund) (Fund, error)
	GetBySchemeCode(ctx context.Context, schemeCode string) (Fund, error)
	ListActive(ctx context.Context, limit, offset int32) ([]Fund, error)
	ListFiltered(ctx context.Context, category, amc string, limit, offset int32) ([]Fund, error)
	GetSummaryBySchemeCode(ctx context.Context, schemeCode string) (FundSummary, error)
	ListSummaries(ctx context.Context, category, amcPrefix string, limit, offset int32) ([]FundSummary, error)
}

type NavRepository interface {
	Upsert(ctx context.Context, nav NAVHistory) error
	GetByDate(ctx context.Context, fundID int64, navDate time.Time) (NAVHistory, error)
	GetLatestByFundID(ctx context.Context, fundID int64, limit int32) ([]NAVHistory, error)
	ListByFundID(ctx context.Context, fundID int64) ([]NAVHistory, error)
}

type AnalyticsRepository interface {
	Upsert(ctx context.Context, snapshot AnalyticsSnapshot) error
	GetByFundAndWindow(ctx context.Context, fundID int64, windowCode string) (AnalyticsSnapshot, error)
	ListByWindow(ctx context.Context, windowCode string, asOfDate time.Time, limit int32) ([]AnalyticsSnapshot, error)
	ListRanked(ctx context.Context, query RankQuery) ([]RankedFund, int64, error)
}

type SyncRepository interface {
	StartRun(ctx context.Context, triggeredBy string) (SyncRun, error)
	CompleteRun(ctx context.Context, runID int64, status string, recordsProcessed int32, errorMessage string) error
	UpsertFundState(ctx context.Context, state SyncFundState) error
	ListPendingFundStates(ctx context.Context, now time.Time, limit int32) ([]SyncFundState, error)
	GetLatestRunByTriggeredBy(ctx context.Context, triggeredBy string) (SyncRun, error)
	GetActiveRun(ctx context.Context) (SyncRun, error)
	GetLatestRun(ctx context.Context) (SyncRun, error)
	ListFundStates(ctx context.Context, limit, offset int32) ([]SyncFundStateView, error)
}

type RateLimitStateRepository interface {
	Upsert(ctx context.Context, state RateLimitState) error
	Get(ctx context.Context, provider string) (RateLimitState, error)
}
