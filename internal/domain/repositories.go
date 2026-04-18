package domain

import (
	"context"
	"time"
)

type FundRepository interface {
	Upsert(ctx context.Context, fund Fund) (Fund, error)
	GetBySchemeCode(ctx context.Context, schemeCode string) (Fund, error)
	ListActive(ctx context.Context, limit, offset int32) ([]Fund, error)
}

type NavRepository interface {
	Upsert(ctx context.Context, nav NAVHistory) error
	GetByDate(ctx context.Context, fundID int64, navDate time.Time) (NAVHistory, error)
	GetLatestByFundID(ctx context.Context, fundID int64, limit int32) ([]NAVHistory, error)
}

type AnalyticsRepository interface {
	Upsert(ctx context.Context, snapshot AnalyticsSnapshot) error
	GetLatestForFund(ctx context.Context, fundID int64) (AnalyticsSnapshot, error)
	TopByReturn1Y(ctx context.Context, asOfDate time.Time, limit int32) ([]AnalyticsSnapshot, error)
}

type SyncRepository interface {
	StartRun(ctx context.Context, triggeredBy string) (SyncRun, error)
	CompleteRun(ctx context.Context, runID int64, status string, recordsProcessed int32, errorMessage string) error
	UpsertFundState(ctx context.Context, state SyncFundState) error
	ListPendingFundStates(ctx context.Context, now time.Time, limit int32) ([]SyncFundState, error)
}

type RateLimitStateRepository interface {
	Upsert(ctx context.Context, state RateLimitState) error
	Get(ctx context.Context, provider string) (RateLimitState, error)
}
