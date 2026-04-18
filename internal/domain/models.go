package domain

import "time"

type Fund struct {
	ID         int64
	SchemeCode string
	Name       string
	Category   string
	ISIN       string
	Active     bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type FundSummary struct {
	ID            int64
	SchemeCode    string
	Name          string
	Category      string
	ISIN          string
	Active        bool
	LatestNAVDate *time.Time
	LatestNAV     *float64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type NAVHistory struct {
	FundID    int64
	NAVDate   time.Time
	NAV       float64
	Source    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type AnalyticsSnapshot struct {
	FundID                int64
	WindowCode            string
	AsOfDate              time.Time
	StartDate             *time.Time
	EndDate               *time.Time
	TotalDays             int32
	NAVDataPoints         int32
	InsufficientData      bool
	RollingReturnMin      float64
	RollingReturnMax      float64
	RollingReturnMedian   float64
	RollingReturnP25      float64
	RollingReturnP75      float64
	MaxDrawdownDeclinePct float64
	MaxDrawdownPeakDate   *time.Time
	MaxDrawdownTroughDate *time.Time
	CAGRMin               float64
	CAGRMax               float64
	CAGRMedian            float64
	AnnualizedVolatility  float64
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type RankQuery struct {
	Category   string
	WindowCode string
	SortBy     string
	Limit      int32
	Offset     int32
}

type RankedFund struct {
	FundID                int64
	SchemeCode            string
	FundName              string
	Category              string
	WindowCode            string
	RollingReturnMedian   float64
	MaxDrawdownDeclinePct float64
	CurrentNAV            *float64
	LastUpdated           time.Time
}

type SyncRun struct {
	ID               int64
	StartedAt        time.Time
	CompletedAt      *time.Time
	Status           string
	TriggeredBy      string
	RecordsProcessed int32
	ErrorMessage     string
	UpdatedAt        time.Time
}

type SyncFundState struct {
	FundID        int64
	LastSyncedAt  *time.Time
	LastNAVDate   *time.Time
	Status        string
	RetryCount    int32
	NextRetryAt   *time.Time
	LastError     string
	UpdatedAt     time.Time
	LastRunID     *int64
	ConsecutiveOK int32
}

type RateLimitState struct {
	Provider      string
	WindowStart   time.Time
	WindowSeconds int32
	RequestCount  int32
	SecondBucket  int64
	MinuteBucket  int64
	HourBucket    int64
	SecondCount   int32
	MinuteCount   int32
	HourCount     int32
	UpdatedAt     time.Time
}
