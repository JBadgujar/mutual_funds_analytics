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

type NAVHistory struct {
	FundID    int64
	NAVDate   time.Time
	NAV       float64
	Source    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type AnalyticsSnapshot struct {
	FundID       int64
	AsOfDate     time.Time
	Return1Y     float64
	Return3Y     float64
	Return5Y     float64
	Volatility1Y float64
	SharpeRatio  float64
	ExpenseRatio float64
	CreatedAt    time.Time
	UpdatedAt    time.Time
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
	UpdatedAt     time.Time
}
