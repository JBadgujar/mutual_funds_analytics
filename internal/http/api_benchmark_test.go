package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
)

func BenchmarkAPI_HandleRankFundsWarmCache(b *testing.B) {
	now := time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC)
	nav := 78.45

	analytics := &contractAnalyticsRepo{
		rankedRows: []domain.RankedFund{
			{
				FundID:                1,
				SchemeCode:            "119598",
				FundName:              "Axis Mid Cap Fund - Direct Plan - Growth",
				Category:              "Mid Cap Direct Growth",
				WindowCode:            "3Y",
				RollingReturnMedian:   22.3,
				MaxDrawdownDeclinePct: -32.1,
				CurrentNAV:            &nav,
				LastUpdated:           now,
			},
		},
		totalRanked: 1,
	}

	api := NewAPI(&contractFundRepo{byCode: map[string]domain.FundSummary{}}, analytics)
	router := NewRouter(api)

	warmReq := httptest.NewRequest(http.MethodGet, "/funds/rank?window=3Y&sort_by=median_return&limit=10&offset=0", nil)
	warmRec := httptest.NewRecorder()
	router.ServeHTTP(warmRec, warmReq)
	if warmRec.Code != http.StatusOK {
		b.Fatalf("warmup request failed with code %d", warmRec.Code)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/funds/rank?window=3Y&sort_by=median_return&limit=10&offset=0", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("unexpected status code: %d", rec.Code)
		}
	}
}
