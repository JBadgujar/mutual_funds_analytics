package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
)

type contractFundRepo struct {
	byCode map[string]domain.FundSummary
	list   []domain.FundSummary
}

func (r *contractFundRepo) Upsert(ctx context.Context, fund domain.Fund) (domain.Fund, error) {
	return fund, nil
}

func (r *contractFundRepo) GetBySchemeCode(ctx context.Context, schemeCode string) (domain.Fund, error) {
	s, err := r.GetSummaryBySchemeCode(ctx, schemeCode)
	if err != nil {
		return domain.Fund{}, err
	}
	return domain.Fund{
		ID:         s.ID,
		SchemeCode: s.SchemeCode,
		Name:       s.Name,
		Category:   s.Category,
		ISIN:       s.ISIN,
		Active:     s.Active,
		CreatedAt:  s.CreatedAt,
		UpdatedAt:  s.UpdatedAt,
	}, nil
}

func (r *contractFundRepo) ListActive(ctx context.Context, limit, offset int32) ([]domain.Fund, error) {
	out := make([]domain.Fund, 0, len(r.list))
	for _, s := range r.list {
		if s.Active {
			out = append(out, domain.Fund{
				ID:         s.ID,
				SchemeCode: s.SchemeCode,
				Name:       s.Name,
				Category:   s.Category,
				ISIN:       s.ISIN,
				Active:     s.Active,
				CreatedAt:  s.CreatedAt,
				UpdatedAt:  s.UpdatedAt,
			})
		}
	}
	return out, nil
}

func (r *contractFundRepo) ListFiltered(ctx context.Context, category, amc string, limit, offset int32) ([]domain.Fund, error) {
	out := make([]domain.Fund, 0, len(r.list))
	for _, s := range r.list {
		if category != "" && s.Category != category {
			continue
		}
		if amc != "" && !strings.HasPrefix(s.Name, amc) {
			continue
		}
		out = append(out, domain.Fund{
			ID:         s.ID,
			SchemeCode: s.SchemeCode,
			Name:       s.Name,
			Category:   s.Category,
			ISIN:       s.ISIN,
			Active:     s.Active,
			CreatedAt:  s.CreatedAt,
			UpdatedAt:  s.UpdatedAt,
		})
	}
	return out, nil
}

func (r *contractFundRepo) GetSummaryBySchemeCode(ctx context.Context, schemeCode string) (domain.FundSummary, error) {
	s, ok := r.byCode[schemeCode]
	if !ok {
		return domain.FundSummary{}, sql.ErrNoRows
	}
	return s, nil
}

func (r *contractFundRepo) ListSummaries(ctx context.Context, category, amcPrefix string, limit, offset int32) ([]domain.FundSummary, error) {
	out := make([]domain.FundSummary, 0, len(r.list))
	for _, s := range r.list {
		if !s.Active {
			continue
		}
		if category != "" && s.Category != category {
			continue
		}
		if amcPrefix != "" && !strings.HasPrefix(s.Name, amcPrefix) {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

type contractAnalyticsRepo struct {
	byFundWindow map[string]domain.AnalyticsSnapshot
}

func (r *contractAnalyticsRepo) Upsert(ctx context.Context, snapshot domain.AnalyticsSnapshot) error {
	return nil
}

func (r *contractAnalyticsRepo) GetByFundAndWindow(ctx context.Context, fundID int64, windowCode string) (domain.AnalyticsSnapshot, error) {
	key := snapshotKey(fundID, windowCode)
	s, ok := r.byFundWindow[key]
	if !ok {
		return domain.AnalyticsSnapshot{}, sql.ErrNoRows
	}
	return s, nil
}

func (r *contractAnalyticsRepo) ListByWindow(ctx context.Context, windowCode string, asOfDate time.Time, limit int32) ([]domain.AnalyticsSnapshot, error) {
	return nil, nil
}

func snapshotKey(fundID int64, window string) string {
	return fmt.Sprintf("%d::%s", fundID, window)
}

func TestFundsContract_List200(t *testing.T) {
	navDate := time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC)
	value := 78.45

	repo := &contractFundRepo{
		byCode: map[string]domain.FundSummary{
			"119598": {
				ID:            1,
				SchemeCode:    "119598",
				Name:          "Axis Mid Cap Fund - Direct Plan - Growth",
				Category:      "Mid Cap Direct Growth",
				Active:        true,
				LatestNAVDate: &navDate,
				LatestNAV:     &value,
				UpdatedAt:     now,
			},
		},
		list: []domain.FundSummary{
			{
				ID:            1,
				SchemeCode:    "119598",
				Name:          "Axis Mid Cap Fund - Direct Plan - Growth",
				Category:      "Mid Cap Direct Growth",
				Active:        true,
				LatestNAVDate: &navDate,
				LatestNAV:     &value,
				UpdatedAt:     now,
			},
		},
	}
	api := NewAPI(repo, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds?category=Mid%20Cap%20Direct%20Growth&amc=Axis", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	funds, ok := body["funds"].([]any)
	if !ok || len(funds) != 1 {
		t.Fatalf("expected one fund item, got %v", body["funds"])
	}

	item := funds[0].(map[string]any)
	if item["fund_code"] != "119598" {
		t.Fatalf("unexpected fund_code: %v", item["fund_code"])
	}
	if item["amc"] != "Axis Mutual Fund" {
		t.Fatalf("unexpected amc: %v", item["amc"])
	}
}

func TestFundsContract_List400InvalidQuery(t *testing.T) {
	api := NewAPI(&contractFundRepo{byCode: map[string]domain.FundSummary{}}, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds?amc=UnknownAMC", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != "invalid_query" {
		t.Fatalf("expected invalid_query code, got %v", errorBody["code"])
	}
}

func TestFundsContract_GetByCode200(t *testing.T) {
	navDate := time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC)
	value := 78.45

	repo := &contractFundRepo{
		byCode: map[string]domain.FundSummary{
			"119598": {
				ID:            1,
				SchemeCode:    "119598",
				Name:          "Axis Mid Cap Fund - Direct Plan - Growth",
				Category:      "Mid Cap Direct Growth",
				Active:        true,
				LatestNAVDate: &navDate,
				LatestNAV:     &value,
				UpdatedAt:     now,
			},
		},
	}
	api := NewAPI(repo, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds/119598", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if body["fund_code"] != "119598" {
		t.Fatalf("unexpected fund_code: %v", body["fund_code"])
	}
	if body["latest_nav"] == nil {
		t.Fatalf("expected latest_nav object")
	}
}

func TestFundsContract_GetByCode404(t *testing.T) {
	api := NewAPI(&contractFundRepo{byCode: map[string]domain.FundSummary{}}, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds/119598", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != "not_found" {
		t.Fatalf("expected not_found code, got %v", errorBody["code"])
	}
}

func TestFundsContract_Analytics200(t *testing.T) {
	start := time.Date(2023, 1, 6, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 6, 12, 0, 0, 0, time.UTC)

	repo := &contractFundRepo{
		byCode: map[string]domain.FundSummary{
			"119598": {
				ID:         1,
				SchemeCode: "119598",
				Name:       "Axis Mid Cap Fund - Direct Plan - Growth",
				Category:   "Mid Cap Direct Growth",
				Active:     true,
				UpdatedAt:  now,
			},
		},
	}

	analytics := &contractAnalyticsRepo{
		byFundWindow: map[string]domain.AnalyticsSnapshot{
			snapshotKey(1, "3Y"): {
				FundID:                1,
				WindowCode:            "3Y",
				StartDate:             &start,
				EndDate:               &end,
				TotalDays:             1096,
				NAVDataPoints:         750,
				RollingReturnMin:      8.2,
				RollingReturnMax:      48.5,
				RollingReturnMedian:   22.3,
				RollingReturnP25:      15.7,
				RollingReturnP75:      28.9,
				MaxDrawdownDeclinePct: -32.1,
				CAGRMin:               9.5,
				CAGRMax:               45.2,
				CAGRMedian:            21.8,
				AnnualizedVolatility:  17.4,
				UpdatedAt:             now,
			},
		},
	}

	api := NewAPI(repo, analytics)
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds/119598/analytics?window=3Y", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if body["fund_code"] != "119598" {
		t.Fatalf("unexpected fund_code: %v", body["fund_code"])
	}
	if body["window"] != "3Y" {
		t.Fatalf("unexpected window: %v", body["window"])
	}
}

func TestFundsContract_Analytics400InvalidWindow(t *testing.T) {
	repo := &contractFundRepo{
		byCode: map[string]domain.FundSummary{
			"119598": {ID: 1, SchemeCode: "119598", Name: "Axis Mid Cap Fund - Direct Plan - Growth", Active: true},
		},
	}
	api := NewAPI(repo, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds/119598/analytics?window=2Y", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != "invalid_query" {
		t.Fatalf("expected invalid_query code, got %v", errorBody["code"])
	}
}

func TestFundsContract_Analytics404(t *testing.T) {
	repo := &contractFundRepo{
		byCode: map[string]domain.FundSummary{
			"119598": {ID: 1, SchemeCode: "119598", Name: "Axis Mid Cap Fund - Direct Plan - Growth", Active: true},
		},
	}
	api := NewAPI(repo, &contractAnalyticsRepo{byFundWindow: map[string]domain.AnalyticsSnapshot{}})
	router := NewRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/funds/119598/analytics?window=3Y", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	errorBody := body["error"].(map[string]any)
	if errorBody["code"] != "not_found" {
		t.Fatalf("expected not_found code, got %v", errorBody["code"])
	}
}
