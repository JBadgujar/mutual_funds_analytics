package http

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"mutual-fund-analytics/internal/domain"
)

var allowedWindows = map[string]struct{}{
	"1Y":  {},
	"3Y":  {},
	"5Y":  {},
	"10Y": {},
}

var fundCodePattern = regexp.MustCompile(`^[0-9]{3,10}$`)

type amcFilter struct {
	Canonical string
	Prefix    string
}

var allowedAMCs = map[string]amcFilter{
	"icici prudential":             {Canonical: "ICICI Prudential Mutual Fund", Prefix: "ICICI Prudential"},
	"icici prudential mutual fund": {Canonical: "ICICI Prudential Mutual Fund", Prefix: "ICICI Prudential"},
	"hdfc":                         {Canonical: "HDFC Mutual Fund", Prefix: "HDFC"},
	"hdfc mutual fund":             {Canonical: "HDFC Mutual Fund", Prefix: "HDFC"},
	"axis":                         {Canonical: "Axis Mutual Fund", Prefix: "Axis"},
	"axis mutual fund":             {Canonical: "Axis Mutual Fund", Prefix: "Axis"},
	"sbi":                          {Canonical: "SBI Mutual Fund", Prefix: "SBI"},
	"sbi mutual fund":              {Canonical: "SBI Mutual Fund", Prefix: "SBI"},
	"kotak":                        {Canonical: "Kotak Mahindra Mutual Fund", Prefix: "Kotak"},
	"kotak mahindra":               {Canonical: "Kotak Mahindra Mutual Fund", Prefix: "Kotak"},
	"kotak mahindra mutual fund":   {Canonical: "Kotak Mahindra Mutual Fund", Prefix: "Kotak"},
}

var allowedCategories = map[string]struct{}{
	"Mid Cap Direct Growth":   {},
	"Small Cap Direct Growth": {},
}

type API struct {
	funds     domain.FundRepository
	analytics domain.AnalyticsRepository
}

type FundsQuery struct {
	Category  string
	AMC       string
	AMCPrefix string
	Limit     int32
	Offset    int32
}

func NewAPI(funds domain.FundRepository, analytics domain.AnalyticsRepository) *API {
	return &API{funds: funds, analytics: analytics}
}

func (a *API) HandleListFunds(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	query, err := parseFundsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	items, err := a.funds.ListSummaries(r.Context(), query.Category, query.AMCPrefix, query.Limit, query.Offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list funds")
		return
	}

	response := make([]fundListItemResponse, 0, len(items))
	for _, item := range items {
		response = append(response, toFundListItemResponse(item, query.AMC))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"filters": map[string]any{
			"category": query.Category,
			"amc":      query.AMC,
		},
		"total_funds": len(response),
		"funds":       response,
	})
}

func (a *API) HandleGetFund(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	schemeCode, err := parseFundCodeFromPath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_path", err.Error())
		return
	}

	fund, err := a.funds.GetSummaryBySchemeCode(r.Context(), schemeCode)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "fund not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch fund")
		return
	}

	var latestNAV any
	if fund.LatestNAVDate != nil && fund.LatestNAV != nil {
		latestNAV = map[string]any{
			"date":  fund.LatestNAVDate.UTC().Format("2006-01-02"),
			"value": *fund.LatestNAV,
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"fund_code":    fund.SchemeCode,
		"fund_name":    fund.Name,
		"category":     fund.Category,
		"amc":          deriveAMCFromName(fund.Name),
		"active":       fund.Active,
		"latest_nav":   latestNAV,
		"last_updated": fund.UpdatedAt.UTC().Format(time.RFC3339),
	})
}

func (a *API) HandleGetFundAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	schemeCode, err := parseFundCodeFromAnalyticsPath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_path", err.Error())
		return
	}

	window := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("window")))
	if _, ok := allowedWindows[window]; !ok {
		writeError(w, http.StatusBadRequest, "invalid_query", "window must be one of 1Y,3Y,5Y,10Y")
		return
	}

	fund, err := a.funds.GetSummaryBySchemeCode(r.Context(), schemeCode)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "fund not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch fund")
		return
	}

	snapshot, err := a.analytics.GetByFundAndWindow(r.Context(), fund.ID, window)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", "analytics snapshot not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch analytics")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"fund_code": fund.SchemeCode,
		"fund_name": fund.Name,
		"category":  fund.Category,
		"amc":       deriveAMCFromName(fund.Name),
		"window":    snapshot.WindowCode,
		"data_availability": map[string]any{
			"start_date":        formatDate(snapshot.StartDate),
			"end_date":          formatDate(snapshot.EndDate),
			"total_days":        snapshot.TotalDays,
			"nav_data_points":   snapshot.NAVDataPoints,
			"insufficient_data": snapshot.InsufficientData,
		},
		"rolling_returns": map[string]any{
			"min":    snapshot.RollingReturnMin,
			"max":    snapshot.RollingReturnMax,
			"median": snapshot.RollingReturnMedian,
			"p25":    snapshot.RollingReturnP25,
			"p75":    snapshot.RollingReturnP75,
		},
		"max_drawdown": snapshot.MaxDrawdownDeclinePct,
		"max_drawdown_details": map[string]any{
			"peak_date":   formatDate(snapshot.MaxDrawdownPeakDate),
			"trough_date": formatDate(snapshot.MaxDrawdownTroughDate),
		},
		"cagr": map[string]any{
			"min":    snapshot.CAGRMin,
			"max":    snapshot.CAGRMax,
			"median": snapshot.CAGRMedian,
		},
		"annualized_volatility": snapshot.AnnualizedVolatility,
		"computed_at":           snapshot.UpdatedAt.UTC().Format(time.RFC3339),
	})
}

type fundListItemResponse struct {
	FundCode      string  `json:"fund_code"`
	FundName      string  `json:"fund_name"`
	AMC           string  `json:"amc"`
	Category      string  `json:"category"`
	LatestNAVDate *string `json:"latest_nav_date"`
}

func toFundListItemResponse(fund domain.FundSummary, fallbackAMC string) fundListItemResponse {
	amc := deriveAMCFromName(fund.Name)
	if amc == "" {
		amc = fallbackAMC
	}

	var latestNAVDate *string
	if fund.LatestNAVDate != nil {
		formatted := fund.LatestNAVDate.UTC().Format("2006-01-02")
		latestNAVDate = &formatted
	}

	return fundListItemResponse{
		FundCode:      fund.SchemeCode,
		FundName:      fund.Name,
		AMC:           amc,
		Category:      fund.Category,
		LatestNAVDate: latestNAVDate,
	}
}

func parseFundsQuery(r *http.Request) (FundsQuery, error) {
	query := r.URL.Query()
	category := strings.TrimSpace(query.Get("category"))
	rawAMC := strings.TrimSpace(query.Get("amc"))
	amc := ""
	amcPrefix := ""

	if category != "" {
		if _, ok := allowedCategories[category]; !ok {
			return FundsQuery{}, fmt.Errorf("category must be Mid Cap Direct Growth or Small Cap Direct Growth")
		}
	}

	if rawAMC != "" {
		filter, ok := allowedAMCs[strings.ToLower(rawAMC)]
		if !ok {
			return FundsQuery{}, fmt.Errorf("amc must be one of ICICI Prudential, HDFC, Axis, SBI, Kotak Mahindra")
		}
		amc = filter.Canonical
		amcPrefix = filter.Prefix
	}

	return FundsQuery{
		Category:  category,
		AMC:       amc,
		AMCPrefix: amcPrefix,
		Limit:     200,
		Offset:    0,
	}, nil
}

func parseFundCodeFromPath(path string) (string, error) {
	trimmed := strings.Trim(path, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] != "funds" {
		return "", fmt.Errorf("path must be /funds/{code}")
	}

	code := strings.TrimSpace(parts[1])
	if code == "" {
		return "", fmt.Errorf("fund code is required")
	}
	if !fundCodePattern.MatchString(code) {
		return "", fmt.Errorf("fund code must be numeric")
	}

	return code, nil
}

func parseFundCodeFromAnalyticsPath(path string) (string, error) {
	trimmed := strings.Trim(path, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 3 || parts[0] != "funds" || parts[2] != "analytics" {
		return "", fmt.Errorf("path must be /funds/{code}/analytics")
	}

	code := strings.TrimSpace(parts[1])
	if code == "" {
		return "", fmt.Errorf("fund code is required")
	}
	if !fundCodePattern.MatchString(code) {
		return "", fmt.Errorf("fund code must be numeric")
	}

	return code, nil
}

func deriveAMCFromName(name string) string {
	n := strings.TrimSpace(name)
	if n == "" {
		return ""
	}

	if strings.HasPrefix(n, "ICICI Prudential") {
		return "ICICI Prudential Mutual Fund"
	}
	if strings.HasPrefix(n, "HDFC") {
		return "HDFC Mutual Fund"
	}
	if strings.HasPrefix(n, "Axis") {
		return "Axis Mutual Fund"
	}
	if strings.HasPrefix(n, "SBI") {
		return "SBI Mutual Fund"
	}
	if strings.HasPrefix(n, "Kotak") {
		return "Kotak Mahindra Mutual Fund"
	}

	return ""
}

func formatDate(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format("2006-01-02")
	return &formatted
}

func isNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

type apiErrorEnvelope struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(w, statusCode, apiErrorEnvelope{Error: apiErrorBody{Code: code, Message: message}})
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
