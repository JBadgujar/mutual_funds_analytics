package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/syncer"
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
	rankCache *rankingCache
	syncs     SyncController
}

type SyncController interface {
	TriggerIncremental(ctx context.Context, source string) (domain.SyncRun, error)
	GetStatus(ctx context.Context, limit, offset int32) (*domain.SyncRun, *domain.SyncRun, []domain.SyncFundStateView, error)
}

type FundsQuery struct {
	Category  string
	AMC       string
	AMCPrefix string
	Limit     int32
	Offset    int32
}

func NewAPI(funds domain.FundRepository, analytics domain.AnalyticsRepository) *API {
	return &API{
		funds:     funds,
		analytics: analytics,
		rankCache: newRankingCache(60 * time.Second),
	}
}

func (a *API) SetSyncController(controller SyncController) {
	a.syncs = controller
}

type RankQuery struct {
	Category string
	Window   string
	SortBy   string
	Limit    int32
	Offset   int32
}

func (a *API) HandleRankFunds(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	query, err := parseRankQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	cacheKey := rankCacheKey(query)
	if cachedPayload, ok := a.rankCache.Get(cacheKey, time.Now().UTC()); ok {
		writeJSONBytes(w, http.StatusOK, cachedPayload)
		return
	}

	rows, total, err := a.analytics.ListRanked(r.Context(), domain.RankQuery{
		Category:   query.Category,
		WindowCode: query.Window,
		SortBy:     query.SortBy,
		Limit:      query.Limit,
		Offset:     query.Offset,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to rank funds")
		return
	}

	if total == 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"category":    query.Category,
			"window":      query.Window,
			"sorted_by":   query.SortBy,
			"total_funds": 0,
			"showing":     0,
			"funds":       []any{},
			"note":        "no ranked funds available for requested window; data may be insufficient",
		})
		return
	}

	medianKey := fmt.Sprintf("median_return_%s", strings.ToLower(query.Window))
	drawdownKey := fmt.Sprintf("max_drawdown_%s", strings.ToLower(query.Window))

	items := make([]map[string]any, 0, len(rows))
	for i, row := range rows {
		item := map[string]any{
			"rank":         int(query.Offset) + i + 1,
			"fund_code":    row.SchemeCode,
			"fund_name":    row.FundName,
			"amc":          deriveAMCFromName(row.FundName),
			"current_nav":  row.CurrentNAV,
			"last_updated": row.LastUpdated.UTC().Format("2006-01-02"),
		}
		item[medianKey] = row.RollingReturnMedian
		item[drawdownKey] = row.MaxDrawdownDeclinePct
		items = append(items, item)
	}

	response := map[string]any{
		"category":    query.Category,
		"window":      query.Window,
		"sorted_by":   query.SortBy,
		"total_funds": total,
		"showing":     len(items),
		"funds":       items,
	}

	payload, err := json.Marshal(response)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to encode rank response")
		return
	}

	a.rankCache.Set(cacheKey, payload, time.Now().UTC())
	writeJSONBytes(w, http.StatusOK, payload)
}

func (a *API) InvalidateRankingCache() {
	a.rankCache.Invalidate()
}

func (a *API) HandleSyncTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}

	if a.syncs == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "sync control plane is not configured")
		return
	}

	run, err := a.syncs.TriggerIncremental(r.Context(), "manual-api")
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			writeError(w, http.StatusRequestTimeout, "timeout", "sync trigger timed out")
		case errors.Is(err, syncer.ErrSyncAlreadyRunning), strings.Contains(strings.ToLower(err.Error()), "already running"):
			writeError(w, http.StatusConflict, "conflict", "sync run already active")
		default:
			writeError(w, http.StatusInternalServerError, "internal_error", "failed to trigger sync")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id":       run.ID,
		"status":       run.Status,
		"triggered_by": run.TriggeredBy,
		"started_at":   run.StartedAt.UTC().Format(time.RFC3339),
	})
}

func (a *API) HandleSyncStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}

	if a.syncs == nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "sync control plane is not configured")
		return
	}

	limit, offset, err := parsePagination(r, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	currentRun, lastRun, fundStates, err := a.syncs.GetStatus(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to fetch sync status")
		return
	}

	pending := 0
	synced := 0
	failed := 0
	for _, state := range fundStates {
		switch state.Status {
		case "pending":
			pending++
		case "synced":
			synced++
		case "failed":
			failed++
		}
	}

	states := make([]map[string]any, 0, len(fundStates))
	for _, state := range fundStates {
		states = append(states, map[string]any{
			"fund_id":        state.FundID,
			"fund_code":      state.SchemeCode,
			"fund_name":      state.FundName,
			"category":       state.Category,
			"status":         state.Status,
			"retry_count":    state.RetryCount,
			"next_retry_at":  formatDateTime(state.NextRetryAt),
			"last_error":     state.LastError,
			"last_synced_at": formatDateTime(state.LastSyncedAt),
			"last_nav_date":  formatDate(state.LastNAVDate),
			"updated_at":     state.UpdatedAt.UTC().Format(time.RFC3339),
			"last_run_id":    state.LastRunID,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"current_run": toSyncRunPayload(currentRun),
		"last_run":    toSyncRunPayload(lastRun),
		"summary": map[string]any{
			"total_funds": len(fundStates),
			"pending":     pending,
			"synced":      synced,
			"failed":      failed,
		},
		"fund_states": states,
	})
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

func parseRankQuery(r *http.Request) (RankQuery, error) {
	query := r.URL.Query()

	category := strings.TrimSpace(query.Get("category"))
	if category != "" {
		if _, ok := allowedCategories[category]; !ok {
			return RankQuery{}, fmt.Errorf("category must be Mid Cap Direct Growth or Small Cap Direct Growth")
		}
	}

	window := strings.ToUpper(strings.TrimSpace(query.Get("window")))
	if window == "" {
		window = "3Y"
	}
	if _, ok := allowedWindows[window]; !ok {
		return RankQuery{}, fmt.Errorf("window must be one of 1Y,3Y,5Y,10Y")
	}

	sortBy := strings.TrimSpace(query.Get("sort_by"))
	if sortBy == "" {
		sortBy = "median_return"
	}
	if sortBy != "median_return" && sortBy != "max_drawdown" {
		return RankQuery{}, fmt.Errorf("sort_by must be median_return or max_drawdown")
	}

	limit := int32(10)
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 || parsed > 100 {
			return RankQuery{}, fmt.Errorf("limit must be an integer between 1 and 100")
		}
		limit = int32(parsed)
	}

	offset := int32(0)
	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 {
			return RankQuery{}, fmt.Errorf("offset must be a non-negative integer")
		}
		offset = int32(parsed)
	}

	return RankQuery{
		Category: category,
		Window:   window,
		SortBy:   sortBy,
		Limit:    limit,
		Offset:   offset,
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

func parsePagination(r *http.Request, defaultLimit int32) (int32, int32, error) {
	query := r.URL.Query()

	limit := defaultLimit
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 || parsed > 1000 {
			return 0, 0, fmt.Errorf("limit must be an integer between 1 and 1000")
		}
		limit = int32(parsed)
	}

	offset := int32(0)
	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 {
			return 0, 0, fmt.Errorf("offset must be a non-negative integer")
		}
		offset = int32(parsed)
	}

	return limit, offset, nil
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

func formatDateTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339)
	return &formatted
}

func toSyncRunPayload(run *domain.SyncRun) any {
	if run == nil {
		return nil
	}

	return map[string]any{
		"run_id":            run.ID,
		"status":            run.Status,
		"triggered_by":      run.TriggeredBy,
		"records_processed": run.RecordsProcessed,
		"error":             run.ErrorMessage,
		"started_at":        run.StartedAt.UTC().Format(time.RFC3339),
		"completed_at":      formatDateTime(run.CompletedAt),
		"updated_at":        run.UpdatedAt.UTC().Format(time.RFC3339),
	}
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

func writeJSONBytes(w http.ResponseWriter, statusCode int, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(payload)
}

type rankingCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]rankCacheEntry
}

type rankCacheEntry struct {
	payload   []byte
	expiresAt time.Time
}

func newRankingCache(ttl time.Duration) *rankingCache {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}

	return &rankingCache{
		ttl:     ttl,
		entries: make(map[string]rankCacheEntry),
	}
}

func (c *rankingCache) Get(key string, now time.Time) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}

	if now.After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}

	payload := make([]byte, len(entry.payload))
	copy(payload, entry.payload)
	return payload, true
}

func (c *rankingCache) Set(key string, payload []byte, now time.Time) {
	if len(payload) == 0 {
		return
	}

	copied := make([]byte, len(payload))
	copy(copied, payload)

	c.mu.Lock()
	c.entries[key] = rankCacheEntry{
		payload:   copied,
		expiresAt: now.Add(c.ttl),
	}
	c.mu.Unlock()
}

func (c *rankingCache) Invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]rankCacheEntry)
	c.mu.Unlock()
}

func rankCacheKey(query RankQuery) string {
	category := query.Category
	if category == "" {
		category = "all"
	}

	return fmt.Sprintf("%s|%s|%s|%d|%d", category, query.Window, query.SortBy, query.Limit, query.Offset)
}
