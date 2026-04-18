package mfapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const productionBaseURL = "https://api.mfapi.in"

const (
	defaultMaxAttempts = 4
	defaultBaseBackoff = 200 * time.Millisecond
	defaultMaxBackoff  = 2 * time.Second
)

type AcquireGate interface {
	Acquire(ctx context.Context) error
}

type Client struct {
	baseURL    *url.URL
	httpClient *stdhttp.Client
	gate       AcquireGate
	retry      retryPolicy
	waitFn     func(context.Context, time.Duration) error
}

type retryPolicy struct {
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

type SchemeSummary struct {
	SchemeCode string `json:"schemeCode"`
	SchemeName string `json:"schemeName"`
}

type SchemeNAVPoint struct {
	Date time.Time
	NAV  float64
}

type SchemeNavHistory struct {
	SchemeCode     string
	SchemeName     string
	FundHouse      string
	SchemeType     string
	SchemeCategory string
	Data           []SchemeNAVPoint
}

func NewClient(httpClient *stdhttp.Client, gate AcquireGate) (*Client, error) {
	return newClientWithBaseURL(productionBaseURL, httpClient, gate)
}

func (c *Client) FetchSchemeList(ctx context.Context) ([]SchemeSummary, error) {
	resp, err := c.doWithRetry(ctx, stdhttp.MethodGet, "/mf")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var raw []struct {
		SchemeCode any    `json:"schemeCode"`
		SchemeName string `json:"schemeName"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode mfapi scheme list: %w", err)
	}

	out := make([]SchemeSummary, 0, len(raw))
	for _, item := range raw {
		code := stringifySchemeCode(item.SchemeCode)
		name := strings.TrimSpace(item.SchemeName)
		if code == "" || name == "" {
			continue
		}

		out = append(out, SchemeSummary{
			SchemeCode: code,
			SchemeName: name,
		})
	}

	return out, nil
}

func (c *Client) FetchSchemeNavHistory(ctx context.Context, code string) (SchemeNavHistory, error) {
	schemeCode := strings.TrimSpace(code)
	if schemeCode == "" {
		return SchemeNavHistory{}, fmt.Errorf("scheme code is required")
	}

	resourcePath := path.Join("/mf", filepath.ToSlash(schemeCode))
	resp, err := c.doWithRetry(ctx, stdhttp.MethodGet, resourcePath)
	if err != nil {
		return SchemeNavHistory{}, err
	}
	defer resp.Body.Close()

	var raw struct {
		Meta struct {
			SchemeCode     any    `json:"scheme_code"`
			SchemeName     string `json:"scheme_name"`
			FundHouse      string `json:"fund_house"`
			SchemeType     string `json:"scheme_type"`
			SchemeCategory string `json:"scheme_category"`
		} `json:"meta"`
		Data []struct {
			Date string `json:"date"`
			NAV  string `json:"nav"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return SchemeNavHistory{}, fmt.Errorf("decode mfapi nav history: %w", err)
	}

	out := SchemeNavHistory{
		SchemeCode:     stringifySchemeCode(raw.Meta.SchemeCode),
		SchemeName:     strings.TrimSpace(raw.Meta.SchemeName),
		FundHouse:      strings.TrimSpace(raw.Meta.FundHouse),
		SchemeType:     strings.TrimSpace(raw.Meta.SchemeType),
		SchemeCategory: strings.TrimSpace(raw.Meta.SchemeCategory),
		Data:           make([]SchemeNAVPoint, 0, len(raw.Data)),
	}

	if out.SchemeCode == "" {
		out.SchemeCode = schemeCode
	}

	for _, point := range raw.Data {
		navDate, err := time.Parse("02-01-2006", strings.TrimSpace(point.Date))
		if err != nil {
			continue
		}

		navValue, err := strconv.ParseFloat(strings.TrimSpace(point.NAV), 64)
		if err != nil {
			continue
		}

		out.Data = append(out.Data, SchemeNAVPoint{
			Date: navDate,
			NAV:  navValue,
		})
	}

	return out, nil
}

func (c *Client) Do(ctx context.Context, req *stdhttp.Request) (*stdhttp.Response, error) {
	if err := c.gate.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("acquire mfapi rate limiter: %w", err)
	}

	return c.httpClient.Do(req.WithContext(ctx))
}

func (c *Client) NewRequest(ctx context.Context, method, resourcePath string, body io.Reader) (*stdhttp.Request, error) {
	relativePath := strings.TrimSpace(resourcePath)
	if relativePath == "" {
		relativePath = "/"
	}

	resolved := *c.baseURL
	resolved.Path = path.Join(c.baseURL.Path, relativePath)
	if strings.HasSuffix(resourcePath, "/") && !strings.HasSuffix(resolved.Path, "/") {
		resolved.Path += "/"
	}

	req, err := stdhttp.NewRequestWithContext(ctx, method, resolved.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create mfapi request: %w", err)
	}

	return req, nil
}

func newClientWithBaseURL(base string, httpClient *stdhttp.Client, gate AcquireGate) (*Client, error) {
	if gate == nil {
		return nil, fmt.Errorf("mfapi limiter gate is required")
	}

	parsed, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse mfapi base url: %w", err)
	}

	if httpClient == nil {
		httpClient = &stdhttp.Client{Timeout: 15 * time.Second}
	}

	return &Client{
		baseURL:    parsed,
		httpClient: httpClient,
		gate:       gate,
		retry: retryPolicy{
			maxAttempts: defaultMaxAttempts,
			baseBackoff: defaultBaseBackoff,
			maxBackoff:  defaultMaxBackoff,
		},
		waitFn: waitWithContext,
	}, nil
}

func (c *Client) doWithRetry(ctx context.Context, method, resourcePath string) (*stdhttp.Response, error) {
	maxAttempts := c.retry.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	base := c.retry.baseBackoff
	if base <= 0 {
		base = defaultBaseBackoff
	}

	maxBackoff := c.retry.maxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}

	wait := c.waitFn
	if wait == nil {
		wait = waitWithContext
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := c.NewRequest(ctx, method, resourcePath, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.Do(ctx, req)
		if shouldRetry(resp, err) {
			if resp != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}

			if attempt == maxAttempts {
				if err != nil {
					return nil, fmt.Errorf("mfapi retry exhausted: %w", err)
				}
				return nil, httpStatusError(resourcePath, resp)
			}

			delay := backoffDuration(base, maxBackoff, attempt)
			if err := wait(ctx, delay); err != nil {
				return nil, err
			}
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("mfapi request failed: %w", err)
		}

		if resp.StatusCode >= stdhttp.StatusBadRequest {
			defer resp.Body.Close()
			return nil, httpStatusError(resourcePath, resp)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("mfapi request failed: retry loop ended unexpectedly")
}

func shouldRetry(resp *stdhttp.Response, err error) bool {
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		return true
	}

	if resp == nil {
		return false
	}

	if resp.StatusCode == stdhttp.StatusTooManyRequests {
		return true
	}

	return resp.StatusCode >= stdhttp.StatusInternalServerError
}

func backoffDuration(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}

	if delay > max {
		return max
	}

	return delay
}

func httpStatusError(resourcePath string, resp *stdhttp.Response) error {
	body := ""
	if resp.Body != nil {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		body = strings.TrimSpace(string(payload))
	}

	if body == "" {
		return fmt.Errorf("mfapi request failed for %s with status %d", resourcePath, resp.StatusCode)
	}

	return fmt.Errorf("mfapi request failed for %s with status %d: %s", resourcePath, resp.StatusCode, body)
}

func stringifySchemeCode(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
