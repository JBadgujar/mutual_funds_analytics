package mfapi

import (
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type fakeGate struct {
	calls int32
	err   error
}

func (g *fakeGate) Acquire(context.Context) error {
	atomic.AddInt32(&g.calls, 1)
	return g.err
}

func TestClient_Do_AcquiresLimiterBeforeRequest(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer server.Close()

	gate := &fakeGate{}
	client, err := newClientWithBaseURL(server.URL, server.Client(), gate)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	req, err := client.NewRequest(context.Background(), stdhttp.MethodGet, "/mf", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&gate.calls) != 1 {
		t.Fatalf("expected gate acquire to be called once, got %d", gate.calls)
	}
	if atomic.LoadInt32(&requestCount) != 1 {
		t.Fatalf("expected one outbound request, got %d", requestCount)
	}
}

func TestClient_Do_BlocksRequestWhenLimiterFails(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer server.Close()

	gate := &fakeGate{err: errors.New("rate limited")}
	client, err := newClientWithBaseURL(server.URL, server.Client(), gate)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	req, err := client.NewRequest(context.Background(), stdhttp.MethodGet, "/mf", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	_, err = client.Do(context.Background(), req)
	if err == nil {
		t.Fatal("expected limiter error")
	}

	if atomic.LoadInt32(&gate.calls) != 1 {
		t.Fatalf("expected gate acquire to be called once, got %d", gate.calls)
	}
	if atomic.LoadInt32(&requestCount) != 0 {
		t.Fatalf("expected zero outbound requests when limiter fails, got %d", requestCount)
	}
}

func TestFetchSchemeList_RetryAlwaysUsesLimiter(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempt := atomic.AddInt32(&requestCount, 1)
		if attempt <= 2 {
			w.WriteHeader(stdhttp.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}

		w.WriteHeader(stdhttp.StatusOK)
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"schemeCode": 120503,
			"schemeName": "ICICI Prudential Mid Cap Fund - Direct Plan - Growth",
		}})
	}))
	defer server.Close()

	gate := &fakeGate{}
	client, err := newClientWithBaseURL(server.URL, server.Client(), gate)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	client.retry = retryPolicy{maxAttempts: 4, baseBackoff: time.Millisecond, maxBackoff: 10 * time.Millisecond}
	client.waitFn = func(context.Context, time.Duration) error { return nil }

	items, err := client.FetchSchemeList(context.Background())
	if err != nil {
		t.Fatalf("fetch scheme list: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected one scheme, got %d", len(items))
	}
	if atomic.LoadInt32(&requestCount) != 3 {
		t.Fatalf("expected 3 HTTP attempts, got %d", requestCount)
	}
	if atomic.LoadInt32(&gate.calls) != 3 {
		t.Fatalf("expected limiter acquire called 3 times, got %d", gate.calls)
	}
}

func TestFetchSchemeList_DoesNotRetryOnBadRequest(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(stdhttp.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	gate := &fakeGate{}
	client, err := newClientWithBaseURL(server.URL, server.Client(), gate)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	client.retry = retryPolicy{maxAttempts: 4, baseBackoff: time.Millisecond, maxBackoff: 10 * time.Millisecond}
	client.waitFn = func(context.Context, time.Duration) error { return nil }

	_, err = client.FetchSchemeList(context.Background())
	if err == nil {
		t.Fatal("expected bad request error")
	}

	if atomic.LoadInt32(&requestCount) != 1 {
		t.Fatalf("expected 1 HTTP attempt for 400 response, got %d", requestCount)
	}
	if atomic.LoadInt32(&gate.calls) != 1 {
		t.Fatalf("expected limiter acquire called once, got %d", gate.calls)
	}
}
