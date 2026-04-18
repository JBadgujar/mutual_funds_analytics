package syncer

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/limiter"
	"mutual-fund-analytics/internal/mfapi"
	"mutual-fund-analytics/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeSchemeClient struct {
	items []mfapi.SchemeSummary
	err   error
}

func (f fakeSchemeClient) FetchSchemeList(context.Context) ([]mfapi.SchemeSummary, error) {
	return f.items, f.err
}

type memoryFundRepo struct {
	items map[string]domain.Fund
}

func newMemoryFundRepo() *memoryFundRepo {
	return &memoryFundRepo{items: make(map[string]domain.Fund)}
}

func (m *memoryFundRepo) Upsert(ctx context.Context, fund domain.Fund) (domain.Fund, error) {
	if existing, ok := m.items[fund.SchemeCode]; ok {
		existing.Name = fund.Name
		existing.Category = fund.Category
		existing.Active = fund.Active
		m.items[fund.SchemeCode] = existing
		return existing, nil
	}

	m.items[fund.SchemeCode] = fund
	return fund, nil
}

func (m *memoryFundRepo) GetBySchemeCode(ctx context.Context, schemeCode string) (domain.Fund, error) {
	v, ok := m.items[schemeCode]
	if !ok {
		return domain.Fund{}, os.ErrNotExist
	}
	return v, nil
}

func (m *memoryFundRepo) ListActive(ctx context.Context, limit, offset int32) ([]domain.Fund, error) {
	out := make([]domain.Fund, 0, len(m.items))
	for _, item := range m.items {
		if item.Active {
			out = append(out, item)
		}
	}
	return out, nil
}

func TestDiscoveryService_DiscoverAndTrack_FiltersAndStores(t *testing.T) {
	client := fakeSchemeClient{items: []mfapi.SchemeSummary{
		{SchemeCode: "1001", SchemeName: "ICICI Prudential Mid Cap Fund - Direct Plan - Growth"},
		{SchemeCode: "1002", SchemeName: "Axis Mid Cap Fund - Regular Plan - Growth"},
		{SchemeCode: "1003", SchemeName: "HDFC Small Cap Fund - Direct Plan - Growth"},
		{SchemeCode: "1004", SchemeName: "Nippon India Small Cap Fund - Direct Plan - Growth"},
		{SchemeCode: "1001", SchemeName: "ICICI Prudential Mid Cap Fund - Direct Plan - Growth"},
	}}

	repo := newMemoryFundRepo()
	svc := NewDiscoveryService(client, repo)

	result, err := svc.DiscoverAndTrack(context.Background())
	if err != nil {
		t.Fatalf("discover and track: %v", err)
	}

	if result.TotalDiscoveredSchemes != 5 {
		t.Fatalf("expected total discovered 5, got %d", result.TotalDiscoveredSchemes)
	}

	if len(result.TrackedSchemes) != 2 {
		t.Fatalf("expected tracked schemes 2, got %d", len(result.TrackedSchemes))
	}

	if len(repo.items) != 2 {
		t.Fatalf("expected persisted funds 2, got %d", len(repo.items))
	}

	if _, ok := repo.items["1001"]; !ok {
		t.Fatal("expected ICICI scheme to be persisted")
	}
	if _, ok := repo.items["1003"]; !ok {
		t.Fatal("expected HDFC scheme to be persisted")
	}
}

func TestLiveDiscoveryReport(t *testing.T) {
	if os.Getenv("RUN_LIVE_DISCOVERY") != "1" {
		t.Skip("set RUN_LIVE_DISCOVERY=1 to run live discovery report")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/mutual_fund_analytics?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create postgres pool: %v", err)
	}
	defer pool.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := storage.RunMigrations(databaseURL, "../../migrations", logger); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	client, err := mfapi.NewClient(nil, limiter.NewPersistentLimiter(pool))
	if err != nil {
		t.Fatalf("create mfapi client: %v", err)
	}

	funds := storage.NewFundRepository(pool)
	svc := NewDiscoveryService(client, funds)

	result, err := svc.DiscoverAndTrack(ctx)
	if err != nil {
		t.Fatalf("discover and track: %v", err)
	}

	t.Logf("total discovered schemes: %d", result.TotalDiscoveredSchemes)
	t.Logf("tracked schemes count: %d", len(result.TrackedSchemes))
	for _, tracked := range result.TrackedSchemes {
		t.Logf("tracked: %s | %s | %s | %s", tracked.SchemeCode, tracked.AMC, tracked.Category, tracked.SchemeName)
		if _, err := funds.GetBySchemeCode(ctx, tracked.SchemeCode); err != nil {
			t.Fatalf("expected persisted fund for scheme code %s: %v", tracked.SchemeCode, err)
		}
	}

	if len(result.TrackedSchemes) == 0 {
		t.Fatal("expected at least one tracked scheme from live discovery")
	}
}
