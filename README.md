# Mutual Fund Analytics

A Go + PostgreSQL service for mutual fund data ingestion and analytics.
It syncs NAV history from mfapi with rate-limited, resumable sync workflows.
It precomputes 1Y/3Y/5Y/10Y analytics snapshots for fast API reads.
It exposes fund listing, fund analytics, ranking, and sync control/status endpoints.

## Setup And Run

Prerequisites: Go, Docker, and Make.

```bash
make db-up
make migrate
make sync-backfill-recompute
make run
```

If port 8080 is blocked on your machine (common on Windows), run:

```powershell
$env:PORT='9000'; go run ./cmd/server
```

## cURL Collection

Set base URL:

```bash
BASE_URL="http://localhost:9000"
```

Core endpoints:

```bash
curl -i "$BASE_URL/health"
curl -i "$BASE_URL/funds"
curl -i "$BASE_URL/funds?category=Mid%20Cap%20Direct%20Growth&amc=Axis"
curl -i "$BASE_URL/funds/3001"
curl -i "$BASE_URL/funds/3001/analytics?window=3Y"
curl -i "$BASE_URL/funds/rank"
curl -i "$BASE_URL/funds/rank?window=5Y&sort_by=max_drawdown&limit=20&offset=0"
```

Sync endpoints:

```bash
curl -i -X POST "$BASE_URL/sync/trigger"
curl -i "$BASE_URL/sync/status"
```
