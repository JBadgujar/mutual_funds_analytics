# Mutual Fund Analytics - Prompt Chunks Playbook (PostgreSQL Edition)


## Chunk 1 - Project 

```text
Task: Initialize production-ready Go project for Mutual Fund Analytics service using PostgreSQL.

Implement:
1) Go module setup and folder layout:
   - cmd/server
   - internal/config
   - internal/http
   - internal/storage
   - internal/mfapi
   - internal/limiter
   - internal/syncer
   - internal/analytics
   - internal/domain
   - migrations
2) Basic server entrypoint with health route GET /health
3) Config loader (env + defaults):
   - PORT
   - DATABASE_URL
   - LOG_LEVEL
   - SYNC_SCHEDULE
4) PostgreSQL connection setup (pool-based, retry on startup)
5) Migration runner bootstrapping for PostgreSQL
6) Makefile with targets: run, lint, migrate, db-up, db-down
7) Local development support:
   - .env.example
   - docker-compose.yml with PostgreSQL service

Acceptance criteria:
- go run ./cmd/server starts and serves GET /health = 200
- App starts with fresh PostgreSQL database and runs migrations automatically

Return:
- Changed files list
- Commands run + outcomes
- Any TODOs left intentionally
```

---

## Chunk 2 - PostgreSQL Schema + Repositories

```text
Task: Implement PostgreSQL schema and repository layer based on IMPLEMENTATION_PLAN.md.

Implement:
1) SQL migrations for tables:
   - funds
   - nav_history
   - analytics_snapshot
   - sync_runs
   - sync_fund_state
   - rate_limit_state
2) Use PostgreSQL-appropriate types and constraints:
   - TIMESTAMPTZ for event times
   - DATE for NAV date fields
   - CHECK constraints where useful
3) Required indexes for read paths and ranking performance
4) Repository interfaces + PostgreSQL implementations for:
   - FundRepository
   - NavRepository
   - AnalyticsRepository
   - SyncRepository
   - RateLimitStateRepository
5) Upsert semantics using ON CONFLICT for:
   - funds
   - nav_history
   - analytics_snapshot
   - sync_fund_state
6) Transaction helper for atomic operations

Acceptance criteria:
- Migrations apply cleanly on fresh PostgreSQL DB
- Migrations are versioned and rerunnable safely
- Repository CRUD and upsert flows are validated

Return:
- DDL summary
- Repo method list
- Validation summary
```

---

## Chunk 3 - Persistent Multi-Limit Rate Limiter

```text
Task: Implement provably safe persistent rate limiter for mfapi calls.

Implement:
1) Global Acquire(ctx) gate that enforces all 3 limits simultaneously:
   - 2 requests/sec
   - 50 requests/min
   - 300 requests/hour
2) Persistent counters in rate_limit_state with bucket rollover logic:
   - second_bucket, minute_bucket, hour_bucket and counts
3) Atomic check + increment in one DB transaction
4) Row-level lock strategy (SELECT FOR UPDATE) to avoid race conditions
5) Wait duration calculation when blocked
6) Thread-safe behavior for concurrent callers
7) Integration in HTTP client wrapper so no outbound call bypasses limiter

Tests:
- No >2 requests in any 1-second bucket
- No >50 requests in any 1-minute bucket
- No >300 requests in any 1-hour bucket
- Correct behavior at boundary transitions
- Concurrency safety under parallel goroutines
- Restart simulation: state persistence remains respected

Acceptance criteria:
- Compliance evidence is provided
- Limiter is the only path to external mfapi requests

Return:
- Compliance explanation
- Key compliance evidence and outputs
```

---

## Chunk 4 - mfapi Client + Scheme Discovery

```text
Task: Build mfapi client and scheme discovery/filtering for target AMCs + categories.

Implement:
1) mfapi client methods:
   - FetchSchemeList()
   - FetchSchemeNavHistory(code)
2) Retry policy for transient failures (network/5xx/429) with bounded exponential backoff
3) Ensure retries always pass through limiter (no bypass)
4) Normalization helpers for scheme matching:
   - case normalization
   - whitespace cleanup
   - alias mapping (midcap/mid cap, kotak/kotak mahindra)
5) Discovery service that filters to:
   - AMCs: ICICI Prudential, HDFC, Axis, SBI, Kotak Mahindra
   - Categories: Mid Cap Direct Growth, Small Cap Direct Growth
6) Save discovered tracked funds into funds table

Acceptance criteria:
- Discovery run stores expected filtered funds
- Matching rules are validated
- Retry logic never bypasses limiter

Return:
- Total discovered schemes
- Filtered tracked schemes list
- Any ambiguous matching assumptions
```

---

## Chunk 5 - Backfill + Incremental Sync + Resumability

```text
Task: Implement sync orchestration pipeline with crash-safe resumability.

Implement:
1) Sync run lifecycle in sync_runs:
   - queued -> running -> success/failed/partial
2) Per-fund status tracking in sync_fund_state
3) Backfill job:
   - fetch full NAV history per tracked fund
   - upsert nav_history (fund_code, nav_date)
   - checkpoint after each fund
4) Incremental sync job:
   - fetch latest NAV history
   - insert only unseen NAV rows
5) Resumability:
   - on restart/new trigger continue from pending/failed funds
6) Concurrency control:
   - enforce only one active sync run
   - use DB-backed lock/advisory-lock strategy

Acceptance criteria:
- Simulated crash and rerun resumes from checkpoint
- Duplicate NAV rows are not created
- Parallel trigger attempts do not create concurrent active runs

Return:
- Pipeline flow summary
- Crash-resume proof steps
```

---

## Chunk 6 - Analytics Engine (1Y/3Y/5Y/10Y)

```text
Task: Implement precompute analytics engine for all required windows.

Implement:
1) Window computations for: 1Y, 3Y, 5Y, 10Y
2) Rolling returns distribution:
   - min, max, median, p25, p75
3) Max drawdown:
   - worst, peak-to-trough, decline (%)
4) CAGR distribution:
   - min, max, median
5) Data availability fields:
   - start_date, end_date, total_days, nav_data_points
6) Trading-day handling:
   - nearest NAV on or before target start date
   - skip invalid periods
   - flag/handle insufficient data
7) Persist results to analytics_snapshot (upsert by fund_code + window)
8) Optional but recommended (from service background):
   - annualized volatility per window as additional metric

Acceptance criteria:
- Metrics are reproducible
- Snapshot rows generated for each eligible fund/window

Return:
- Formula details used
- Validation outputs and tolerances
```

---

## Chunk 7 - API Endpoints: Funds + Analytics

```text
Task: Implement required read APIs using precomputed data.

Implement endpoints:
1) GET /funds (?category=, ?amc=)
2) GET /funds/{code}
3) GET /funds/{code}/analytics?window=1Y|3Y|5Y|10Y

Requirements:
- Response shape aligned with requirement examples
- Validation for query/path params
- Consistent error format and HTTP codes
- Fast read paths using indexed PostgreSQL queries

Acceptance criteria:
- Endpoint contract requirements pass
- 200/400/404 behavior verified

Return:
- Endpoint request/response samples
- Notes on any schema mismatch vs sample payloads
```

---

## Chunk 8 - API Endpoints: Ranking + Performance

```text
Task: Implement ranking endpoint with low-latency behavior.

Implement:
1) GET /funds/rank
   - params: category, window, sort_by, limit, offset
   - default sort_by=median_return
   - support sort_by=max_drawdown
2) Ranking query optimized with proper joins/indexes
3) In-memory cache for rank responses (TTL around 60s)
4) Cache invalidation on successful analytics recompute

Performance target:
- Keep API response time under 200ms for expected local load

Acceptance criteria:
- Ranking endpoint contract requirements pass
- Sub-200ms target is confirmed with warm-cache validation

Return:
- Query design summary
- Cache key strategy
- Measured latency numbers
```

---

## Chunk 9 - Sync Trigger/Status APIs + Scheduler

```text
Task: Expose sync control plane APIs and scheduler support.

Implement endpoints:
1) POST /sync/trigger
   - start a sync run if none active
   - return run id + status
2) GET /sync/status
   - current/last run state
   - per-fund sync states

Also implement:
- Config-driven scheduler for daily incremental sync
- Safe shutdown for in-flight jobs
- Structured logs for run start/end/failure

Acceptance criteria:
- Manual trigger works
- Parallel runs are prevented
- Status endpoint reflects real-time progress and last errors

Return:
- Trigger/status flow
- Scheduler behavior and config knobs
```

---

## Chunk 11 - DESIGN_DECISIONS.md

```text
Task: Create DESIGN_DECISIONS.md covering required architectural reasoning.

Must include:
1) Rate limiting strategy and proof of multi-limit coordination
2) Backfill orchestration and resume mod
3) Storage choice rationale (why PostgreSQL over SQLite)
4) Storage schema rationale and indexing decisions
5) Precompute vs on-demand analytics tradeoff
6) Caching strategy and invalidation
7) Handling insufficient data and missing NAV dates
8) Failure handling and retry policy
9) Known limitations and next-step improvements

Acceptance criteria:
- Document is implementation-faithful and specific
- Matches final code behavior

Return:
- Final doc outline and highlights
```

---