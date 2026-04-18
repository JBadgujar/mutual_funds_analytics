# Design Decisions

This document records the architectural reasoning behind the current implementation in this repository. It is intentionally implementation-faithful: when behavior in code is incomplete or constrained, this document states that directly.

## 1) Rate Limiting Strategy And Multi-Limit Coordination

### Decision

Use a persistent, database-backed global limiter with one provider row and simultaneous enforcement of:

- 2 requests per second
- 50 requests per minute
- 300 requests per hour

### Why

The upstream API must be protected across process restarts and concurrent goroutines. In-memory counters alone would violate limits during restarts or horizontal concurrency.

### Implementation

- Limiter state is stored in rate_limit_state with second_bucket, minute_bucket, hour_bucket plus counts.
- Acquire executes in one transaction:
  - Ensure row exists.
  - SELECT ... FOR UPDATE on provider row.
  - Roll bucket windows based on current UTC time.
  - Compute wait based on all three windows.
  - Increment all three counters only when allowed.
  - Persist and commit.
- HTTP client calls always pass through limiter gate before each request.

### Proof And Evidence

- Persistent limiter tests verify:
  - limit compliance under long runs
  - boundary transitions
  - concurrent safety
  - persistence across restart simulation
- Schema constraints also cap counter ranges.

## 2) Backfill Orchestration And Resume Mode

### Decision

Model sync as run lifecycle plus per-fund checkpoints and resume from pending or failed states.

### Why

Backfill and incremental jobs may crash mid-run. Work must resume without redoing successful funds and without creating duplicate NAV rows.

### Implementation

- Run lifecycle in sync_runs: queued -> running -> success/failed/partial.
- Per-fund checkpoint in sync_fund_state:
  - status
  - retry_count and next_retry_at
  - last_nav_date and last_synced_at
  - last_error
- Backfill queueing logic:
  - Creates missing fund state rows.
  - Reuses pending or failed work when present.
  - Otherwise schedules funds with no last_nav_date.
- Incremental queueing logic:
  - Schedules active funds; if pending or failed exists, reuses those first.
- Resume behavior:
  - On restart, queued/running runs are marked partial with recovery message.
  - Next run continues from pending/failed fund states.
- Duplicate prevention:
  - nav_history primary key is (fund_id, nav_date) with upsert.
  - Incremental mode skips rows not newer than latest seen NAV date.

### Concurrency Safety

- Global advisory lock prevents parallel orchestrator execution.
- Unique partial index on sync_runs enforces one active run at DB level.

### Proof And Evidence

- Pipeline tests verify:
  - crash and resume from checkpoint
  - no duplicate rows in repeated incremental runs
  - parallel trigger rejection

## 3) Storage Choice Rationale: PostgreSQL Over SQLite

### Decision

Use PostgreSQL as primary and only data store.

### Why

This implementation depends on PostgreSQL-specific features for correctness and operability:

- Advisory locks for singleton run control.
- Row-level locking with SELECT ... FOR UPDATE in limiter transaction.
- Partial unique indexes for active-run singleton.
- Rich index and query plan capabilities for ranking/filter paths.
- Better concurrency semantics for multi-goroutine, API-triggered workloads.

SQLite would require significant redesign for lock semantics and high-concurrency behavior.

## 4) Storage Schema Rationale And Indexing Decisions

### Core Tables

- funds: tracked scheme metadata, active flag.
- nav_history: time-series NAV by fund/date.
- analytics_snapshot: precomputed metrics by fund/window.
- sync_runs: run lifecycle and run-level diagnostics.
- sync_fund_state: resumable per-fund progress and retry metadata.
- rate_limit_state: persistent bucket counters.

### Schema Design Rationale

- TIMESTAMPTZ for event timing and audit fields.
- DATE for NAV dates and analytics boundaries.
- CHECK constraints for data quality (status enums, non-negative counters, valid windows).
- ON CONFLICT upserts for idempotent writes and reruns.

### Indexing Decisions

- NAV latest lookups: index on (fund_id, nav_date DESC).
- Fund list filters:
  - (active, name)
  - prefix-oriented text_pattern_ops indexes for name and (category, name).
- Analytics ranking:
  - partial indexes on window and metric columns for sufficient-data rows.
- Sync queue/status:
  - status + started_at index on sync_runs
  - status + next_retry_at index on sync_fund_state
- Limiter:
  - hour_bucket index for time-window diagnostics.

## 5) Precompute Vs On-Demand Analytics Tradeoff

### Decision

Precompute analytics snapshots per fund and window (1Y/3Y/5Y/10Y), then serve reads from analytics_snapshot.

### Why

Read APIs and ranking queries require low latency and predictable response times. On-demand computation over NAV history on every request would increase latency variance and database load.

### Implementation

- Engine computes rolling return distribution, CAGR distribution, max drawdown, volatility, and data availability fields.
- Snapshot key is (fund_id, window_code), enabling one active row per window per fund.
- Reads for fund analytics and rank avoid runtime heavy math.

### Tradeoff

- Pros: fast and stable API reads.
- Cons: freshness depends on recompute cadence and sync schedule.

## 6) Caching Strategy And Invalidation

### Decision

Use in-process cache for ranking endpoint responses with around 60-second TTL.

### Implementation

- Cache key dimensions:
  - category
  - window
  - sort_by
  - limit
  - offset
- Cache value stores serialized JSON payload bytes.
- Cache is local to process (not distributed).

### Invalidation

- API provides explicit invalidation method for rank cache.
- Analytics engine exposes a post-success callback hook.
- Current wiring does not yet connect runtime recompute flow to API cache invalidation in main server path.

### Current Behavior Note

Because invalidation is not fully wired into the running recompute path, cache coherence currently relies on TTL expiry unless invalidation is called explicitly.

## 7) Handling Insufficient Data And Missing NAV Dates

### Decision

Prefer correctness and explicit insufficiency over extrapolation.

### Implementation

- NAV normalization removes invalid NAV values and de-duplicates same-day points.
- For window start, choose nearest NAV on or before target date.
- If valid rolling/CAGR series cannot be formed, mark insufficient_data true.
- Data availability fields persist start_date, end_date, total_days, nav_data_points.
- Drawdown and volatility are computed only from valid normalized slices.

### Effect

APIs can report meaningful metadata even when metrics are insufficient.

## 8) Failure Handling And Retry Policy

### Upstream API Retry

- mfapi client retries on:
  - transient transport errors
  - HTTP 429
  - HTTP 5xx
- Exponential backoff:
  - max attempts: 4
  - base delay: 200ms
  - max delay: 2s
- Retries still pass through limiter gate (no bypass path).

### Sync Failure Handling

- Per-fund errors are captured in sync_fund_state.last_error.
- Retry scheduling uses exponential backoff from 1 minute up to 1 hour.
- Run finalization:
  - failed when nothing progressed and error occurred
  - partial when some work progressed but errors occurred
  - success otherwise
- Startup recovery marks abandoned active runs as partial.

### HTTP Error Strategy

- Consistent error envelope with code and message.
- Trigger conflict returns HTTP 409 when another run is active.

## 9) Known Limitations And Next-Step Improvements

### Known Limitations

- Ranking cache invalidation hook exists but is not fully wired into runtime recompute flow.
- Scheduler is interval-based from process start; @daily means every 24 hours, not wall-clock midnight.
- Sync status endpoint does not currently expose a total row count for paginated states.
- Retry backoff has no jitter, which can create synchronized retry bursts across instances.
- Cache is per-process only, so multi-instance deployments can serve temporarily divergent rank caches.
- There is no global circuit-breaker around repeated upstream outages.

### Next Improvements

- Wire analytics recompute success callback to rank cache invalidation in main runtime path.
- Add wall-clock cron-style scheduling option (for example local midnight).
- Add jitter to sync and HTTP retry policies.
- Add distributed cache option for rank responses.
- Add status aggregation counters directly from SQL for large state sets.
- Add explicit observability metrics (queue depth, run duration, retry counts, API latency percentiles).

## Final Outline Snapshot

- Rate limiting strategy and multi-limit proof
- Backfill orchestration and resume mode
- PostgreSQL storage rationale
- Schema and indexing rationale
- Precompute analytics rationale
- Caching strategy and invalidation
- Insufficient data and missing NAV handling
- Failure handling and retries
- Limitations and forward plan
