package analytics

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"mutual-fund-analytics/internal/domain"
)

const fundBatchSize int32 = 500

type WindowSpec struct {
	Code  string
	Years int
}

var DefaultWindows = []WindowSpec{
	{Code: "1Y", Years: 1},
	{Code: "3Y", Years: 3},
	{Code: "5Y", Years: 5},
	{Code: "10Y", Years: 10},
}

type NAVPoint struct {
	Date time.Time
	NAV  float64
}

type Engine struct {
	funds     domain.FundRepository
	navs      domain.NavRepository
	analytics domain.AnalyticsRepository
	nowFn     func() time.Time
	windows   []WindowSpec
	onSuccess func()
}

type PrecomputeResult struct {
	FundsProcessed        int
	SnapshotsGenerated    int
	InsufficientSnapshots int
}

func NewEngine(funds domain.FundRepository, navs domain.NavRepository, analyticsRepo domain.AnalyticsRepository) *Engine {
	return &Engine{
		funds:     funds,
		navs:      navs,
		analytics: analyticsRepo,
		nowFn:     time.Now,
		windows:   append([]WindowSpec(nil), DefaultWindows...),
	}
}

func (e *Engine) SetOnSuccessfulRecompute(fn func()) {
	e.onSuccess = fn
}

func (e *Engine) RecomputeAll(ctx context.Context) (PrecomputeResult, error) {
	allFunds, err := e.listActiveFunds(ctx)
	if err != nil {
		return PrecomputeResult{}, err
	}

	result := PrecomputeResult{}
	for _, fund := range allFunds {
		navRows, err := e.navs.ListByFundID(ctx, fund.ID)
		if err != nil {
			return result, fmt.Errorf("list nav history for fund %s: %w", fund.SchemeCode, err)
		}

		points := normalizeNAVPoints(navRows)
		for _, window := range e.windows {
			snapshot := computeWindowSnapshot(points, window)
			snapshot.FundID = fund.ID
			if snapshot.AsOfDate.IsZero() {
				snapshot.AsOfDate = e.nowFn().UTC()
			}

			if err := e.analytics.Upsert(ctx, snapshot); err != nil {
				return result, fmt.Errorf("upsert analytics snapshot for fund %s window %s: %w", fund.SchemeCode, window.Code, err)
			}

			result.SnapshotsGenerated++
			if snapshot.InsufficientData {
				result.InsufficientSnapshots++
			}
		}

		result.FundsProcessed++
	}

	if e.onSuccess != nil {
		e.onSuccess()
	}

	return result, nil
}

func (e *Engine) listActiveFunds(ctx context.Context) ([]domain.Fund, error) {
	out := make([]domain.Fund, 0)
	offset := int32(0)

	for {
		chunk, err := e.funds.ListActive(ctx, fundBatchSize, offset)
		if err != nil {
			return nil, fmt.Errorf("list active funds at offset %d: %w", offset, err)
		}
		if len(chunk) == 0 {
			break
		}

		out = append(out, chunk...)
		offset += int32(len(chunk))
	}

	return out, nil
}

func computeWindowSnapshot(points []NAVPoint, window WindowSpec) domain.AnalyticsSnapshot {
	now := time.Now().UTC()
	snapshot := domain.AnalyticsSnapshot{
		WindowCode:       window.Code,
		AsOfDate:         now,
		InsufficientData: true,
	}

	if len(points) == 0 {
		return snapshot
	}

	last := points[len(points)-1]
	snapshot.AsOfDate = last.Date

	rollingReturns := make([]float64, 0)
	cagrs := make([]float64, 0)

	for endIdx := 0; endIdx < len(points); endIdx++ {
		startIdx, ok := findStartIndexOnOrBefore(points, endIdx, window.Years)
		if !ok || startIdx >= endIdx {
			continue
		}

		start := points[startIdx]
		end := points[endIdx]
		if start.NAV <= 0 || end.NAV <= 0 {
			continue
		}

		days := int(end.Date.Sub(start.Date).Hours() / 24)
		if days <= 0 {
			continue
		}

		ratio := end.NAV / start.NAV
		rollingReturns = append(rollingReturns, ratio-1)
		cagrs = append(cagrs, math.Pow(ratio, 365.2425/float64(days))-1)
	}

	latestStartIdx, latestOk := findStartIndexOnOrBefore(points, len(points)-1, window.Years)
	if latestOk {
		startDate := points[latestStartIdx].Date
		endDate := points[len(points)-1].Date
		snapshot.StartDate = &startDate
		snapshot.EndDate = &endDate
		snapshot.TotalDays = int32(int(endDate.Sub(startDate).Hours() / 24))
		snapshot.NAVDataPoints = int32(len(points[latestStartIdx:]))

		decline, peakDate, troughDate := computeMaxDrawdown(points[latestStartIdx:])
		snapshot.MaxDrawdownDeclinePct = decline
		snapshot.MaxDrawdownPeakDate = peakDate
		snapshot.MaxDrawdownTroughDate = troughDate
		snapshot.AnnualizedVolatility = computeAnnualizedVolatility(points[latestStartIdx:])
	}

	if len(rollingReturns) == 0 || len(cagrs) == 0 || !latestOk {
		return snapshot
	}

	snapshot.RollingReturnMin = minValue(rollingReturns)
	snapshot.RollingReturnMax = maxValue(rollingReturns)
	snapshot.RollingReturnMedian = percentile(rollingReturns, 0.5)
	snapshot.RollingReturnP25 = percentile(rollingReturns, 0.25)
	snapshot.RollingReturnP75 = percentile(rollingReturns, 0.75)

	snapshot.CAGRMin = minValue(cagrs)
	snapshot.CAGRMax = maxValue(cagrs)
	snapshot.CAGRMedian = percentile(cagrs, 0.5)
	snapshot.InsufficientData = false

	return snapshot
}

func normalizeNAVPoints(rows []domain.NAVHistory) []NAVPoint {
	if len(rows) == 0 {
		return nil
	}

	points := make([]NAVPoint, 0, len(rows))
	for _, row := range rows {
		if row.NAV <= 0 {
			continue
		}

		points = append(points, NAVPoint{
			Date: row.NAVDate.UTC(),
			NAV:  row.NAV,
		})
	}

	if len(points) == 0 {
		return nil
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Date.Before(points[j].Date)
	})

	deduped := make([]NAVPoint, 0, len(points))
	for _, point := range points {
		n := len(deduped)
		if n > 0 && sameDate(deduped[n-1].Date, point.Date) {
			deduped[n-1] = point
			continue
		}
		deduped = append(deduped, point)
	}

	return deduped
}

func sameDate(left, right time.Time) bool {
	ly, lm, ld := left.Date()
	ry, rm, rd := right.Date()
	return ly == ry && lm == rm && ld == rd
}

func findStartIndexOnOrBefore(points []NAVPoint, endIdx int, years int) (int, bool) {
	if len(points) == 0 || endIdx < 0 || endIdx >= len(points) {
		return 0, false
	}

	target := points[endIdx].Date.AddDate(-years, 0, 0)
	lo, hi := 0, endIdx
	best := -1

	for lo <= hi {
		mid := (lo + hi) / 2
		if points[mid].Date.After(target) {
			hi = mid - 1
			continue
		}

		best = mid
		lo = mid + 1
	}

	if best == -1 {
		return 0, false
	}

	return best, true
}

func computeMaxDrawdown(points []NAVPoint) (float64, *time.Time, *time.Time) {
	if len(points) == 0 {
		return 0, nil, nil
	}

	peakNAV := points[0].NAV
	peakDate := points[0].Date
	worstDecline := 0.0
	var worstPeakDate *time.Time
	var worstTroughDate *time.Time

	for _, point := range points {
		if point.NAV > peakNAV {
			peakNAV = point.NAV
			peakDate = point.Date
		}

		if peakNAV <= 0 {
			continue
		}

		decline := (peakNAV - point.NAV) / peakNAV
		if decline > worstDecline {
			worstDecline = decline
			peakCopy := peakDate
			troughCopy := point.Date
			worstPeakDate = &peakCopy
			worstTroughDate = &troughCopy
		}
	}

	return worstDecline, worstPeakDate, worstTroughDate
}

func computeAnnualizedVolatility(points []NAVPoint) float64 {
	if len(points) < 2 {
		return 0
	}

	returns := make([]float64, 0, len(points)-1)
	for i := 1; i < len(points); i++ {
		prev := points[i-1].NAV
		curr := points[i].NAV
		if prev <= 0 || curr <= 0 {
			continue
		}
		returns = append(returns, math.Log(curr/prev))
	}

	if len(returns) < 2 {
		return 0
	}

	mean := 0.0
	for _, r := range returns {
		mean += r
	}
	mean /= float64(len(returns))

	variance := 0.0
	for _, r := range returns {
		delta := r - mean
		variance += delta * delta
	}
	variance /= float64(len(returns) - 1)

	return math.Sqrt(variance) * math.Sqrt(252)
}

func minValue(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	best := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] < best {
			best = values[i]
		}
	}

	return best
}

func maxValue(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	best := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] > best {
			best = values[i]
		}
	}

	return best
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return minValue(values)
	}
	if p >= 1 {
		return maxValue(values)
	}

	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	position := p * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}

	weight := position - float64(lower)
	return sorted[lower] + weight*(sorted[upper]-sorted[lower])
}
