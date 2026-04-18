package syncer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"mutual-fund-analytics/internal/domain"
	"mutual-fund-analytics/internal/mfapi"
)

type SchemeListFetcher interface {
	FetchSchemeList(ctx context.Context) ([]mfapi.SchemeSummary, error)
}

type DiscoveryService struct {
	client SchemeListFetcher
	funds  domain.FundRepository
}

type TrackedScheme struct {
	SchemeCode string
	SchemeName string
	AMC        string
	Category   string
}

type DiscoveryResult struct {
	TotalDiscoveredSchemes int
	TrackedSchemes         []TrackedScheme
}

func NewDiscoveryService(client SchemeListFetcher, funds domain.FundRepository) *DiscoveryService {
	return &DiscoveryService{client: client, funds: funds}
}

func (s *DiscoveryService) DiscoverAndTrack(ctx context.Context) (DiscoveryResult, error) {
	schemes, err := s.client.FetchSchemeList(ctx)
	if err != nil {
		return DiscoveryResult{}, fmt.Errorf("fetch scheme list: %w", err)
	}

	result := DiscoveryResult{
		TotalDiscoveredSchemes: len(schemes),
		TrackedSchemes:         make([]TrackedScheme, 0),
	}

	seenCodes := make(map[string]struct{}, len(schemes))

	for _, scheme := range schemes {
		code := strings.TrimSpace(scheme.SchemeCode)
		name := strings.TrimSpace(scheme.SchemeName)
		if code == "" || name == "" {
			continue
		}
		if _, seen := seenCodes[code]; seen {
			continue
		}

		match, ok := mfapi.MatchTrackingScheme(name)
		if !ok {
			continue
		}

		persisted, err := s.funds.Upsert(ctx, domain.Fund{
			SchemeCode: code,
			Name:       name,
			Category:   match.Category,
			Active:     true,
		})
		if err != nil {
			return result, fmt.Errorf("upsert discovered fund %s: %w", code, err)
		}

		result.TrackedSchemes = append(result.TrackedSchemes, TrackedScheme{
			SchemeCode: persisted.SchemeCode,
			SchemeName: persisted.Name,
			AMC:        match.AMC,
			Category:   match.Category,
		})
		seenCodes[code] = struct{}{}
	}

	sort.Slice(result.TrackedSchemes, func(i, j int) bool {
		left := result.TrackedSchemes[i]
		right := result.TrackedSchemes[j]
		if left.AMC != right.AMC {
			return left.AMC < right.AMC
		}
		if left.Category != right.Category {
			return left.Category < right.Category
		}
		return left.SchemeName < right.SchemeName
	})

	return result, nil
}
