package mfapi

import "testing"

func TestNormalizeSchemeText_AppliesCaseWhitespaceAndAliases(t *testing.T) {
	input := "  KOTAK   Midcap Fund - Direct   Plan - Growth "
	normalized := NormalizeSchemeText(input)

	expected := "kotak mahindra mid cap fund direct plan growth"
	if normalized != expected {
		t.Fatalf("unexpected normalization. expected=%q got=%q", expected, normalized)
	}
}

func TestMatchTrackingScheme(t *testing.T) {
	tests := []struct {
		name          string
		schemeName    string
		expectedMatch bool
		expectedAMC   string
		expectedCat   string
	}{
		{
			name:          "target icici mid cap",
			schemeName:    "ICICI Prudential Mid Cap Fund - Direct Plan - Growth",
			expectedMatch: true,
			expectedAMC:   "ICICI Prudential",
			expectedCat:   "Mid Cap Direct Growth",
		},
		{
			name:          "target kotak alias",
			schemeName:    "Kotak Midcap Fund - Direct Plan - Growth",
			expectedMatch: true,
			expectedAMC:   "Kotak Mahindra",
			expectedCat:   "Mid Cap Direct Growth",
		},
		{
			name:          "target sbi small cap",
			schemeName:    "SBI Small Cap Fund - Direct Plan - Growth",
			expectedMatch: true,
			expectedAMC:   "SBI",
			expectedCat:   "Small Cap Direct Growth",
		},
		{
			name:          "reject non target amc",
			schemeName:    "Nippon India Small Cap Fund - Direct Plan - Growth",
			expectedMatch: false,
		},
		{
			name:          "reject regular plan",
			schemeName:    "Axis Mid Cap Fund - Regular Plan - Growth",
			expectedMatch: false,
		},
		{
			name:          "reject non growth option",
			schemeName:    "HDFC Mid Cap Opportunities Fund - Direct Plan - IDCW",
			expectedMatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, ok := MatchTrackingScheme(tc.schemeName)
			if ok != tc.expectedMatch {
				t.Fatalf("expected match=%v got=%v", tc.expectedMatch, ok)
			}

			if !tc.expectedMatch {
				return
			}

			if match.AMC != tc.expectedAMC {
				t.Fatalf("expected amc=%q got=%q", tc.expectedAMC, match.AMC)
			}
			if match.Category != tc.expectedCat {
				t.Fatalf("expected category=%q got=%q", tc.expectedCat, match.Category)
			}
		})
	}
}
