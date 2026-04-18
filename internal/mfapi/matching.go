package mfapi

import (
	"regexp"
	"strings"
)

type SchemeMatch struct {
	AMC      string
	Category string
}

var punctuationRegex = regexp.MustCompile(`[^a-z0-9]+`)

var amcMatchers = []struct {
	Canonical string
	Tokens    []string
}{
	{Canonical: "ICICI Prudential", Tokens: []string{"icici prudential"}},
	{Canonical: "HDFC", Tokens: []string{"hdfc"}},
	{Canonical: "Axis", Tokens: []string{"axis"}},
	{Canonical: "SBI", Tokens: []string{"sbi"}},
	{Canonical: "Kotak Mahindra", Tokens: []string{"kotak mahindra", "kotak"}},
}

func NormalizeSchemeText(input string) string {
	normalized := strings.ToLower(strings.TrimSpace(input))
	normalized = punctuationRegex.ReplaceAllString(normalized, " ")
	normalized = strings.Join(strings.Fields(normalized), " ")

	aliasReplacements := []struct {
		from string
		to   string
	}{
		{from: "midcap", to: "mid cap"},
		{from: "smallcap", to: "small cap"},
		{from: "kotak ", to: "kotak mahindra "},
	}

	padded := " " + normalized + " "
	for _, alias := range aliasReplacements {
		padded = strings.ReplaceAll(padded, " "+alias.from, " "+alias.to)
	}

	normalized = strings.Join(strings.Fields(padded), " ")
	return normalized
}

func MatchTrackingScheme(schemeName string) (SchemeMatch, bool) {
	normalized := NormalizeSchemeText(schemeName)
	if normalized == "" {
		return SchemeMatch{}, false
	}

	amc, ok := matchAMC(normalized)
	if !ok {
		return SchemeMatch{}, false
	}

	category, ok := matchCategory(normalized)
	if !ok {
		return SchemeMatch{}, false
	}

	return SchemeMatch{AMC: amc, Category: category}, true
}

func matchAMC(normalized string) (string, bool) {
	for _, candidate := range amcMatchers {
		for _, token := range candidate.Tokens {
			if strings.Contains(normalized, token) {
				return candidate.Canonical, true
			}
		}
	}

	return "", false
}

func matchCategory(normalized string) (string, bool) {
	if !strings.Contains(normalized, "direct") || !strings.Contains(normalized, "growth") {
		return "", false
	}

	switch {
	case strings.Contains(normalized, "mid cap"):
		return "Mid Cap Direct Growth", true
	case strings.Contains(normalized, "small cap"):
		return "Small Cap Direct Growth", true
	default:
		return "", false
	}
}
