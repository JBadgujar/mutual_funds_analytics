package syncer

import (
	"testing"
	"time"
)

func TestParseScheduleInterval(t *testing.T) {
	tests := []struct {
		name      string
		schedule  string
		enabled   bool
		hasErr    bool
		minMinute bool
	}{
		{name: "default empty", schedule: "", enabled: true},
		{name: "disabled", schedule: "disabled", enabled: false},
		{name: "daily", schedule: "@daily", enabled: true},
		{name: "every", schedule: "@every 2h", enabled: true, minMinute: true},
		{name: "duration", schedule: "90m", enabled: true, minMinute: true},
		{name: "invalid", schedule: "abc", hasErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enabled, interval, err := ParseScheduleInterval(tc.schedule)
			if tc.hasErr {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if enabled != tc.enabled {
				t.Fatalf("expected enabled=%v got=%v", tc.enabled, enabled)
			}
			if tc.enabled && tc.minMinute && interval < time.Minute {
				t.Fatalf("expected interval >= 1m, got %s", interval)
			}
		})
	}
}
