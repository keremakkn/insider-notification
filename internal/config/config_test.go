package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestDefaultsAreValid(t *testing.T) {
	c, err := load(env(nil))
	if err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
	if c.RateLimitPerChannel != 100 || c.PriorityWeights != [3]int{6, 3, 1} || c.QueueThresholds != [3]int{600, 300, 100} {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestMaxQueueWait(t *testing.T) {
	c, _ := load(env(nil))
	// Every default queue drains in 10 s: 600/60, 300/30, 100/10.
	if got := c.MaxQueueWait(); got != 10*time.Second {
		t.Fatalf("MaxQueueWait = %s, want 10s", got)
	}
}

func TestOverrides(t *testing.T) {
	c, err := load(env(map[string]string{
		"RATE_LIMIT_PER_CHANNEL": "50",
		"PRIORITY_WEIGHTS":       "5, 4, 1",
		"LEASE_DURATION":         "1m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.RateLimitPerChannel != 50 || c.PriorityWeights != [3]int{5, 4, 1} || c.LeaseDuration != time.Minute {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestRejectsUnsafeCombinations(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"lease shorter than provider timeout", map[string]string{"LEASE_DURATION": "3s"}, "LEASE_DURATION"},
		{"visibility timeout shorter than queue wait", map[string]string{"VISIBILITY_TIMEOUT": "5s"}, "VISIBILITY_TIMEOUT"},
		{"zero weight starves a priority", map[string]string{"PRIORITY_WEIGHTS": "6,3,0"}, "starves"},
		{"retry cap below base", map[string]string{"RETRY_CAP": "500ms"}, "RETRY_CAP"},
		{"bad integer", map[string]string{"WORKERS_PER_CHANNEL": "many"}, "not an integer"},
		{"bad duration", map[string]string{"LEASE_DURATION": "30"}, "not a duration"},
		{"wrong number of weights", map[string]string{"PRIORITY_WEIGHTS": "6,3"}, "want 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(env(tt.env))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
