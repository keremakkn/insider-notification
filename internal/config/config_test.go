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
		{"negative provider limit", map[string]string{"PROVIDER_RATE_LIMIT": "-1"}, "PROVIDER_RATE_LIMIT"},
		// 20 workers queued at 1/s wait 20 s; with the 5 s provider timeout that outlasts a 20 s lease.
		{"channel rate so low the lease runs out", map[string]string{"RATE_LIMIT_PER_CHANNEL": "1", "QUEUE_THRESHOLDS": "6,3,1", "LEASE_DURATION": "20s"}, "rate-limit wait"},
		{"explicit thresholds too long for a slow provider",
			map[string]string{"PROVIDER_RATE_LIMIT": "5", "QUEUE_THRESHOLDS": "600,300,100"}, "VISIBILITY_TIMEOUT"},
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

// Without a provider limit nothing changes: each channel drains at its own rate.
func TestNoProviderLimitByDefault(t *testing.T) {
	c, _ := load(env(nil))
	if c.ProviderRateLimit != 0 || c.ChannelDrainRate() != 100 || c.MaxLimiterWait() != 200*time.Millisecond {
		t.Fatalf("provider=%d drain=%v wait=%s", c.ProviderRateLimit, c.ChannelDrainRate(), c.MaxLimiterWait())
	}
}

// A provider limit (free webhook.site) is shared by the three channels, and
// the default queue thresholds shrink with it so a full queue still drains
// in about 10 s instead of outliving the visibility timeout.
func TestProviderLimitShrinksQueues(t *testing.T) {
	c, err := load(env(map[string]string{"PROVIDER_RATE_LIMIT": "6"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.ChannelDrainRate() != 2 { // 6/s over three channels
		t.Fatalf("ChannelDrainRate = %v, want 2", c.ChannelDrainRate())
	}
	if c.QueueThresholds != [3]int{12, 6, 2} {
		t.Fatalf("QueueThresholds = %v, want [12 6 2]", c.QueueThresholds)
	}
	if got := c.MaxQueueWait(); got != 10*time.Second {
		t.Fatalf("MaxQueueWait = %s, want 10s", got)
	}
	// The quota is waited for before the claim, so it does not lengthen
	// the wait that a lease must cover.
	if got := c.MaxLimiterWait(); got != 200*time.Millisecond {
		t.Fatalf("MaxLimiterWait = %s, want 200ms", got)
	}
}

// Even a very low quota is safe: waiting for it holds no lease.
func TestLowProviderLimitIsAccepted(t *testing.T) {
	c, err := load(env(map[string]string{"PROVIDER_RATE_LIMIT": "1"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.QueueThresholds != [3]int{2, 1, 1} {
		t.Fatalf("QueueThresholds = %v, want [2 1 1]", c.QueueThresholds)
	}
}

// A provider limit above the sum of the channel limits never binds.
func TestGenerousProviderLimitChangesNothing(t *testing.T) {
	c, err := load(env(map[string]string{"PROVIDER_RATE_LIMIT": "1000"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.ChannelDrainRate() != 100 || c.QueueThresholds != [3]int{600, 300, 100} {
		t.Fatalf("drain=%v thresholds=%v", c.ChannelDrainRate(), c.QueueThresholds)
	}
}
