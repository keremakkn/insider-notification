package worker

import (
	"testing"
	"time"
)

func TestBackoffGrowsWithJitterAndCap(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Minute)
	for attempt := 1; attempt <= 7; attempt++ {
		lo := time.Second << attempt
		for range 200 {
			d := b.Delay(attempt, 0)
			if d < min(lo, b.Cap) || d >= min(2*lo, b.Cap+1) {
				t.Fatalf("attempt %d: %s outside [%s, %s)", attempt, d, lo, 2*lo)
			}
		}
	}
	// 2^9 s is past the cap; very large attempts must not overflow.
	for _, attempt := range []int{9, 30, 64, 1000} {
		if d := b.Delay(attempt, 0); d != 5*time.Minute {
			t.Errorf("attempt %d: want the 5m cap, got %s", attempt, d)
		}
	}
}

func TestBackoffJitterSpreadsRetries(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Minute)
	seen := map[time.Duration]bool{}
	for range 100 {
		seen[b.Delay(3, 0).Truncate(100*time.Millisecond)] = true
	}
	if len(seen) < 20 {
		t.Fatalf("100 failures retried at only %d distinct times; jitter is not spreading them", len(seen))
	}
}

func TestBackoffHonoursRetryAfter(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Minute)
	b.rand = func() float64 { return 0 }

	if d := b.Delay(1, 30*time.Second); d != 30*time.Second {
		t.Errorf("a longer Retry-After wins: got %s", d)
	}
	if d := b.Delay(1, 500*time.Millisecond); d != 2*time.Second {
		t.Errorf("a shorter Retry-After does not shorten our backoff: got %s", d)
	}
	if d := b.Delay(1, 10*time.Minute); d != 10*time.Minute {
		t.Errorf("the provider may ask for more than our cap: got %s", d)
	}
	if d := b.Delay(1, 48*time.Hour); d != time.Hour {
		t.Errorf("an absurd Retry-After is bounded to an hour: got %s", d)
	}
}
