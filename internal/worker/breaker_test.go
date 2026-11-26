package worker

import (
	"sync"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *clock, *[]string) {
	c := &clock{t: time.Date(2026, 11, 26, 3, 0, 0, 0, time.UTC)}
	var changes []string
	b := NewBreaker(threshold, cooldown, func(from, to BreakerState) { changes = append(changes, string(from)+"→"+string(to)) })
	b.now = c.now
	return b, c, &changes
}

func fail(b *Breaker, n int) {
	for range n {
		t, _, _ := b.Allow()
		b.Done(t, ResultFailure)
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, _ := newTestBreaker(3, 30*time.Second)
	fail(b, 2)
	t1, _, _ := b.Allow()
	b.Done(t1, ResultSuccess) // a success resets the streak
	fail(b, 2)
	if b.State() != BreakerClosed {
		t.Fatal("failures must be consecutive")
	}
	fail(b, 1)
	if b.State() != BreakerOpen {
		t.Fatal("third consecutive failure should open it")
	}
	if _, ok, wait := b.Allow(); ok || wait != 30*time.Second {
		t.Fatalf("open breaker must refuse for the cool-down, got ok=%v wait=%s", ok, wait)
	}
}

func TestBreakerLetsExactlyOneProbeThrough(t *testing.T) {
	b, c, changes := newTestBreaker(1, 30*time.Second)
	fail(b, 1)
	c.advance(30 * time.Second)

	// Twenty workers ask at once after the cool-down: one probe, nineteen wait.
	var (
		mu      sync.Mutex
		allowed []Ticket
		wg      sync.WaitGroup
	)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tk, ok, _ := b.Allow(); ok {
				mu.Lock()
				allowed = append(allowed, tk)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(allowed) != 1 || !allowed[0].probe {
		t.Fatalf("want exactly one probe, got %d", len(allowed))
	}

	b.Done(allowed[0], ResultSuccess)
	if b.State() != BreakerClosed {
		t.Fatal("a successful probe closes the breaker")
	}
	want := []string{"closed→open", "open→half-open", "half-open→closed"}
	if len(*changes) != 3 || (*changes)[0] != want[0] || (*changes)[1] != want[1] || (*changes)[2] != want[2] {
		t.Fatalf("transitions: %v", *changes)
	}
}

func TestFailedProbeReopens(t *testing.T) {
	b, c, _ := newTestBreaker(1, 30*time.Second)
	fail(b, 1)
	c.advance(30 * time.Second)
	probe, _, _ := b.Allow()
	b.Done(probe, ResultFailure)
	if _, ok, wait := b.Allow(); ok || wait != 30*time.Second {
		t.Fatalf("a failed probe should start a new cool-down, got ok=%v wait=%s", ok, wait)
	}
}

func TestProbeThatSentNothingIsReleased(t *testing.T) {
	b, c, _ := newTestBreaker(1, 30*time.Second)
	fail(b, 1)
	c.advance(30 * time.Second)
	probe, _, _ := b.Allow()
	b.Done(probe, ResultNone) // the queue was empty
	if _, ok, _ := b.Allow(); !ok {
		t.Fatal("the next worker should be able to probe")
	}
}

func TestLateResultsDoNotDecideForTheProbe(t *testing.T) {
	b, c, _ := newTestBreaker(2, 30*time.Second)
	inFlight, _, _ := b.Allow() // started while closed
	fail(b, 2)                  // breaker opens meanwhile
	b.Done(inFlight, ResultSuccess)
	if b.State() != BreakerOpen {
		t.Fatal("a request that started before the breaker opened must not close it")
	}
	c.advance(30 * time.Second)
	if _, ok, _ := b.Allow(); !ok {
		t.Fatal("after the cool-down a probe should be allowed")
	}
}
