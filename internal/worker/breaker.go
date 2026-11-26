package worker

import (
	"sync"
	"time"
)

type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"    // normal operation
	BreakerOpen     BreakerState = "open"      // provider considered down: stop sending
	BreakerHalfOpen BreakerState = "half-open" // cool-down over: one probe may go through
)

// Result of a delivery, as the breaker sees it.
type Result int

const (
	ResultNone    Result = iota // nothing was sent (empty queue, claim lost)
	ResultSuccess               // the provider answered: 2xx, or a 4xx that blames the message
	ResultFailure               // the provider is struggling: 5xx, 429, timeout, no connection
)

// Breaker is a per-channel circuit breaker. After Threshold consecutive
// failures it opens and workers stop taking messages for Cooldown, so a
// provider that is down is not hammered and messages do not burn their
// attempts. Then exactly one probe is let through: success closes the
// breaker, failure opens it again.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	onChange  func(from, to BreakerState)

	mu        sync.Mutex
	state     BreakerState
	failures  int
	openUntil time.Time
	probing   bool
}

func NewBreaker(threshold int, cooldown time.Duration, onChange func(from, to BreakerState)) *Breaker {
	if onChange == nil {
		onChange = func(BreakerState, BreakerState) {}
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, onChange: onChange, state: BreakerClosed}
}

// Ticket is permission to take one message. Pass it back to Done.
type Ticket struct{ probe bool }

// Allow reports whether a worker may take a message now. When it may not,
// wait says how long until that could change.
func (b *Breaker) Allow() (t Ticket, ok bool, wait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return Ticket{}, true, 0
	case BreakerOpen:
		if left := b.openUntil.Sub(b.now()); left > 0 {
			return Ticket{}, false, left
		}
		b.set(BreakerHalfOpen)
		fallthrough
	default: // half-open
		if b.probing {
			return Ticket{}, false, b.cooldown / 10 // the probe is out; check again soon
		}
		b.probing = true
		return Ticket{probe: true}, true, 0
	}
}

// Done reports what happened with a ticket's message.
func (b *Breaker) Done(t Ticket, r Result) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.probe {
		b.probing = false
		switch r {
		case ResultSuccess:
			b.failures = 0
			b.set(BreakerClosed)
		case ResultFailure:
			b.trip()
		}
		// ResultNone: the probe found nothing to send; the next caller probes.
		return
	}
	if b.state != BreakerClosed {
		return // a request that started before the breaker opened; the probe decides now
	}
	switch r {
	case ResultSuccess:
		b.failures = 0
	case ResultFailure:
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	}
}

func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.openUntil = b.now().Add(b.cooldown)
	b.set(BreakerOpen)
}

func (b *Breaker) set(s BreakerState) {
	if s == b.state {
		return
	}
	from := b.state
	b.state = s
	b.onChange(from, s)
}
