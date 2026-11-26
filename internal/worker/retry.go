package worker

import (
	"math/rand/v2"
	"time"
)

// maxRetryAfter bounds a provider's Retry-After, so a broken header cannot
// park a notification for days.
const maxRetryAfter = time.Hour

// Backoff computes how long to wait before the next attempt:
//
//	min(base·2^attempt + jitter, cap), jitter uniform in [0, base·2^attempt)
//
// The exponential part gives a struggling provider room to recover. The
// jitter spreads out notifications that failed together, so they do not all
// retry in the same instant. The cap keeps late attempts reasonable: without
// it the tenth would wait about 17 minutes.
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	rand func() float64 // [0, 1); replaceable in tests
}

func NewBackoff(base, maxWait time.Duration) Backoff {
	return Backoff{Base: base, Cap: maxWait, rand: rand.Float64}
}

// Delay returns the wait after the given attempt (1 for the first) failed.
// A longer Retry-After from the provider wins: it knows its own limits.
func (b Backoff) Delay(attempt int, retryAfter time.Duration) time.Duration {
	d := b.Cap
	// Beyond 2^30 the shift would overflow; the cap applies long before.
	if attempt < 30 {
		exp := b.Base << attempt
		if exp > 0 && exp < b.Cap {
			d = min(exp+time.Duration(b.rand()*float64(exp)), b.Cap)
		}
	}
	if retryAfter > d {
		d = min(retryAfter, maxRetryAfter)
	}
	return d
}
