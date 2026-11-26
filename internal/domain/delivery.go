package domain

import (
	"errors"
	"time"
)

// ErrLeaseLost: the worker's claim is no longer current (the lease expired
// and the notification was claimed again), so its result must not be written.
var ErrLeaseLost = errors.New("lease lost: the notification was reclaimed")

// Outcome is the result of one delivery attempt.
type Outcome struct {
	Status            Status // StatusSent, StatusPending (retry) or StatusFailed
	ProviderMessageID string
	NextAttemptAt     time.Time // for a retry
	Error             string
	HTTPStatus        int // 0 when no response arrived
	Duration          time.Duration
}
