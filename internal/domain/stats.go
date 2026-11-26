package domain

import "time"

// Stats is a human-readable snapshot of the system for /api/v1/stats.
type Stats struct {
	Window   time.Duration
	Channels map[Channel]*ChannelStats
}

type ChannelStats struct {
	// Right now.
	QueueDepth map[Priority]int // IDs waiting in Redis
	Pending    int              // waiting overall: in Redis, backing off, scheduled, or not yet dispatched
	NotYetDue  int              // the part of Pending that may not be sent yet: scheduled for later or backing off before a retry
	Processing int

	// Over the window.
	Sent, Failed               int // notifications that finished
	AttemptsOK, AttemptsFailed int // provider calls
	ProviderP50, ProviderP95   time.Duration
	EndToEndP50, EndToEndP95   time.Duration // created → sent
}

// SuccessRate is the share of provider calls in the window that succeeded,
// or nil when there were none.
func (c ChannelStats) SuccessRate() *float64 {
	total := c.AttemptsOK + c.AttemptsFailed
	if total == 0 {
		return nil
	}
	r := float64(c.AttemptsOK) / float64(total)
	return &r
}
