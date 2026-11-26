package domain

import (
	"time"

	"github.com/google/uuid"
)

// StatusChange is published whenever a notification changes status, for
// clients following it live. It is a notice, not the record: PostgreSQL
// stays the source of truth, and a client that misses one reads the
// current state with GET.
type StatusChange struct {
	ID       uuid.UUID  `json:"id"`
	BatchID  *uuid.UUID `json:"batchId,omitempty"`
	Channel  Channel    `json:"channel"`
	Status   Status     `json:"status"`
	Attempts int        `json:"attempts"`
	Error    string     `json:"error,omitempty"`
	At       time.Time  `json:"at"`
}

func ChangeOf(n Notification, at time.Time) StatusChange {
	return StatusChange{ID: n.ID, BatchID: n.BatchID, Channel: n.Channel, Status: n.Status,
		Attempts: n.Attempts, Error: n.LastError, At: at}
}
