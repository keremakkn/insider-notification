package domain

import (
	"time"

	"github.com/google/uuid"
)

const MaxBatchSize = 1000

// Batch records one batch request. Rejected items are never stored, so
// their count lives here.
type Batch struct {
	ID        uuid.UUID
	Total     int
	Accepted  int
	Rejected  int
	CreatedAt time.Time
}

// BatchSummary is a batch with the current status of its notifications.
type BatchSummary struct {
	Batch
	StatusCounts map[Status]int
}

// CreateOutcome is the storage result for one notification of a batch.
type CreateOutcome struct {
	Stored  Notification // what is in the database for this item
	Created bool         // false: the idempotency key already existed
}
