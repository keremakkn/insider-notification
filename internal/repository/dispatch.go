package repository

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// TakeDue marks up to limit due notifications of one channel and priority
// as enqueued and returns their IDs, oldest due first.
//
// A row is due when it is pending, its next_attempt_at has passed, and it is
// either not in the queue or was put there longer than visibilityTimeout ago
// (the visibility timeout: a worker may have popped it and died before
// claiming it).
//
// Selection and marking are one statement, so they commit together and the
// caller pushes to Redis only after the commit: no transaction stays open
// across a network call to another system. FOR UPDATE SKIP LOCKED lets
// several dispatchers run without taking the same rows.
func (r *Notifications) TakeDue(ctx context.Context, c domain.Channel, p domain.Priority, limit int, visibilityTimeout time.Duration) ([]uuid.UUID, error) {
	rows, err := r.db.Query(ctx, `
		WITH due AS (
			SELECT id FROM notifications
			WHERE status = 'pending'
			  AND channel = $1 AND priority = $2
			  AND next_attempt_at <= now()
			  AND (enqueued_at IS NULL OR enqueued_at < now() - make_interval(secs => $3))
			ORDER BY next_attempt_at
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		)
		UPDATE notifications n SET enqueued_at = now()
		FROM due WHERE n.id = due.id
		RETURNING n.id, n.next_attempt_at`,
		string(c), int16(p), visibilityTimeout.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("take due %s/%s: %w", c, p, err)
	}
	type due struct {
		id uuid.UUID
		at time.Time
	}
	found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (due, error) {
		var d due
		return d, row.Scan(&d.id, &d.at)
	})
	if err != nil {
		return nil, fmt.Errorf("take due %s/%s: %w", c, p, err)
	}
	// RETURNING has no guaranteed order; keep the queue oldest first.
	sort.Slice(found, func(i, j int) bool { return found[i].at.Before(found[j].at) })
	ids := make([]uuid.UUID, len(found))
	for i, d := range found {
		ids[i] = d.id
	}
	return ids, nil
}

// RecoverExpiredLeases releases notifications whose worker stopped
// renewing them (it crashed or hung). Attempts are counted when a worker
// claims a notification, so a recovered row has already used its attempt:
// if none are left it fails instead of looping forever on a message that
// keeps crashing workers.
func (r *Notifications) RecoverExpiredLeases(ctx context.Context) (requeued, failed int, err error) {
	rows, err := r.db.Query(ctx, `
		UPDATE notifications SET
			status           = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
			last_error       = CASE WHEN attempts >= max_attempts THEN 'lease expired on the final attempt' ELSE last_error END,
			enqueued_at      = NULL,
			lease_expires_at = NULL
		WHERE status = 'processing' AND lease_expires_at < now()
		RETURNING status`)
	if err != nil {
		return 0, 0, fmt.Errorf("recover leases: %w", err)
	}
	statuses, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, 0, fmt.Errorf("recover leases: %w", err)
	}
	for _, s := range statuses {
		if s == string(domain.StatusFailed) {
			failed++
		} else {
			requeued++
		}
	}
	return requeued, failed, nil
}
