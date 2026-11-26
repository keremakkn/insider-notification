package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Claim moves a notification from pending to processing for one worker. It
// counts the attempt and sets a lease. ok is false when someone else got
// there first or the notification is no longer eligible.
//
// Every condition is checked by the UPDATE itself, so two workers, a cancel
// and a claim, or a stale duplicate in the queue can never both win:
//   - status = 'pending': not cancelled, not already claimed or finished.
//   - next_attempt_at <= now(): a stale copy of the ID left in Redis must not
//     skip a retry's backoff or a scheduled time.
func (r *Notifications) Claim(ctx context.Context, id uuid.UUID, lease time.Duration) (n domain.Notification, ok bool, err error) {
	n, err = scan(r.db.QueryRow(ctx, `
		UPDATE notifications SET
			status = 'processing',
			attempts = attempts + 1,
			lease_expires_at = now() + make_interval(secs => $2)
		WHERE id = $1 AND status = 'pending' AND next_attempt_at <= now()
		RETURNING `+columns, id, lease.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, false, nil
	}
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("claim %s: %w", id, err)
	}
	return n, true, nil
}

// Finish records an attempt and writes its outcome. attempt is the attempts
// value returned by Claim and acts as a fencing token: if the lease expired
// and another worker claimed the notification since, attempts has moved on,
// the UPDATE matches nothing and ErrLeaseLost is returned. The attempt
// itself is still recorded, because the provider call did happen.
func (r *Notifications) Finish(ctx context.Context, id uuid.UUID, attempt int, o domain.Outcome) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("finish %s: %w", id, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var httpStatus *int
	if o.HTTPStatus != 0 {
		httpStatus = &o.HTTPStatus
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (notification_id, attempt_no, http_status, error, duration_ms)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (notification_id, attempt_no) DO NOTHING`,
		id, attempt, httpStatus, o.Error, o.Duration.Milliseconds()); err != nil {
		return fmt.Errorf("record attempt %d of %s: %w", attempt, id, err)
	}

	var tag interface{ RowsAffected() int64 }
	switch o.Status {
	case domain.StatusSent:
		tag, err = tx.Exec(ctx, `
			UPDATE notifications SET status = 'sent', sent_at = now(),
				provider_message_id = NULLIF($3, ''), last_error = NULL, lease_expires_at = NULL
			WHERE id = $1 AND status = 'processing' AND attempts = $2`,
			id, attempt, o.ProviderMessageID)
	case domain.StatusPending:
		tag, err = tx.Exec(ctx, `
			UPDATE notifications SET status = 'pending', next_attempt_at = $3,
				enqueued_at = NULL, lease_expires_at = NULL, last_error = $4
			WHERE id = $1 AND status = 'processing' AND attempts = $2`,
			id, attempt, o.NextAttemptAt, o.Error)
	case domain.StatusFailed:
		tag, err = tx.Exec(ctx, `
			UPDATE notifications SET status = 'failed', lease_expires_at = NULL, last_error = $3
			WHERE id = $1 AND status = 'processing' AND attempts = $2`,
			id, attempt, o.Error)
	default:
		return fmt.Errorf("finish %s: unexpected outcome status %q", id, o.Status)
	}
	if err != nil {
		return fmt.Errorf("finish %s: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("finish %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrLeaseLost
	}
	return nil
}
