package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type Notifications struct {
	db *pgxpool.Pool
}

func NewNotifications(db *pgxpool.Pool) *Notifications {
	return &Notifications{db: db}
}

const columns = `id, batch_id, idempotency_key, request_hash, correlation_id,
	recipient, channel, content, priority,
	status, attempts, max_attempts,
	next_attempt_at, scheduled_at, enqueued_at, lease_expires_at,
	provider_message_id, last_error,
	created_at, updated_at, sent_at`

// Create inserts n. If n carries an idempotency key that already exists, it
// inserts nothing and returns the stored notification with created = false.
// Comparing request hashes is the caller's decision.
//
// The unique index decides the winner, so concurrent requests with the same
// key cannot both create a row; the follow-up SELECT is only a read.
func (r *Notifications) Create(ctx context.Context, n domain.Notification) (stored domain.Notification, created bool, err error) {
	row := r.db.QueryRow(ctx, `
		INSERT INTO notifications (
			id, batch_id, idempotency_key, request_hash, correlation_id,
			recipient, channel, content, priority,
			status, max_attempts, next_attempt_at, scheduled_at, created_at, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+columns,
		n.ID, n.BatchID, n.IdempotencyKey, n.RequestHash, n.CorrelationID,
		n.Recipient, string(n.Channel), n.Content, int16(n.Priority),
		string(n.Status), n.MaxAttempts, n.NextAttemptAt, n.ScheduledAt, n.CreatedAt)

	stored, err = scan(row)
	if err == nil {
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, false, fmt.Errorf("insert notification: %w", err)
	}
	// ON CONFLICT DO NOTHING returns no row: the key exists, read the original.
	stored, err = scan(r.db.QueryRow(ctx,
		`SELECT `+columns+` FROM notifications WHERE idempotency_key = $1`, n.IdempotencyKey))
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("read notification for idempotency key: %w", err)
	}
	return stored, false, nil
}

func (r *Notifications) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scan(r.db.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return n, nil
}

// Cancel moves a pending notification to cancelled in one atomic UPDATE, so
// it cannot race with a worker claiming it. If nothing was updated it tells
// apart "does not exist" (ErrNotFound) from "exists but is no longer pending"
// (ErrNotCancellable, returned together with the current notification).
func (r *Notifications) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scan(r.db.QueryRow(ctx, `
		UPDATE notifications SET status = 'cancelled'
		WHERE id = $1 AND status = 'pending'
		RETURNING `+columns, id))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, fmt.Errorf("cancel notification: %w", err)
	}
	current, err := r.Get(ctx, id)
	if err != nil {
		return domain.Notification{}, err // ErrNotFound or a database error
	}
	return current, domain.ErrNotCancellable
}

// List returns one page, newest first, and the cursor for the next page
// (nil on the last page). It uses keyset pagination on (created_at, id).
func (r *Notifications) List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		add("status = $%d", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = $%d", string(*f.Channel))
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}

	query := `SELECT ` + columns + ` FROM notifications`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	// Fetch one extra row to learn whether another page exists.
	args = append(args, f.Limit+1)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}
	page, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Notification, error) { return scan(row) })
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}

	if len(page) <= f.Limit {
		return page, nil, nil
	}
	page = page[:f.Limit]
	last := page[len(page)-1]
	return page, &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

func scan(row pgx.Row) (domain.Notification, error) {
	var (
		n                                     domain.Notification
		idemKey, reqHash, providerID, lastErr *string
		channel, status                       string
		priority                              int16
	)
	err := row.Scan(
		&n.ID, &n.BatchID, &idemKey, &reqHash, &n.CorrelationID,
		&n.Recipient, &channel, &n.Content, &priority,
		&status, &n.Attempts, &n.MaxAttempts,
		&n.NextAttemptAt, &n.ScheduledAt, &n.EnqueuedAt, &n.LeaseExpiresAt,
		&providerID, &lastErr,
		&n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	if err != nil {
		return domain.Notification{}, err
	}
	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.Priority(priority)
	n.IdempotencyKey = deref(idemKey)
	n.RequestHash = deref(reqHash)
	n.ProviderMessageID = deref(providerID)
	n.LastError = deref(lastErr)
	return n, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
