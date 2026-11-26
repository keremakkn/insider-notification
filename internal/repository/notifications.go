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
	recipient, channel, content, priority, template_id,
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
	stored, err = scan(r.db.QueryRow(ctx, insertSQL, insertArgs(n)...))
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

const insertSQL = `
	INSERT INTO notifications (
		id, batch_id, idempotency_key, request_hash, correlation_id,
		recipient, channel, content, priority, template_id,
		status, max_attempts, next_attempt_at, scheduled_at, created_at, updated_at)
	VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)
	ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
	RETURNING ` + columns

func insertArgs(n domain.Notification) []any {
	return []any{
		n.ID, n.BatchID, n.IdempotencyKey, n.RequestHash, n.CorrelationID,
		n.Recipient, string(n.Channel), n.Content, int16(n.Priority), n.TemplateID,
		string(n.Status), n.MaxAttempts, n.NextAttemptAt, n.ScheduledAt, n.CreatedAt,
	}
}

// CreateBatch stores a batch and its notifications in one transaction.
//
// Items whose idempotency key already exists insert nothing. For each of
// them, isReplay (the caller's business rule) decides whether the stored
// notification is a legitimate replay (counted as accepted) or a key reused
// for a different request (counted as rejected). b.Accepted and b.Rejected
// must already count the items that failed validation; they are adjusted
// for rejected replays. If nothing ends up accepted, the transaction is
// rolled back and no batch is stored.
//
// The inserts are pipelined: one network round trip for the whole batch.
func (r *Notifications) CreateBatch(
	ctx context.Context,
	b domain.Batch,
	items []domain.Notification,
	isReplay func(item, stored domain.Notification) bool,
) ([]domain.CreateOutcome, domain.Batch, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, domain.Batch{}, fmt.Errorf("begin batch: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if err := tx.QueryRow(ctx, `
		INSERT INTO batches (id, total, accepted, rejected, created_at)
		VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
		b.ID, b.Total, b.Accepted, b.Rejected, b.CreatedAt).Scan(&b.CreatedAt); err != nil {
		return nil, domain.Batch{}, fmt.Errorf("insert batch: %w", err)
	}

	pipe := &pgx.Batch{}
	for _, n := range items {
		pipe.Queue(insertSQL, insertArgs(n)...)
	}
	results := tx.SendBatch(ctx, pipe)
	outcomes := make([]domain.CreateOutcome, len(items))
	var conflictKeys []string
	for i := range items {
		stored, err := scan(results.QueryRow())
		switch {
		case err == nil:
			outcomes[i] = domain.CreateOutcome{Stored: stored, Created: true}
		case errors.Is(err, pgx.ErrNoRows):
			conflictKeys = append(conflictKeys, items[i].IdempotencyKey)
		default:
			results.Close()
			return nil, domain.Batch{}, fmt.Errorf("insert batch item %d: %w", i, err)
		}
	}
	if err := results.Close(); err != nil {
		return nil, domain.Batch{}, fmt.Errorf("insert batch items: %w", err)
	}

	if len(conflictKeys) > 0 {
		existing, err := r.byKeys(ctx, tx, conflictKeys)
		if err != nil {
			return nil, domain.Batch{}, err
		}
		for i, n := range items {
			if outcomes[i].Created {
				continue
			}
			stored := existing[n.IdempotencyKey]
			outcomes[i].Stored = stored
			if !isReplay(n, stored) {
				b.Accepted--
				b.Rejected++
			}
		}
		if b.Accepted == 0 {
			return outcomes, domain.Batch{}, nil // rollback: nothing new, no empty batch row
		}
		if _, err := tx.Exec(ctx, `UPDATE batches SET accepted = $2, rejected = $3 WHERE id = $1`,
			b.ID, b.Accepted, b.Rejected); err != nil {
			return nil, domain.Batch{}, fmt.Errorf("update batch counts: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, domain.Batch{}, fmt.Errorf("commit batch: %w", err)
	}
	return outcomes, b, nil
}

func (r *Notifications) byKeys(ctx context.Context, tx pgx.Tx, keys []string) (map[string]domain.Notification, error) {
	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM notifications WHERE idempotency_key = ANY($1)`, keys)
	if err != nil {
		return nil, fmt.Errorf("read existing idempotency keys: %w", err)
	}
	found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Notification, error) { return scan(row) })
	if err != nil {
		return nil, fmt.Errorf("read existing idempotency keys: %w", err)
	}
	byKey := make(map[string]domain.Notification, len(found))
	for _, n := range found {
		byKey[n.IdempotencyKey] = n
	}
	return byKey, nil
}

// GetBatch returns a batch with a count of its notifications per status.
func (r *Notifications) GetBatch(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error) {
	s := domain.BatchSummary{StatusCounts: map[domain.Status]int{}}
	err := r.db.QueryRow(ctx, `SELECT id, total, accepted, rejected, created_at FROM batches WHERE id = $1`, id).
		Scan(&s.ID, &s.Total, &s.Accepted, &s.Rejected, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BatchSummary{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.BatchSummary{}, fmt.Errorf("get batch: %w", err)
	}

	rows, err := r.db.Query(ctx, `SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return domain.BatchSummary{}, fmt.Errorf("count batch statuses: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			status string
			count  int
		)
		if err := rows.Scan(&status, &count); err != nil {
			return domain.BatchSummary{}, err
		}
		s.StatusCounts[domain.Status(status)] = count
	}
	return s, rows.Err()
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
	if f.BatchID != nil {
		add("batch_id = $%d", *f.BatchID)
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
		&n.Recipient, &channel, &n.Content, &priority, &n.TemplateID,
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
