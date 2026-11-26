package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type Templates struct {
	db *pgxpool.Pool
}

func NewTemplates(db *pgxpool.Pool) *Templates { return &Templates{db: db} }

const templateColumns = `id, name, channel, body, created_at, updated_at`

func (r *Templates) Create(ctx context.Context, t domain.Template) (domain.Template, error) {
	stored, err := scanTemplate(r.db.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, body, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5) RETURNING `+templateColumns,
		t.ID, t.Name, string(t.Channel), t.Body, t.CreatedAt))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation on name
		return domain.Template{}, domain.ErrTemplateNameTaken
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("create template: %w", err)
	}
	return stored, nil
}

func (r *Templates) Get(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	t, err := scanTemplate(r.db.QueryRow(ctx, `SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Template{}, domain.ErrTemplateNotFound
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("get template: %w", err)
	}
	return t, nil
}

// List returns every template by name. Templates are few (written by people,
// not generated), so there is no pagination.
func (r *Templates) List(ctx context.Context) ([]domain.Template, error) {
	rows, err := r.db.Query(ctx, `SELECT `+templateColumns+` FROM templates ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Template, error) { return scanTemplate(row) })
}

func (r *Templates) UpdateBody(ctx context.Context, id uuid.UUID, body string) (domain.Template, error) {
	t, err := scanTemplate(r.db.QueryRow(ctx,
		`UPDATE templates SET body = $2 WHERE id = $1 RETURNING `+templateColumns, id, body))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Template{}, domain.ErrTemplateNotFound
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("update template: %w", err)
	}
	return t, nil
}

func scanTemplate(row pgx.Row) (domain.Template, error) {
	var (
		t       domain.Template
		channel string
	)
	if err := row.Scan(&t.ID, &t.Name, &channel, &t.Body, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Template{}, err
	}
	t.Channel = domain.Channel(channel)
	return t, nil
}
