package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type TemplateStore interface {
	Create(ctx context.Context, t domain.Template) (domain.Template, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Template, error)
	List(ctx context.Context) ([]domain.Template, error)
	UpdateBody(ctx context.Context, id uuid.UUID, body string) (domain.Template, error)
}

type Templates struct {
	store TemplateStore
	now   func() time.Time
}

func NewTemplates(store TemplateStore) *Templates { return &Templates{store: store, now: time.Now} }

func (s *Templates) Create(ctx context.Context, name, channel, body string) (domain.Template, error) {
	t, err := domain.NewTemplate(name, channel, body, s.now())
	if err != nil {
		return domain.Template{}, err
	}
	return s.store.Create(ctx, t)
}

func (s *Templates) Get(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	return s.store.Get(ctx, id)
}

func (s *Templates) List(ctx context.Context) ([]domain.Template, error) { return s.store.List(ctx) }

// Update replaces a template's body. Notifications already created from it
// keep the text they were rendered with.
func (s *Templates) Update(ctx context.Context, id uuid.UUID, body string) (domain.Template, error) {
	t, err := s.store.Get(ctx, id)
	if err != nil {
		return domain.Template{}, err
	}
	if err := t.Update(body, s.now()); err != nil {
		return domain.Template{}, err
	}
	return s.store.UpdateBody(ctx, id, body)
}
