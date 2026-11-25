// Package service holds the business rules that sit between HTTP and SQL.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Repository is what the service needs from storage. It is declared here,
// where it is used, so tests can pass an in-memory fake.
type Repository interface {
	Create(ctx context.Context, n domain.Notification) (domain.Notification, bool, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error)
}

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

type Notifications struct {
	repo        Repository
	maxAttempts int
	now         func() time.Time
}

func NewNotifications(repo Repository, maxAttempts int) *Notifications {
	return &Notifications{repo: repo, maxAttempts: maxAttempts, now: time.Now}
}

// Create validates and stores a notification. created is false when the
// idempotency key was seen before with the same request; the original is
// returned. The same key with a different request is ErrIdempotencyMismatch.
func (s *Notifications) Create(ctx context.Context, req domain.CreateRequest, correlationID string) (n domain.Notification, created bool, err error) {
	n, err = domain.NewNotification(req, s.maxAttempts, s.now())
	if err != nil {
		return domain.Notification{}, false, err
	}
	n.CorrelationID = correlationID

	stored, created, err := s.repo.Create(ctx, n)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if !created && stored.RequestHash != n.RequestHash {
		return domain.Notification{}, false, domain.ErrIdempotencyMismatch
	}
	return stored, created, nil
}

func (s *Notifications) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.repo.Get(ctx, id)
}

// Cancel cancels a pending notification. On ErrNotCancellable the current
// notification is returned too, so the caller can show its status.
func (s *Notifications) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.repo.Cancel(ctx, id)
}

func (s *Notifications) List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error) {
	var v domain.ValidationError
	switch {
	case f.Limit == 0:
		f.Limit = DefaultPageSize
	case f.Limit < 0 || f.Limit > MaxPageSize:
		v.Add("limit", fmt.Sprintf("must be between 1 and %d", MaxPageSize))
	}
	if f.From != nil && f.To != nil && !f.From.Before(*f.To) {
		v.Add("from", "must be before to")
	}
	if v.HasErrors() {
		return nil, nil, &v
	}
	return s.repo.List(ctx, f)
}
