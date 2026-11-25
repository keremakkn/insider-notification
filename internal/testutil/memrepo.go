package testutil

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// MemRepo is an in-memory notification repository with the same semantics
// as the PostgreSQL one, for tests that do not need a database.
type MemRepo struct {
	mu    sync.Mutex
	byID  map[uuid.UUID]domain.Notification
	byKey map[string]uuid.UUID
}

func NewMemRepo() *MemRepo {
	return &MemRepo{byID: map[uuid.UUID]domain.Notification{}, byKey: map[string]uuid.UUID{}}
}

func (m *MemRepo) Create(_ context.Context, n domain.Notification) (domain.Notification, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n.IdempotencyKey != "" {
		if id, ok := m.byKey[n.IdempotencyKey]; ok {
			return m.byID[id], false, nil
		}
		m.byKey[n.IdempotencyKey] = n.ID
	}
	m.byID[n.ID] = n
	return n, true, nil
}

func (m *MemRepo) Get(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byID[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return n, nil
}

func (m *MemRepo) Cancel(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.byID[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	if n.Status != domain.StatusPending {
		return n, domain.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	m.byID[id] = n
	return n, nil
}

// SetStatus lets a test move a notification as a worker would.
func (m *MemRepo) SetStatus(id uuid.UUID, s domain.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.byID[id]
	n.Status = s
	m.byID[id] = n
}

func (m *MemRepo) List(_ context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []domain.Notification
	for _, n := range m.byID {
		switch {
		case f.Status != nil && n.Status != *f.Status,
			f.Channel != nil && n.Channel != *f.Channel,
			f.From != nil && n.CreatedAt.Before(*f.From),
			f.To != nil && !n.CreatedAt.Before(*f.To),
			f.After != nil && !before(n, *f.After):
			continue
		}
		all = append(all, n)
	}
	sort.Slice(all, func(i, j int) bool {
		return before(all[j], domain.Cursor{CreatedAt: all[i].CreatedAt, ID: all[i].ID})
	})
	if len(all) <= f.Limit {
		return all, nil, nil
	}
	page := all[:f.Limit]
	last := page[len(page)-1]
	return page, &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// before reports whether n sorts after c in newest-first order: (created_at, id) < c.
func before(n domain.Notification, c domain.Cursor) bool {
	if !n.CreatedAt.Equal(c.CreatedAt) {
		return n.CreatedAt.Before(c.CreatedAt)
	}
	return n.ID.String() < c.ID.String()
}
