package testutil

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// MemTemplates is an in-memory template store.
type MemTemplates struct {
	mu   sync.Mutex
	byID map[uuid.UUID]domain.Template
}

func NewMemTemplates() *MemTemplates { return &MemTemplates{byID: map[uuid.UUID]domain.Template{}} }

func (m *MemTemplates) Create(_ context.Context, t domain.Template) (domain.Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.byID {
		if existing.Name == t.Name {
			return domain.Template{}, domain.ErrTemplateNameTaken
		}
	}
	m.byID[t.ID] = t
	return t, nil
}

func (m *MemTemplates) Get(_ context.Context, id uuid.UUID) (domain.Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byID[id]
	if !ok {
		return domain.Template{}, domain.ErrTemplateNotFound
	}
	return t, nil
}

func (m *MemTemplates) List(_ context.Context) ([]domain.Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Template, 0, len(m.byID))
	for _, t := range m.byID {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *MemTemplates) UpdateBody(_ context.Context, id uuid.UUID, body string) (domain.Template, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byID[id]
	if !ok {
		return domain.Template{}, domain.ErrTemplateNotFound
	}
	t.Body = body
	m.byID[id] = t
	return t, nil
}
