// Package realtime fans status changes out to WebSocket subscribers.
package realtime

import (
	"sync"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// BufferSize is how many messages may wait for a slow client before it is
// dropped. One slow connection must not hold up the others or grow memory
// without bound.
const BufferSize = 64

// Hub keeps the subscriptions of this api process in memory.
type Hub struct {
	mu             sync.Mutex
	byNotification map[uuid.UUID]map[*Subscription]struct{}
	byBatch        map[uuid.UUID]map[*Subscription]struct{}
}

func NewHub() *Hub {
	return &Hub{byNotification: map[uuid.UUID]map[*Subscription]struct{}{}, byBatch: map[uuid.UUID]map[*Subscription]struct{}{}}
}

// Subscription receives the JSON of matching status changes on C. Dropped
// is closed when the hub gave up on a client that read too slowly.
type Subscription struct {
	C       chan []byte
	Dropped chan struct{}

	hub           *Hub
	notifications []uuid.UUID
	batches       []uuid.UUID
	once          sync.Once
}

// Subscribe registers interest in notifications and batches.
func (h *Hub) Subscribe(notifications, batches []uuid.UUID) *Subscription {
	s := &Subscription{C: make(chan []byte, BufferSize), Dropped: make(chan struct{}), hub: h,
		notifications: notifications, batches: batches}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range notifications {
		add(h.byNotification, id, s)
	}
	for _, id := range batches {
		add(h.byBatch, id, s)
	}
	return s
}

// Close unregisters the subscription. Safe to call more than once.
func (s *Subscription) Close() {
	s.once.Do(func() {
		h := s.hub
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, id := range s.notifications {
			remove(h.byNotification, id, s)
		}
		for _, id := range s.batches {
			remove(h.byBatch, id, s)
		}
	})
}

// Deliver sends one change to every subscription that follows its
// notification or its batch. It never blocks: a full buffer drops that
// subscriber.
func (h *Hub) Deliver(raw []byte, c domain.StatusChange) {
	h.mu.Lock()
	targets := map[*Subscription]struct{}{}
	for s := range h.byNotification[c.ID] {
		targets[s] = struct{}{}
	}
	if c.BatchID != nil {
		for s := range h.byBatch[*c.BatchID] {
			targets[s] = struct{}{}
		}
	}
	h.mu.Unlock()

	for s := range targets {
		select {
		case s.C <- raw:
		default:
			s.drop()
		}
	}
}

func (s *Subscription) drop() {
	select {
	case <-s.Dropped:
	default:
		close(s.Dropped)
	}
	go s.Close()
}

// Count is the number of live subscriptions, for tests and metrics.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := map[*Subscription]struct{}{}
	for _, m := range []map[uuid.UUID]map[*Subscription]struct{}{h.byNotification, h.byBatch} {
		for _, subs := range m {
			for s := range subs {
				seen[s] = struct{}{}
			}
		}
	}
	return len(seen)
}

func add(m map[uuid.UUID]map[*Subscription]struct{}, id uuid.UUID, s *Subscription) {
	if m[id] == nil {
		m[id] = map[*Subscription]struct{}{}
	}
	m[id][s] = struct{}{}
}

func remove(m map[uuid.UUID]map[*Subscription]struct{}, id uuid.UUID, s *Subscription) {
	delete(m[id], s)
	if len(m[id]) == 0 {
		delete(m, id)
	}
}
