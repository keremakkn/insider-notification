package realtime

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func change(id uuid.UUID, batch *uuid.UUID, s domain.Status) ([]byte, domain.StatusChange) {
	c := domain.StatusChange{ID: id, BatchID: batch, Status: s}
	b, _ := json.Marshal(c)
	return b, c
}

func TestDeliversByNotificationAndByBatch(t *testing.T) {
	h := NewHub()
	n1, n2, batch := uuid.New(), uuid.New(), uuid.New()
	byID := h.Subscribe([]uuid.UUID{n1}, nil)
	byBatch := h.Subscribe(nil, []uuid.UUID{batch})

	h.Deliver(change(n1, &batch, domain.StatusSent))      // both care
	h.Deliver(change(n2, &batch, domain.StatusSent))      // only the batch follower
	h.Deliver(change(uuid.New(), nil, domain.StatusSent)) // nobody

	if len(byID.C) != 1 || len(byBatch.C) != 2 {
		t.Fatalf("by id got %d, by batch got %d; want 1 and 2", len(byID.C), len(byBatch.C))
	}
}

func TestSubscribingToBothGetsOneCopy(t *testing.T) {
	h := NewHub()
	n, batch := uuid.New(), uuid.New()
	s := h.Subscribe([]uuid.UUID{n}, []uuid.UUID{batch})
	h.Deliver(change(n, &batch, domain.StatusSent))
	if len(s.C) != 1 {
		t.Fatalf("a change that matches twice must arrive once, got %d", len(s.C))
	}
}

func TestSlowSubscriberIsDroppedWithoutBlocking(t *testing.T) {
	h := NewHub()
	n := uuid.New()
	slow := h.Subscribe([]uuid.UUID{n}, nil)
	fast := h.Subscribe([]uuid.UUID{n}, nil)

	for range BufferSize + 1 {
		h.Deliver(change(n, nil, domain.StatusProcessing))
		<-fast.C // the fast one keeps up
	}
	select {
	case <-slow.Dropped:
	default:
		t.Fatal("a subscriber that never reads should be dropped once its buffer is full")
	}
	select {
	case <-fast.Dropped:
		t.Fatal("the fast subscriber must not suffer")
	default:
	}
}

func TestCloseUnsubscribes(t *testing.T) {
	h := NewHub()
	s := h.Subscribe([]uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
	s.Close()
	s.Close() // safe twice
	if h.Count() != 0 {
		t.Fatalf("want no subscriptions, got %d", h.Count())
	}
}
