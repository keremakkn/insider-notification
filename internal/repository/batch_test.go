package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func sameHash(item, stored domain.Notification) bool { return item.RequestHash == stored.RequestHash }

func batchOf(t *testing.T, batchID uuid.UUID, keys ...string) []domain.Notification {
	t.Helper()
	items := make([]domain.Notification, len(keys))
	for i, k := range keys {
		items[i] = newNotification(t, k, time.Now())
		items[i].BatchID = &batchID
	}
	return items
}

func TestCreateBatchStoresEverythingInOneTransaction(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	id := uuid.New()
	keys := make([]string, domain.MaxBatchSize)
	for i := range keys {
		keys[i] = fmt.Sprintf("item-%d", i)
	}

	start := time.Now()
	outcomes, b, err := repo.CreateBatch(ctx, domain.Batch{ID: id, Total: 1000, Accepted: 1000, CreatedAt: time.Now()},
		batchOf(t, id, keys...), sameHash)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("1000 notifications stored in %s", elapsed)

	if b.Accepted != 1000 || len(outcomes) != 1000 || !outcomes[999].Created {
		t.Fatalf("unexpected result: accepted=%d outcomes=%d", b.Accepted, len(outcomes))
	}
	s, err := repo.GetBatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if s.Total != 1000 || s.StatusCounts[domain.StatusPending] != 1000 {
		t.Fatalf("summary: %+v", s)
	}
}

func TestCreateBatchReplaysAndConflicts(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()

	// An earlier request stored "a".
	earlier := newNotification(t, "a", time.Now())
	if _, _, err := repo.Create(ctx, earlier); err != nil {
		t.Fatal(err)
	}

	// The batch resends "a" unchanged (replay), reuses "b" twice with
	// different content (conflict on the second), and adds "c".
	id := uuid.New()
	items := batchOf(t, id, "a", "b", "b", "c")
	items[2].Content = "different"
	items[2].RequestHash = domain.RequestHash(items[2])

	// One more item failed validation before reaching the repository.
	outcomes, b, err := repo.CreateBatch(ctx,
		domain.Batch{ID: id, Total: 5, Accepted: 4, Rejected: 1, CreatedAt: time.Now()}, items, sameHash)
	if err != nil {
		t.Fatal(err)
	}

	if outcomes[0].Created || outcomes[0].Stored.ID != earlier.ID {
		t.Error("a: want the earlier notification as a replay")
	}
	if !outcomes[1].Created || outcomes[2].Created || outcomes[2].Stored.ID != outcomes[1].Stored.ID {
		t.Error("b: want the first created and the second pointing to it")
	}
	if !outcomes[3].Created {
		t.Error("c: want created")
	}
	if b.Accepted != 3 || b.Rejected != 2 {
		t.Fatalf("want 3 accepted (a replay, b, c) and 2 rejected (invalid, b conflict), got %d/%d", b.Accepted, b.Rejected)
	}

	s, _ := repo.GetBatch(ctx, id)
	if s.Accepted != 3 || s.Rejected != 2 || s.StatusCounts[domain.StatusPending] != 2 {
		t.Fatalf("stored summary: %+v (the replayed item belongs to its original request, not this batch)", s)
	}
}

func TestCreateBatchWithNothingAcceptedStoresNothing(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	original := newNotification(t, "x", time.Now())
	_, _, _ = repo.Create(ctx, original)

	id := uuid.New()
	items := batchOf(t, id, "x")
	items[0].Content = "changed"
	items[0].RequestHash = domain.RequestHash(items[0])

	_, b, err := repo.CreateBatch(ctx, domain.Batch{ID: id, Total: 1, Accepted: 1, CreatedAt: time.Now()}, items, sameHash)
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != uuid.Nil {
		t.Fatal("no batch should be returned")
	}
	if _, err := repo.GetBatch(ctx, id); err != domain.ErrNotFound {
		t.Fatalf("batch row should have been rolled back, got %v", err)
	}
}

func TestListByBatch(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	id := uuid.New()
	_, _, _ = repo.CreateBatch(ctx, domain.Batch{ID: id, Total: 2, Accepted: 2, CreatedAt: time.Now()}, batchOf(t, id, "", ""), sameHash)
	_, _, _ = repo.Create(ctx, newNotification(t, "", time.Now()))

	page, _, err := repo.List(ctx, domain.ListFilter{BatchID: &id, Limit: 10})
	if err != nil || len(page) != 2 {
		t.Fatalf("want the 2 batch notifications, got %d (%v)", len(page), err)
	}
}
