package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func TestClaimCountsTheAttemptAndSetsALease(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))

	claimed, ok, err := repo.Claim(ctx, n.ID, 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if claimed.Status != domain.StatusProcessing || claimed.Attempts != 1 || claimed.LeaseExpiresAt == nil ||
		time.Until(*claimed.LeaseExpiresAt) < 25*time.Second {
		t.Fatalf("unexpected claim: %+v", claimed)
	}
	if _, ok, _ := repo.Claim(ctx, n.ID, 30*time.Second); ok {
		t.Fatal("a second worker must not claim it")
	}
}

// A stale copy of the ID in Redis must not bypass a retry's backoff.
func TestClaimRespectsNextAttemptAt(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))
	if _, err := pool.Exec(ctx, `UPDATE notifications SET next_attempt_at = now() + interval '1 minute' WHERE id = $1`, n.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := repo.Claim(ctx, n.ID, 30*time.Second); ok {
		t.Fatal("a notification that is not due must not be claimed")
	}
}

func TestFinishWritesOutcomeAndAttempt(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))
	c, _, _ := repo.Claim(ctx, n.ID, 30*time.Second)

	err := repo.Finish(ctx, n.ID, c.Attempts, domain.Outcome{
		Status: domain.StatusSent, ProviderMessageID: "m-1", HTTPStatus: 202, Duration: 120 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := repo.Get(ctx, n.ID)
	if got.Status != domain.StatusSent || got.SentAt == nil || got.ProviderMessageID != "m-1" || got.LeaseExpiresAt != nil {
		t.Fatalf("unexpected row: %+v", got)
	}
	var status, ms int
	_ = pool.QueryRow(ctx, `SELECT http_status, duration_ms FROM delivery_attempts WHERE notification_id = $1 AND attempt_no = 1`, n.ID).Scan(&status, &ms)
	if status != 202 || ms != 120 {
		t.Fatalf("attempt row: status=%d ms=%d", status, ms)
	}
}

// Worker A's lease expired while the provider was slow; worker B claimed
// the notification. A's late result must not overwrite B's work.
func TestFinishWithAStaleClaimIsFenced(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))

	a, _, _ := repo.Claim(ctx, n.ID, 30*time.Second)
	if _, err := pool.Exec(ctx, `UPDATE notifications SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, n.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.RecoverExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
	b, ok, _ := repo.Claim(ctx, n.ID, 30*time.Second)
	if !ok || b.Attempts != 2 {
		t.Fatalf("worker B should claim attempt 2, got ok=%v attempts=%d", ok, b.Attempts)
	}

	err := repo.Finish(ctx, n.ID, a.Attempts, domain.Outcome{Status: domain.StatusFailed, Error: "late answer"})
	if !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("worker A: want ErrLeaseLost, got %v", err)
	}
	got, _ := repo.Get(ctx, n.ID)
	if got.Status != domain.StatusProcessing || got.Attempts != 2 {
		t.Fatalf("B's claim must be untouched, got %s attempts=%d", got.Status, got.Attempts)
	}
	if err := repo.Finish(ctx, n.ID, b.Attempts, domain.Outcome{Status: domain.StatusSent}); err != nil {
		t.Fatalf("worker B: %v", err)
	}
	var attempts int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE notification_id = $1`, n.ID).Scan(&attempts)
	if attempts != 2 {
		t.Fatalf("both provider calls happened and both are recorded: got %d", attempts)
	}
}

func TestFinishRetrySchedulesTheNextAttempt(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))
	c, _, _ := repo.Claim(ctx, n.ID, 30*time.Second)
	next := time.Now().Add(time.Minute).Truncate(time.Microsecond)

	_ = repo.Finish(ctx, n.ID, c.Attempts, domain.Outcome{Status: domain.StatusPending, NextAttemptAt: next, Error: "503", HTTPStatus: 503})
	got, _ := repo.Get(ctx, n.ID)
	if got.Status != domain.StatusPending || !got.NextAttemptAt.Equal(next) || got.EnqueuedAt != nil || got.LastError != "503" {
		t.Fatalf("unexpected row: %+v", got)
	}
}
