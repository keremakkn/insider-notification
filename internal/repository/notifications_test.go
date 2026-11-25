package repository

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

var pool *pgxpool.Pool

// One PostgreSQL container for the whole package; tests truncate between runs.
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	p, stop, err := testutil.StartPostgres(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	pool = p
	code := m.Run()
	stop()
	os.Exit(code)
}

func setup(t *testing.T) *Notifications {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	testutil.Truncate(t, pool)
	return NewNotifications(pool)
}

func newNotification(t *testing.T, key string, createdAt time.Time) domain.Notification {
	t.Helper()
	n, err := domain.NewNotification(domain.CreateRequest{
		Recipient: "+905551234567", Channel: "sms", Content: "Merhaba", IdempotencyKey: key,
	}, 5, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	n.CorrelationID = "corr-" + n.ID.String()
	return n
}

func TestCreateAndGet(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n := newNotification(t, "", time.Now())

	stored, created, err := repo.Create(ctx, n)
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	got, err := repo.Get(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != n.ID || got.Status != domain.StatusPending || got.Recipient != n.Recipient ||
		got.Channel != domain.ChannelSMS || got.Priority != domain.PriorityNormal || got.IdempotencyKey != "" {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, n)
	}
	if !stored.CreatedAt.Equal(got.CreatedAt) {
		t.Error("Create should return the row as stored")
	}
}

func TestGetUnknown(t *testing.T) {
	repo := setup(t)
	if _, err := repo.Get(context.Background(), uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCreateWithSameKeyReturnsOriginal(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()

	first, created, err := repo.Create(ctx, newNotification(t, "order-42", time.Now()))
	if err != nil || !created {
		t.Fatal(err)
	}
	second, created, err := repo.Create(ctx, newNotification(t, "order-42", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if created || second.ID != first.ID {
		t.Fatalf("same key must return the original: created=%v first=%s second=%s", created, first.ID, second.ID)
	}
}

// Twenty requests with the same key arrive at the same moment, as a client
// retrying aggressively would send them. Exactly one row may be created.
func TestConcurrentCreatesWithSameKey(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()

	const n = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		ids     = map[uuid.UUID]bool{}
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored, ok, err := repo.Create(ctx, newNotification(t, "flash-sale", time.Now()))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[stored.ID] = true
			if ok {
				created++
			}
		}()
	}
	wg.Wait()

	if created != 1 || len(ids) != 1 {
		t.Fatalf("want exactly 1 created and 1 distinct id, got created=%d ids=%d", created, len(ids))
	}
	var rows int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM notifications`).Scan(&rows)
	if rows != 1 {
		t.Fatalf("want 1 row in the table, got %d", rows)
	}
}

func TestCancel(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now()))

	cancelled, err := repo.Cancel(ctx, n.ID)
	if err != nil || cancelled.Status != domain.StatusCancelled {
		t.Fatalf("cancel pending: status=%s err=%v", cancelled.Status, err)
	}

	current, err := repo.Cancel(ctx, n.ID)
	if !errors.Is(err, domain.ErrNotCancellable) || current.Status != domain.StatusCancelled {
		t.Fatalf("second cancel: want ErrNotCancellable with current status, got %s %v", current.Status, err)
	}

	if _, err := repo.Cancel(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown id: want ErrNotFound, got %v", err)
	}
}

func TestCancelLosesToWorkerClaim(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now()))

	// A worker claims the notification first.
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status = 'processing', lease_expires_at = now() + interval '30 seconds' WHERE id = $1`, n.ID); err != nil {
		t.Fatal(err)
	}
	current, err := repo.Cancel(ctx, n.ID)
	if !errors.Is(err, domain.ErrNotCancellable) || current.Status != domain.StatusProcessing {
		t.Fatalf("want ErrNotCancellable while processing, got %s %v", current.Status, err)
	}
}

func TestListPaginatesWithoutGapsOrDuplicates(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()

	// 25 notifications, one second apart, plus two sharing a timestamp to
	// prove the id tie-breaker keeps the order stable.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := range 25 {
		_, _, err := repo.Create(ctx, newNotification(t, "", base.Add(time.Duration(i)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, _, _ = repo.Create(ctx, newNotification(t, "", base.Add(24*time.Second)))

	seen := map[uuid.UUID]bool{}
	var (
		cursor *domain.Cursor
		pages  []int
		prev   *domain.Notification
	)
	for {
		page, next, err := repo.List(ctx, domain.ListFilter{After: cursor, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, len(page))
		for i := range page {
			n := page[i]
			if seen[n.ID] {
				t.Fatalf("duplicate %s", n.ID)
			}
			seen[n.ID] = true
			if prev != nil && (n.CreatedAt.After(prev.CreatedAt) ||
				(n.CreatedAt.Equal(prev.CreatedAt) && n.ID.String() > prev.ID.String())) {
				t.Fatalf("not newest first: %v after %v", n.CreatedAt, prev.CreatedAt)
			}
			prev = &n
		}
		if next == nil {
			break
		}
		// Round-trip through the opaque token, as a client would.
		decoded, err := domain.DecodeCursor(next.Encode())
		if err != nil {
			t.Fatal(err)
		}
		cursor = &decoded
	}
	if len(seen) != 26 || fmt.Sprint(pages) != "[10 10 6]" {
		t.Fatalf("want 26 rows in pages [10 10 6], got %d rows in %v", len(seen), pages)
	}
}

func TestListFilters(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour).Truncate(time.Second)

	sms := newNotification(t, "", base)
	_, _, _ = repo.Create(ctx, sms)

	email, _ := domain.NewNotification(domain.CreateRequest{Recipient: "a@b.com", Channel: "email", Content: "Hi"}, 5, base.Add(10*time.Minute))
	email.CorrelationID = "c"
	_, _, _ = repo.Create(ctx, email)

	cancelled := newNotification(t, "", base.Add(20*time.Minute))
	_, _, _ = repo.Create(ctx, cancelled)
	_, _ = repo.Cancel(ctx, cancelled.ID)

	ch := domain.ChannelEmail
	st := domain.StatusCancelled
	from, to := base.Add(5*time.Minute), base.Add(15*time.Minute)

	tests := []struct {
		name   string
		filter domain.ListFilter
		want   uuid.UUID
	}{
		{"by channel", domain.ListFilter{Channel: &ch, Limit: 10}, email.ID},
		{"by status", domain.ListFilter{Status: &st, Limit: 10}, cancelled.ID},
		{"by date range", domain.ListFilter{From: &from, To: &to, Limit: 10}, email.ID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, _, err := repo.List(ctx, tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) != 1 || page[0].ID != tt.want {
				t.Fatalf("want only %s, got %d rows", tt.want, len(page))
			}
		})
	}
}
