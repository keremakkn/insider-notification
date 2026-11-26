package dispatcher

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/repository"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

var (
	pool *pgxpool.Pool
	rdb  *redis.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	p, stopPG, err := testutil.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	r, stopRedis, err := testutil.StartRedis(ctx)
	if err != nil {
		stopPG()
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	pool, rdb = p, r
	code := m.Run()
	stopRedis()
	stopPG()
	os.Exit(code)
}

type recordingEvents struct{ changes []domain.StatusChange }

func (r *recordingEvents) Publish(_ context.Context, c domain.StatusChange) {
	r.changes = append(r.changes, c)
}

type env struct {
	events *recordingEvents
	t      *testing.T
	repo   *repository.Notifications
	queue  *queue.Queue
	disp   *Dispatcher
}

// Small thresholds make backpressure visible with a handful of rows.
var testConfig = Config{Interval: time.Second, Thresholds: [3]int{5, 3, 2}, VisibilityTimeout: 5 * time.Minute}

func setup(t *testing.T) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	testutil.Truncate(t, pool)
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewNotifications(pool)
	q := queue.New(rdb, [3]int{6, 3, 1}, time.Second)
	ev := &recordingEvents{}
	cfg := testConfig
	cfg.Events = ev
	return &env{events: ev, t: t, repo: repo, queue: q, disp: New(repo, q, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))}
}

// create stores n pending notifications and returns their IDs.
func (e *env) create(n int, c domain.Channel, p domain.Priority) []uuid.UUID {
	e.t.Helper()
	recipient := map[domain.Channel]string{domain.ChannelSMS: "+905551234567", domain.ChannelEmail: "a@b.com", domain.ChannelPush: "token"}[c]
	ids := make([]uuid.UUID, n)
	for i := range ids {
		nt, err := domain.NewNotification(domain.CreateRequest{
			Recipient: recipient, Channel: string(c), Content: "hi", Priority: p.String(),
		}, 5, time.Now().Add(-time.Second))
		if err != nil {
			e.t.Fatal(err)
		}
		nt.CorrelationID = "c"
		if _, _, err := e.repo.Create(context.Background(), nt); err != nil {
			e.t.Fatal(err)
		}
		ids[i] = nt.ID
	}
	return ids
}

func (e *env) queued(c domain.Channel, p domain.Priority) []string {
	e.t.Helper()
	ids, err := rdb.LRange(context.Background(), queue.Key(c, p), 0, -1).Result()
	if err != nil {
		e.t.Fatal(err)
	}
	return ids
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatal(err)
	}
}

func TestDispatchesIntoTheRightQueues(t *testing.T) {
	e := setup(t)
	sms := e.create(2, domain.ChannelSMS, domain.PriorityHigh)
	email := e.create(1, domain.ChannelEmail, domain.PriorityLow)

	s := e.disp.Tick(context.Background())
	if s.Dispatched != 3 || s.Errors != 0 {
		t.Fatalf("stats: %+v", s)
	}
	if got := e.queued(domain.ChannelSMS, domain.PriorityHigh); len(got) != 2 || got[0] != sms[0].String() {
		t.Fatalf("sms high queue: %v, want %v oldest first", got, sms)
	}
	if got := e.queued(domain.ChannelEmail, domain.PriorityLow); len(got) != 1 || got[0] != email[0].String() {
		t.Fatalf("email low queue: %v", got)
	}

	// The next tick must not push them again: enqueued_at is set.
	if s := e.disp.Tick(context.Background()); s.Dispatched != 0 {
		t.Fatalf("second tick re-dispatched %d rows", s.Dispatched)
	}
}

func TestSkipsNotificationsThatAreNotDue(t *testing.T) {
	e := setup(t)
	ids := e.create(1, domain.ChannelSMS, domain.PriorityNormal)
	e.exec(`UPDATE notifications SET next_attempt_at = now() + interval '1 hour' WHERE id = $1`, ids[0])

	if s := e.disp.Tick(context.Background()); s.Dispatched != 0 {
		t.Fatal("a scheduled or backing-off notification must wait for next_attempt_at")
	}
	e.exec(`UPDATE notifications SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, ids[0])
	if s := e.disp.Tick(context.Background()); s.Dispatched != 1 {
		t.Fatal("once due it must be dispatched")
	}
}

func TestBackpressureKeepsRedisShort(t *testing.T) {
	e := setup(t)
	e.create(20, domain.ChannelSMS, domain.PriorityHigh) // threshold for high is 5

	e.disp.Tick(context.Background())
	if n := len(e.queued(domain.ChannelSMS, domain.PriorityHigh)); n != 5 {
		t.Fatalf("queue should stop at the threshold: got %d", n)
	}

	// Workers take 2; the next tick refills exactly the 2 free places.
	for range 2 {
		if _, _, _, err := e.queue.Pop(context.Background(), domain.ChannelSMS); err != nil {
			t.Fatal(err)
		}
	}
	if s := e.disp.Tick(context.Background()); s.Dispatched != 2 {
		t.Fatalf("refill: want 2, got %d", s.Dispatched)
	}
	var waiting int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE enqueued_at IS NULL`).Scan(&waiting)
	if waiting != 13 {
		t.Fatalf("the backlog should wait in PostgreSQL: want 13 not yet enqueued, got %d", waiting)
	}
}

// A worker popped the ID and died before claiming it: the ID is gone from
// Redis while the row is still pending with enqueued_at set.
func TestVisibilityTimeoutRecoversAPoppedButUnclaimedMessage(t *testing.T) {
	e := setup(t)
	ids := e.create(1, domain.ChannelSMS, domain.PriorityHigh)
	e.disp.Tick(context.Background())
	_, _, _, _ = e.queue.Pop(context.Background(), domain.ChannelSMS) // the worker takes it... and dies

	if s := e.disp.Tick(context.Background()); s.Dispatched != 0 {
		t.Fatal("within the visibility timeout the dispatcher must assume it is still queued")
	}
	// Six minutes later (timeout is five).
	e.exec(`UPDATE notifications SET enqueued_at = now() - interval '6 minutes' WHERE id = $1`, ids[0])
	if s := e.disp.Tick(context.Background()); s.Dispatched != 1 {
		t.Fatal("after the visibility timeout the message must be queued again")
	}
}

type failingQueue struct{ Queue }

func (failingQueue) Push(context.Context, domain.Channel, domain.Priority, ...uuid.UUID) error {
	return errors.New("redis is down")
}

// The dispatcher committed enqueued_at, then the push to Redis failed.
func TestFailedPushIsNotALoss(t *testing.T) {
	e := setup(t)
	ids := e.create(1, domain.ChannelEmail, domain.PriorityNormal)

	broken := New(e.repo, failingQueue{e.queue}, testConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if s := broken.Tick(context.Background()); s.Errors == 0 {
		t.Fatal("the failed push should be reported")
	}
	if len(e.queued(domain.ChannelEmail, domain.PriorityNormal)) != 0 {
		t.Fatal("nothing reached Redis")
	}

	e.exec(`UPDATE notifications SET enqueued_at = now() - interval '6 minutes' WHERE id = $1`, ids[0])
	if s := e.disp.Tick(context.Background()); s.Dispatched != 1 {
		t.Fatal("the row must come back after the visibility timeout")
	}
}

func TestExpiredLeaseIsRecoveredAndRequeuedInTheSameTick(t *testing.T) {
	e := setup(t)
	ids := e.create(2, domain.ChannelPush, domain.PriorityLow)
	// Two workers claimed one each (attempt 1 and the final attempt 5) and died.
	e.exec(`UPDATE notifications SET status = 'processing', attempts = 1, lease_expires_at = now() - interval '1 second' WHERE id = $1`, ids[0])
	e.exec(`UPDATE notifications SET status = 'processing', attempts = 5, lease_expires_at = now() - interval '1 second' WHERE id = $1`, ids[1])

	s := e.disp.Tick(context.Background())
	if s.Recovered != 1 || s.Failed != 1 || s.Dispatched != 1 {
		t.Fatalf("want 1 recovered and dispatched, 1 failed: %+v", s)
	}
	var announced []domain.Status
	for _, c := range e.events.changes {
		announced = append(announced, c.Status)
	}
	if fmt.Sprint(announced) != "[pending failed]" && fmt.Sprint(announced) != "[failed pending]" {
		t.Fatalf("both recoveries must be announced, the failure above all: %v", announced)
	}
	retry, _ := e.repo.Get(context.Background(), ids[0])
	final, _ := e.repo.Get(context.Background(), ids[1])
	if retry.Status != domain.StatusPending || final.Status != domain.StatusFailed || final.LastError == "" {
		t.Fatalf("got %s and %s (%q)", retry.Status, final.Status, final.LastError)
	}
}

func TestLiveLeaseIsLeftAlone(t *testing.T) {
	e := setup(t)
	ids := e.create(1, domain.ChannelSMS, domain.PriorityHigh)
	e.exec(`UPDATE notifications SET status = 'processing', attempts = 1, lease_expires_at = now() + interval '30 seconds' WHERE id = $1`, ids[0])

	if s := e.disp.Tick(context.Background()); s.Recovered != 0 || s.Dispatched != 0 {
		t.Fatalf("a worker is still delivering it: %+v", s)
	}
}

// Eight dispatchers take rows at the same time, 25 at a time, until none
// are left. SKIP LOCKED must hand every row to exactly one of them.
func TestConcurrentDispatchersNeverTakeTheSameRow(t *testing.T) {
	e := setup(t)
	e.create(300, domain.ChannelSMS, domain.PriorityHigh)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		seen     = map[uuid.UUID]int{}
		perTaker [8]int
	)
	for i := range perTaker {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ids, err := e.repo.TakeDue(context.Background(), domain.ChannelSMS, domain.PriorityHigh, 25, 5*time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(ids) == 0 {
					return
				}
				mu.Lock()
				for _, id := range ids {
					seen[id]++
				}
				perTaker[i] += len(ids)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	t.Logf("rows taken by each of 8 dispatchers: %v", perTaker)
	busy := 0
	for _, n := range perTaker {
		if n > 0 {
			busy++
		}
	}
	if len(seen) != 300 {
		t.Fatalf("want all 300 rows taken, got %d", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("row %s taken %d times", id, n)
		}
	}
	if busy < 2 {
		t.Fatalf("the test did not exercise concurrency: only %d dispatcher got rows", busy)
	}
}
