package ratelimit

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/keremakkn/insider-notification/internal/testutil"
)

var rdb *redis.Client

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	c, stop, err := testutil.StartRedis(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	rdb = c
	code := m.Run()
	stop()
	os.Exit(code)
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func setup(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

// busiest is the largest number of times inside any one-second window.
func busiest(times []time.Time) int {
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	worst := 0
	for i := range times {
		j := i
		for j < len(times) && times[j].Sub(times[i]) < time.Second {
			j++
		}
		worst = max(worst, j-i)
	}
	return worst
}

// hammer runs callers goroutines on each limiter, all calling Wait as fast
// as they can for d, and returns when each Wait returned.
func hammer(t *testing.T, limiters []*Redis, callers int, d time.Duration) []time.Time {
	t.Helper()
	var (
		mu    sync.Mutex
		times []time.Time
		wg    sync.WaitGroup
	)
	deadline := time.Now().Add(d)
	for _, l := range limiters {
		for range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					if err := l.Wait(context.Background()); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					times = append(times, time.Now())
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	return times
}

// Two worker processes, each with its own limiter on the same key, share
// one limit instead of getting one each. This is what the in-process
// limiter could not do.
func TestLimitIsSharedAcrossProcesses(t *testing.T) {
	setup(t)
	const limit = 50
	a := NewRedis(rdb, "ratelimit:sms", limit, discard)
	b := NewRedis(rdb, "ratelimit:sms", limit, discard)

	times := hammer(t, []*Redis{a, b}, 10, 2500*time.Millisecond)
	worst := busiest(times)
	t.Logf("%d calls in 2.5 s from two processes; busiest one-second window: %d", len(times), worst)

	// At most one call per interval: limit+1 per window, plus timer
	// wake-ups that run late on a loaded machine (as in the worker test).
	const schedulerAllowance = 2
	if worst > limit+1+schedulerAllowance {
		t.Fatalf("rate limit exceeded: %d calls in one second", worst)
	}
	// And it is a limit, not a stall: the processes together still get
	// close to the full rate.
	if worst < limit*8/10 {
		t.Fatalf("only %d calls in the busiest second; the limiter is too strict", worst)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	setup(t)
	const limit = 20
	sms := NewRedis(rdb, "ratelimit:sms", limit, discard)
	email := NewRedis(rdb, "ratelimit:email", limit, discard)

	times := hammer(t, []*Redis{sms, email}, 5, 1500*time.Millisecond)
	// Each channel gets its own limit, so together they do about twice one.
	if worst := busiest(times); worst < 2*limit*8/10 {
		t.Fatalf("busiest second had %d calls; channels seem to share a limit", worst)
	}
}

// An idle channel leaves no key behind.
func TestKeyExpiresWhenIdle(t *testing.T) {
	setup(t)
	l := NewRedis(rdb, "ratelimit:push", 100, discard)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ttl, err := rdb.PTTL(context.Background(), "ratelimit:push").Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 2*time.Second {
		t.Fatalf("key TTL is %s, want a short positive one", ttl)
	}
}

func TestWaitStopsWithTheContext(t *testing.T) {
	setup(t)
	l := NewRedis(rdb, "ratelimit:sms", 1, discard)
	if err := l.Wait(context.Background()); err != nil { // takes the only slot of this second
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

// Without Redis, delivery goes on under the in-process limit instead of
// stopping. Runs without Docker: nothing listens on port 1.
func TestFallsBackWhenRedisIsDown(t *testing.T) {
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	defer down.Close()
	const limit = 100
	l := NewRedis(down, "ratelimit:sms", limit, discard)

	begin := time.Now()
	for range 21 {
		if err := l.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 21 calls at 100/s with burst 1 need at least 200 ms.
	if elapsed := time.Since(begin); elapsed < 190*time.Millisecond {
		t.Fatalf("21 calls took %s; the fallback is not limiting", elapsed)
	}
	if !l.degraded.Load() {
		t.Fatal("limiter does not report that it is degraded")
	}
}
