package queue

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/keremakkn/insider-notification/internal/domain"
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

func setup(t *testing.T) *Queue {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return New(rdb, [3]int{6, 3, 1}, time.Second)
}

func ids(n int) []uuid.UUID {
	out := make([]uuid.UUID, n)
	for i := range out {
		out[i] = uuid.New()
	}
	return out
}

func TestPushLenAndFIFO(t *testing.T) {
	q := setup(t)
	ctx := context.Background()
	in := ids(3)
	if err := q.Push(ctx, domain.ChannelSMS, domain.PriorityHigh, in...); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.Len(ctx, domain.ChannelSMS, domain.PriorityHigh); n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}
	for i := range in {
		id, p, ok, err := q.Pop(ctx, domain.ChannelSMS)
		if err != nil || !ok || id != in[i] || p != domain.PriorityHigh {
			t.Fatalf("pop %d: got %s %s ok=%v err=%v, want %s first in, first out", i, id, p, ok, err, in[i])
		}
	}
}

// Under load (all three queues full) 10,000 pops must split exactly 6/3/1.
func TestWeightedDistributionUnderLoad(t *testing.T) {
	q := setup(t)
	ctx := context.Background()
	for _, p := range domain.Priorities {
		if err := q.Push(ctx, domain.ChannelSMS, p, ids(10_000)...); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[domain.Priority]int{}
	for range 10_000 {
		_, p, ok, err := q.Pop(ctx, domain.ChannelSMS)
		if err != nil || !ok {
			t.Fatal(err)
		}
		counts[p]++
	}
	t.Logf("10,000 pops: high=%d normal=%d low=%d", counts[domain.PriorityHigh], counts[domain.PriorityNormal], counts[domain.PriorityLow])
	if counts[domain.PriorityHigh] != 6000 || counts[domain.PriorityNormal] != 3000 || counts[domain.PriorityLow] != 1000 {
		t.Fatalf("want exactly 6000/3000/1000, got %v", counts)
	}
}

// An empty high queue must not waste turns: its share goes to the others.
func TestEmptyQueueTurnFallsThrough(t *testing.T) {
	q := setup(t)
	ctx := context.Background()
	_ = q.Push(ctx, domain.ChannelEmail, domain.PriorityLow, ids(5)...)

	for i := range 5 {
		_, p, ok, err := q.Pop(ctx, domain.ChannelEmail)
		if err != nil || !ok || p != domain.PriorityLow {
			t.Fatalf("pop %d: want a low item every time, got %s ok=%v err=%v", i, p, ok, err)
		}
	}
}

func TestChannelsAreIndependent(t *testing.T) {
	q := setup(t)
	ctx := context.Background()
	_ = q.Push(ctx, domain.ChannelSMS, domain.PriorityHigh, ids(1)...)

	if _, _, ok, _ := q.Pop(ctx, domain.ChannelPush); ok {
		t.Fatal("push channel must not see sms items")
	}
}

func TestPopBlocksThenTimesOut(t *testing.T) {
	q := setup(t)
	start := time.Now()
	_, _, ok, err := q.Pop(context.Background(), domain.ChannelSMS)
	if err != nil || ok {
		t.Fatalf("empty queue: want ok=false, got ok=%v err=%v", ok, err)
	}
	if waited := time.Since(start); waited < 900*time.Millisecond {
		t.Fatalf("should block about 1s instead of spinning, returned after %s", waited)
	}
}

func TestPopWakesUpOnPush(t *testing.T) {
	q := New(rdb, [3]int{6, 3, 1}, 5*time.Second)
	if testing.Short() {
		t.Skip("integration test")
	}
	_ = rdb.FlushAll(context.Background()).Err()
	want := uuid.New()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = q.Push(context.Background(), domain.ChannelSMS, domain.PriorityNormal, want)
	}()

	start := time.Now()
	id, p, ok, err := q.Pop(context.Background(), domain.ChannelSMS)
	if err != nil || !ok || id != want || p != domain.PriorityNormal {
		t.Fatalf("got %s %s ok=%v err=%v", id, p, ok, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("a waiting worker should wake up as soon as an item arrives, not at the timeout")
	}
}

func TestMalformedIDIsAnError(t *testing.T) {
	q := setup(t)
	ctx := context.Background()
	rdb.RPush(ctx, Key(domain.ChannelSMS, domain.PriorityHigh), "not-a-uuid")
	if _, _, _, err := q.Pop(ctx, domain.ChannelSMS); err == nil {
		t.Fatal("want an error for a malformed id")
	}
}
