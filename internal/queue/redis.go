// Package queue holds notification IDs in Redis lists, one per channel and
// priority. It only moves IDs; what a notification means is not its concern.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Connect opens a Redis client and checks that the server answers.
//
// poolSize must cover every worker that may block in BLPOP at once: each
// blocked call holds a connection for up to a second. go-redis defaults to
// 10 per CPU, so on a 2-CPU machine 20 connections were all taken by idle
// workers of empty channels while the busy channel waited for one, and
// throughput fell to a few messages per second. 0 keeps the default, which
// is fine for processes that do not block (the api).
func Connect(ctx context.Context, addr string, poolSize int) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: poolSize})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return rdb, nil
}

// Key is the Redis list for one channel and priority, for example "queue:sms:high".
func Key(c domain.Channel, p domain.Priority) string {
	return "queue:" + string(c) + ":" + p.String()
}

type Queue struct {
	rdb          *redis.Client
	weights      [3]int
	blockTimeout time.Duration

	mu    sync.Mutex
	sched map[domain.Channel]*scheduler // one per channel, shared by that channel's workers
}

// New returns a queue whose Pop serves priorities in the ratio of weights
// (high, normal, low) and blocks up to blockTimeout when every queue is empty.
// go-redis sends BLPOP timeouts in whole seconds, so anything under 1s waits 1s.
func New(rdb *redis.Client, weights [3]int, blockTimeout time.Duration) *Queue {
	return &Queue{rdb: rdb, weights: weights, blockTimeout: blockTimeout, sched: map[domain.Channel]*scheduler{}}
}

// Push appends IDs to the tail of one queue.
func (q *Queue) Push(ctx context.Context, c domain.Channel, p domain.Priority, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	vals := make([]any, len(ids))
	for i, id := range ids {
		vals[i] = id.String()
	}
	if err := q.rdb.RPush(ctx, Key(c, p), vals...).Err(); err != nil {
		return fmt.Errorf("push to %s: %w", Key(c, p), err)
	}
	return nil
}

// Len is the number of IDs waiting in one queue.
func (q *Queue) Len(ctx context.Context, c domain.Channel, p domain.Priority) (int, error) {
	n, err := q.rdb.LLen(ctx, Key(c, p)).Result()
	if err != nil {
		return 0, fmt.Errorf("length of %s: %w", Key(c, p), err)
	}
	return int(n), nil
}

// popFirst pops from the first non-empty list in KEYS order and returns
// {position, value}. Running it as one script makes the fall-through atomic
// and costs a single round trip instead of up to three.
var popFirst = redis.NewScript(`
for i, key in ipairs(KEYS) do
	local v = redis.call('LPOP', key)
	if v then return {i, v} end
end
return false
`)

// Pop takes the next ID for a channel. The weighted scheduler picks a
// priority; if that queue is empty, the turn falls through to the others in
// priority order, so no turn is wasted. If all three are empty it blocks up
// to blockTimeout and returns ok = false when nothing arrived.
func (q *Queue) Pop(ctx context.Context, c domain.Channel) (id uuid.UUID, p domain.Priority, ok bool, err error) {
	order := fallThroughOrder(q.scheduler(c).next())
	keys := make([]string, len(order))
	for i, pr := range order {
		keys[i] = Key(c, pr)
	}

	res, err := popFirst.Run(ctx, q.rdb, keys).Slice()
	switch {
	case err == nil:
		pos := res[0].(int64) - 1 // Lua lists start at 1
		return parse(res[1], order[pos])
	case !errors.Is(err, redis.Nil):
		return uuid.Nil, 0, false, fmt.Errorf("pop %s: %w", c, err)
	}

	// Everything was empty: wait for the first push instead of spinning.
	// BLPOP checks keys in order, so a waiting high item still wins.
	byPriority := []string{Key(c, domain.PriorityHigh), Key(c, domain.PriorityNormal), Key(c, domain.PriorityLow)}
	kv, err := q.rdb.BLPop(ctx, q.blockTimeout, byPriority...).Result()
	if errors.Is(err, redis.Nil) {
		return uuid.Nil, 0, false, nil
	}
	if err != nil {
		return uuid.Nil, 0, false, fmt.Errorf("blocking pop %s: %w", c, err)
	}
	for _, pr := range domain.Priorities {
		if kv[0] == Key(c, pr) {
			return parse(kv[1], pr)
		}
	}
	return uuid.Nil, 0, false, fmt.Errorf("blocking pop returned unknown key %q", kv[0])
}

func parse(v any, p domain.Priority) (uuid.UUID, domain.Priority, bool, error) {
	s, _ := v.(string)
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, 0, false, fmt.Errorf("queue held a malformed id %q: %w", s, err)
	}
	return id, p, true, nil
}

// fallThroughOrder puts the chosen priority first and the rest after it in
// priority order: an empty high turn goes to normal, then low.
func fallThroughOrder(chosen domain.Priority) []domain.Priority {
	out := []domain.Priority{chosen}
	for _, p := range domain.Priorities {
		if p != chosen {
			out = append(out, p)
		}
	}
	return out
}

func (q *Queue) scheduler(c domain.Channel) *scheduler {
	q.mu.Lock()
	defer q.mu.Unlock()
	s, ok := q.sched[c]
	if !ok {
		s = newScheduler(q.weights)
		q.sched[c] = s
	}
	return s
}
