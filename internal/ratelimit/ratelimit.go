// Package ratelimit spaces provider calls so that one channel never exceeds
// its rate, no matter how many worker processes are running.
package ratelimit

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// Local is an in-process limiter: rate per second, burst 1, so tokens do not
// pile up while idle. It is correct for a single worker process only.
func Local(perSecond int) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}

// reserve hands out send slots one interval apart, shared by every process
// that uses the same key (GCRA in its reservation form). The key holds the
// next free slot in microseconds of Redis time. A caller takes that slot,
// moves it one interval on, and gets back how long to wait for its own slot.
// Redis's clock is the only clock involved, so skew between worker machines
// does not matter. The key expires once it is in the past, so an idle
// channel leaves nothing behind.
var reserve = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local slot = tonumber(redis.call('GET', KEYS[1]))
if not slot or slot < now then slot = now end
local nextSlot = slot + interval
redis.call('SET', KEYS[1], string.format('%d', nextSlot), 'PX', math.floor((nextSlot - now) / 1000) + 1000)
return slot - now
`)

// Redis is a limiter shared by every worker process through one Redis key.
// Like Local it allows at most one call per interval, so any one-second
// window holds at most rate+1 calls, now summed over all processes.
type Redis struct {
	rdb      redis.Scripter
	key      string
	interval time.Duration
	fallback *rate.Limiter
	degraded atomic.Bool
	log      *slog.Logger
}

// NewRedis returns a limiter for one key, for example "ratelimit:sms".
func NewRedis(rdb redis.Scripter, key string, perSecond int, log *slog.Logger) *Redis {
	return &Redis{
		rdb:      rdb,
		key:      key,
		interval: time.Second / time.Duration(perSecond),
		fallback: Local(perSecond),
		log:      log.With("component", "ratelimit", "key", key),
	}
}

// Wait blocks until this caller's slot. If Redis cannot be reached it falls
// back to the in-process limiter rather than stopping delivery: a claimed
// notification is already waiting. Each process then keeps its own limit,
// so the total can exceed the rate until Redis is back. In practice the
// workers cannot dequeue from Redis either, so little gets this far.
func (l *Redis) Wait(ctx context.Context) error {
	wait, err := reserve.Run(ctx, l.rdb, []string{l.key}, l.interval.Microseconds()).Int64()
	if err != nil {
		if !l.degraded.Swap(true) {
			l.log.Warn("redis rate limiter unavailable, using the in-process limiter", "error", err)
		}
		return l.fallback.Wait(ctx)
	}
	if l.degraded.Swap(false) {
		l.log.Info("redis rate limiter is back")
	}
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(time.Duration(wait) * time.Microsecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
