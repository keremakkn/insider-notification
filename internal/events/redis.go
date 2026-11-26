// Package events carries status changes from the processes that make them
// (worker, dispatcher, api) to the api processes that hold WebSocket
// clients, over Redis Pub/Sub.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Topic is the Redis Pub/Sub channel. Every api process subscribes to it,
// so a client sees changes no matter which process it is connected to.
const Topic = "notification-events"

type Publisher interface {
	Publish(ctx context.Context, c domain.StatusChange)
}

// Redis publishes and subscribes. Publishing is best effort: Pub/Sub has no
// storage, and a failed publish must never fail a delivery, so errors are
// logged and dropped.
type Redis struct {
	rdb *redis.Client
	log *slog.Logger
}

func NewRedis(rdb *redis.Client, log *slog.Logger) *Redis {
	return &Redis{rdb: rdb, log: log.With("component", "events")}
}

func (r *Redis) Publish(ctx context.Context, c domain.StatusChange) {
	b, _ := json.Marshal(c)
	if err := r.rdb.Publish(ctx, Topic, b).Err(); err != nil {
		r.log.Warn("publish status change", "notification_id", c.ID, "error", err)
	}
}

// Subscribe calls fn with the raw JSON of every status change until ctx is
// cancelled. go-redis reconnects a dropped subscription by itself.
func (r *Redis) Subscribe(ctx context.Context, fn func(raw []byte, c domain.StatusChange)) error {
	sub := r.rdb.Subscribe(ctx, Topic)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil { // wait for the subscription to be confirmed
		return fmt.Errorf("subscribe %s: %w", Topic, err)
	}
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var c domain.StatusChange
			if err := json.Unmarshal([]byte(msg.Payload), &c); err != nil {
				r.log.Warn("malformed status change", "error", err)
				continue
			}
			fn([]byte(msg.Payload), c)
		}
	}
}

// Nop discards changes; for tests and processes without subscribers.
type Nop struct{}

func (Nop) Publish(context.Context, domain.StatusChange) {}
