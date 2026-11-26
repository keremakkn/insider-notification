package service

import (
	"context"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type StatsStore interface {
	Stats(ctx context.Context, window time.Duration) (domain.Stats, error)
}

type QueueLener interface {
	Len(ctx context.Context, c domain.Channel, p domain.Priority) (int, error)
}

// Stats combines what PostgreSQL knows (history, backlog) with what Redis
// knows (queue depths) into one snapshot.
type Stats struct {
	store StatsStore
	queue QueueLener
}

func NewStats(store StatsStore, queue QueueLener) *Stats { return &Stats{store: store, queue: queue} }

func (s *Stats) Stats(ctx context.Context, window time.Duration) (domain.Stats, error) {
	st, err := s.store.Stats(ctx, window)
	if err != nil {
		return st, err
	}
	for _, c := range domain.Channels {
		for _, p := range domain.Priorities {
			n, err := s.queue.Len(ctx, c, p)
			if err != nil {
				return st, err
			}
			st.Channels[c].QueueDepth[p] = n
		}
	}
	return st, nil
}
