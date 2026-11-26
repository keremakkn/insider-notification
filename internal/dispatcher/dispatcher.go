// Package dispatcher is the transactional outbox: it moves due notifications
// from PostgreSQL (the source of truth) into the Redis queues, keeps those
// queues short, and recovers notifications whose worker died.
package dispatcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/events"
	"github.com/keremakkn/insider-notification/internal/metrics"
)

type Store interface {
	TakeDue(ctx context.Context, c domain.Channel, p domain.Priority, limit int, visibilityTimeout time.Duration) ([]uuid.UUID, error)
	RecoverExpiredLeases(ctx context.Context) ([]domain.StatusChange, error)
}

type Queue interface {
	Push(ctx context.Context, c domain.Channel, p domain.Priority, ids ...uuid.UUID) error
	Len(ctx context.Context, c domain.Channel, p domain.Priority) (int, error)
}

type Config struct {
	Interval          time.Duration
	Thresholds        [3]int // per priority: high, normal, low
	VisibilityTimeout time.Duration
	Metrics           *metrics.Delivery
	Events            events.Publisher // announces recovered and failed leases
}

type Dispatcher struct {
	store Store
	queue Queue
	cfg   Config
	log   *slog.Logger
}

func New(store Store, queue Queue, cfg Config, log *slog.Logger) *Dispatcher {
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewDelivery(prometheus.NewRegistry())
	}
	if cfg.Events == nil {
		cfg.Events = events.Nop{}
	}
	return &Dispatcher{store: store, queue: queue, cfg: cfg, log: log.With("component", "dispatcher")}
}

// Stats describe one tick, for logs and metrics.
type Stats struct {
	Recovered  int // processing rows whose lease expired, back to pending
	Failed     int // expired leases with no attempts left
	Dispatched int // IDs pushed to Redis
	Errors     int
}

// Run ticks until ctx is cancelled. The first tick runs immediately.
func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		s := d.Tick(ctx)
		d.cfg.Metrics.Dispatched.Add(float64(s.Dispatched))
		d.cfg.Metrics.LeasesRecovered.Add(float64(s.Recovered))
		d.cfg.Metrics.LeasesExhausted.Add(float64(s.Failed))
		if s.Recovered+s.Failed+s.Dispatched+s.Errors > 0 {
			d.log.Info("dispatch tick", "dispatched", s.Dispatched, "recovered", s.Recovered, "failed", s.Failed, "errors", s.Errors)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick does one pass. Lease recovery runs first so recovered notifications
// can be dispatched in the same tick. Each queue is handled independently:
// a failure on one is logged and does not stop the others.
func (d *Dispatcher) Tick(ctx context.Context) Stats {
	var s Stats
	changes, err := d.store.RecoverExpiredLeases(ctx)
	if err != nil {
		d.log.Error("recover leases", "error", err)
		s.Errors++
	}
	for _, c := range changes {
		if c.Status == domain.StatusFailed {
			s.Failed++
		} else {
			s.Recovered++
		}
		// A client waiting for this notification must hear that it failed,
		// or it would wait forever.
		d.cfg.Events.Publish(ctx, c)
	}

	for _, c := range domain.Channels {
		// High first: when room is scarce, it goes to the most urgent work.
		for _, p := range domain.Priorities {
			n, err := d.fill(ctx, c, p)
			s.Dispatched += n
			if err != nil {
				d.log.Error("dispatch", "channel", c, "priority", p, "error", err)
				s.Errors++
			}
		}
	}
	return s
}

// fill tops one queue up to its threshold (backpressure: the backlog waits
// in PostgreSQL, so Redis stays short and the visibility timeout does not
// mistake a long wait for a lost message).
func (d *Dispatcher) fill(ctx context.Context, c domain.Channel, p domain.Priority) (int, error) {
	length, err := d.queue.Len(ctx, c, p)
	if err != nil {
		return 0, err
	}
	room := d.cfg.Thresholds[p] - length
	if room <= 0 {
		return 0, nil
	}
	ids, err := d.store.TakeDue(ctx, c, p, room, d.cfg.VisibilityTimeout)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	// The rows are already marked and committed. If this push fails, nothing
	// is lost: they become due again after the visibility timeout.
	if err := d.queue.Push(ctx, c, p, ids...); err != nil {
		return 0, err
	}
	return len(ids), nil
}
