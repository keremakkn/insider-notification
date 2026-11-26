// Package worker drains the Redis queues: it claims each notification,
// delivers it through the channel's provider within the rate limit, and
// records the outcome.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/provider"
)

type Store interface {
	Claim(ctx context.Context, id uuid.UUID, lease time.Duration) (domain.Notification, bool, error)
	Finish(ctx context.Context, id uuid.UUID, attempt int, o domain.Outcome) error
}

type Queue interface {
	Pop(ctx context.Context, c domain.Channel) (uuid.UUID, domain.Priority, bool, error)
}

type Config struct {
	WorkersPerChannel int
	RatePerSecond     int           // per channel
	Lease             time.Duration // must outlive the provider timeout
	RetryDelay        time.Duration // wait before a retryable failure is tried again
}

type Pool struct {
	store   Store
	queue   Queue
	senders map[domain.Channel]provider.Sender
	cfg     Config
	log     *slog.Logger
}

func NewPool(store Store, queue Queue, senders map[domain.Channel]provider.Sender, cfg Config, log *slog.Logger) *Pool {
	return &Pool{store: store, queue: queue, senders: senders, cfg: cfg, log: log.With("component", "worker")}
}

// Run starts every channel's workers and returns when ctx is cancelled and
// all in-flight deliveries have finished.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range domain.Channels {
		// One bucket per channel shared by its workers. Burst 1: tokens do
		// not pile up while idle, so no second ever exceeds the limit.
		limiter := rate.NewLimiter(rate.Limit(p.cfg.RatePerSecond), 1)
		for range p.cfg.WorkersPerChannel {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.loop(ctx, c, limiter)
			}()
		}
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, c domain.Channel, limiter *rate.Limiter) {
	for ctx.Err() == nil {
		id, _, ok, err := p.queue.Pop(ctx, c)
		if ctx.Err() != nil {
			// Shutting down. A popped but unclaimed ID is not lost: the
			// dispatcher re-queues it after the visibility timeout.
			return
		}
		if err != nil {
			p.log.Error("pop", "channel", c, "error", err)
			sleep(ctx, time.Second) // Redis is struggling; do not spin
			continue
		}
		if !ok {
			continue // nothing arrived within the blocking timeout
		}

		// Token after the pop and right before the claim: a token taken
		// earlier could be spent seconds later (while blocked on an empty
		// queue), letting idle workers burst past the limit. The message is
		// not claimed yet, so no lease runs out while it waits here.
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		// Finish an accepted delivery even during shutdown: abandoning it
		// mid-request would only cause a resend after the lease expires.
		p.process(context.WithoutCancel(ctx), c, id)
	}
}

func (p *Pool) process(ctx context.Context, c domain.Channel, id uuid.UUID) {
	n, ok, err := p.store.Claim(ctx, id, p.cfg.Lease)
	if err != nil {
		p.log.Error("claim", "notification_id", id, "error", err)
		return
	}
	if !ok {
		// Cancelled, taken by another worker, finished, or a stale copy
		// whose next attempt is not due yet.
		p.log.Debug("skipped", "notification_id", id)
		return
	}
	log := p.log.With("notification_id", n.ID, "correlation_id", n.CorrelationID,
		"channel", n.Channel, "priority", n.Priority.String(), "attempt", n.Attempts)

	o := p.deliver(ctx, n)
	err = p.store.Finish(ctx, n.ID, n.Attempts, o)
	switch {
	case errors.Is(err, domain.ErrLeaseLost):
		log.Warn("lease lost before the result was saved; another worker owns this notification now")
		return
	case err != nil:
		// The lease will expire and the notification will be tried again.
		log.Error("save outcome", "error", err)
		return
	}

	attrs := []any{"status", o.Status, "duration_ms", o.Duration.Milliseconds()}
	if o.HTTPStatus != 0 {
		attrs = append(attrs, "http_status", o.HTTPStatus)
	}
	switch o.Status {
	case domain.StatusSent:
		log.Info("delivered", append(attrs, "provider_message_id", o.ProviderMessageID)...)
	case domain.StatusPending:
		log.Warn("delivery failed, will retry", append(attrs, "error", o.Error, "next_attempt_at", o.NextAttemptAt)...)
	default:
		log.Error("delivery failed permanently", append(attrs, "error", o.Error)...)
	}
}

// deliver calls the provider and turns its answer into an outcome.
func (p *Pool) deliver(ctx context.Context, n domain.Notification) domain.Outcome {
	sender, ok := p.senders[n.Channel]
	if !ok {
		return domain.Outcome{Status: domain.StatusFailed, Error: "no provider configured for channel " + string(n.Channel)}
	}

	start := time.Now()
	res, err := sender.Send(ctx, provider.Message{
		ID: n.ID, To: n.Recipient, Channel: string(n.Channel), Content: n.Content, CorrelationID: n.CorrelationID,
	})
	o := domain.Outcome{Duration: time.Since(start)}
	if err == nil {
		o.Status, o.ProviderMessageID, o.HTTPStatus = domain.StatusSent, res.MessageID, res.StatusCode
		return o
	}

	o.Error = err.Error()
	var pe *provider.Error
	if errors.As(err, &pe) {
		o.HTTPStatus = pe.StatusCode
	}
	retryable := pe != nil && pe.Retryable
	switch {
	case !retryable:
		o.Status = domain.StatusFailed
	case n.Attempts >= n.MaxAttempts:
		o.Status = domain.StatusFailed
		o.Error = "retries exhausted: " + o.Error
	default:
		o.Status = domain.StatusPending
		o.NextAttemptAt = time.Now().Add(p.cfg.RetryDelay)
	}
	return o
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
