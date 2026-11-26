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
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/events"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/provider"
	"github.com/keremakkn/insider-notification/internal/tracing"
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
	Backoff           Backoff       // wait before a retryable failure is tried again
	BreakerThreshold  int           // consecutive provider failures that open a channel's breaker
	BreakerCooldown   time.Duration // how long it stays open before one probe
	Metrics           *metrics.Delivery
	Events            events.Publisher // announces every status change
}

type Pool struct {
	store    Store
	queue    Queue
	senders  map[domain.Channel]provider.Sender
	breakers map[domain.Channel]*Breaker
	cfg      Config
	log      *slog.Logger
}

func NewPool(store Store, queue Queue, senders map[domain.Channel]provider.Sender, cfg Config, log *slog.Logger) *Pool {
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewDelivery(prometheus.NewRegistry())
	}
	if cfg.Events == nil {
		cfg.Events = events.Nop{}
	}
	p := &Pool{store: store, queue: queue, senders: senders, cfg: cfg, log: log.With("component", "worker"),
		breakers: map[domain.Channel]*Breaker{}}
	for _, c := range domain.Channels {
		c := c
		p.breakers[c] = NewBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown, func(from, to BreakerState) {
			level := slog.LevelInfo
			if to == BreakerOpen {
				level = slog.LevelWarn
			}
			p.log.Log(context.Background(), level, "circuit breaker "+string(to), "channel", c, "from", from)
			p.cfg.Metrics.BreakerState.WithLabelValues(string(c)).Set(breakerGauge[to])
		})
	}
	return p
}

var breakerGauge = map[BreakerState]float64{BreakerClosed: 0, BreakerHalfOpen: 1, BreakerOpen: 2}

// Breaker exposes a channel's breaker, for health and metrics.
func (p *Pool) Breaker(c domain.Channel) *Breaker { return p.breakers[c] }

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
				p.loop(ctx, c, limiter, p.breakers[c])
			}()
		}
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, c domain.Channel, limiter *rate.Limiter, breaker *Breaker) {
	for ctx.Err() == nil {
		// While the breaker is open, leave messages where they are: in Redis
		// and PostgreSQL, with their attempts untouched.
		ticket, ok, wait := breaker.Allow()
		if !ok {
			sleep(ctx, min(wait, time.Second))
			continue
		}

		id, _, ok, err := p.queue.Pop(ctx, c)
		if ctx.Err() != nil {
			// Shutting down. A popped but unclaimed ID is not lost: the
			// dispatcher re-queues it after the visibility timeout.
			breaker.Done(ticket, ResultNone)
			return
		}
		if err != nil || !ok {
			breaker.Done(ticket, ResultNone)
			if err != nil {
				p.log.Error("pop", "channel", c, "error", err)
				sleep(ctx, time.Second) // Redis is struggling; do not spin
			}
			continue
		}

		// Finish an accepted delivery even during shutdown: abandoning it
		// mid-request would only cause a resend after the lease expires.
		breaker.Done(ticket, p.process(context.WithoutCancel(ctx), id, limiter))
	}
}

func (p *Pool) process(ctx context.Context, id uuid.UUID, limiter *rate.Limiter) Result {
	n, ok, err := p.store.Claim(ctx, id, p.cfg.Lease)
	if err != nil {
		p.log.Error("claim", "notification_id", id, "error", err)
		return ResultNone
	}
	if !ok {
		// Cancelled, taken by another worker, finished, or a stale copy
		// whose next attempt is not due yet.
		p.log.Debug("skipped", "notification_id", id)
		return ResultNone
	}
	log := p.log.With("notification_id", n.ID, "correlation_id", n.CorrelationID,
		"channel", n.Channel, "priority", n.Priority.String(), "attempt", n.Attempts)
	p.cfg.Events.Publish(ctx, domain.ChangeOf(n, time.Now()))

	// Continue the trace of the request that created the notification. The
	// queue broke the in-process context; the row carried it across.
	ctx, span := tracing.Tracer().Start(tracing.Extract(ctx, n.TraceParent), "notification.deliver",
		trace.WithAttributes(attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.channel", string(n.Channel)), attribute.Int("notification.attempt", n.Attempts)))
	defer span.End()

	// The token is taken here, after the claim and right before the provider
	// call, so nothing with a variable duration sits between the limiter and
	// the request. Taken earlier, it would be spent late: while blocked on an
	// empty queue (idle workers then burst together), or behind a claim
	// query that takes 5 ms for one worker and 50 ms for another. Waiting
	// here cannot cost the lease: a worker waits for at most the other
	// workers of its channel, workers/rate = 20/100 = 0.2 s, against a 30 s lease.
	_, wait := tracing.Tracer().Start(ctx, "rate_limit.wait")
	_ = limiter.Wait(ctx) // ctx is not cancellable here, so Wait cannot fail
	wait.End()

	o, result := p.deliver(ctx, n)
	p.record(n, o, result)
	span.SetAttributes(attribute.String("notification.outcome", string(o.Status)))
	if o.Status != domain.StatusSent {
		span.SetStatus(codes.Error, o.Error)
	}
	err = p.store.Finish(ctx, n.ID, n.Attempts, o)
	switch {
	case errors.Is(err, domain.ErrLeaseLost):
		log.Warn("lease lost before the result was saved; another worker owns this notification now")
		return result
	case err != nil:
		// The lease will expire and the notification will be tried again.
		log.Error("save outcome", "error", err)
		return result
	}

	n.Status, n.LastError = o.Status, o.Error
	p.cfg.Events.Publish(ctx, domain.ChangeOf(n, time.Now()))

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
	return result
}

func (p *Pool) record(n domain.Notification, o domain.Outcome, r Result) {
	m, ch := p.cfg.Metrics, string(n.Channel)
	if r != ResultNone {
		m.ProviderLatency.WithLabelValues(ch).Observe(o.Duration.Seconds())
	}
	outcome := map[domain.Status]string{domain.StatusSent: "sent", domain.StatusPending: "retry", domain.StatusFailed: "failed"}[o.Status]
	m.Attempts.WithLabelValues(ch, outcome).Inc()
	if o.Status == domain.StatusSent {
		m.EndToEnd.WithLabelValues(ch, n.Priority.String()).Observe(time.Since(n.CreatedAt).Seconds())
	}
}

// deliver calls the provider and turns its answer into an outcome for the
// notification and a result for the breaker.
func (p *Pool) deliver(ctx context.Context, n domain.Notification) (domain.Outcome, Result) {
	sender, ok := p.senders[n.Channel]
	if !ok {
		return domain.Outcome{Status: domain.StatusFailed, Error: "no provider configured for channel " + string(n.Channel)}, ResultNone
	}

	start := time.Now()
	res, err := sender.Send(ctx, provider.Message{
		ID: n.ID, To: n.Recipient, Channel: string(n.Channel), Content: n.Content, CorrelationID: n.CorrelationID,
	})
	o := domain.Outcome{Duration: time.Since(start)}
	if err == nil {
		o.Status, o.ProviderMessageID, o.HTTPStatus = domain.StatusSent, res.MessageID, res.StatusCode
		return o, ResultSuccess
	}

	o.Error = err.Error()
	var pe *provider.Error
	if errors.As(err, &pe) {
		o.HTTPStatus = pe.StatusCode
	}
	if pe == nil || !pe.Retryable {
		// The provider answered and rejected this message: it is up, the
		// message is the problem. Do not count it against the provider.
		o.Status = domain.StatusFailed
		return o, ResultSuccess
	}
	if n.Attempts >= n.MaxAttempts {
		o.Status = domain.StatusFailed
		o.Error = "retries exhausted: " + o.Error
		return o, ResultFailure
	}
	o.Status = domain.StatusPending
	o.NextAttemptAt = time.Now().Add(p.cfg.Backoff.Delay(n.Attempts, pe.RetryAfter))
	return o, ResultFailure
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
