// Package metrics defines the Prometheus metrics of both processes.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// API is what the api process records.
type API struct {
	Requests *prometheus.HistogramVec // by method, route pattern, status
	Created  *prometheus.CounterVec   // by channel, priority
}

func NewAPI(reg prometheus.Registerer) *API {
	m := &API{
		Requests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		Created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted by the API (idempotent replays excluded).",
		}, []string{"channel", "priority"}),
	}
	reg.MustRegister(m.Requests, m.Created)
	return m
}

// Delivery is what the worker process records.
type Delivery struct {
	Attempts        *prometheus.CounterVec   // by channel, outcome (sent, retry, failed)
	ProviderLatency *prometheus.HistogramVec // by channel
	EndToEnd        *prometheus.HistogramVec // created → sent, by channel and priority
	BreakerState    *prometheus.GaugeVec     // by channel: 0 closed, 1 half-open, 2 open
	Dispatched      prometheus.Counter
	LeasesRecovered prometheus.Counter
	LeasesExhausted prometheus.Counter
}

func NewDelivery(reg prometheus.Registerer) *Delivery {
	m := &Delivery{
		Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "delivery_attempts_total",
			Help: "Provider calls by outcome: sent, retry (will be tried again) or failed (final).",
		}, []string{"channel", "outcome"}),
		ProviderLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_request_duration_seconds",
			Help:    "Latency of one provider call.",
			Buckets: []float64{.025, .05, .1, .2, .5, 1, 2, 5},
		}, []string{"channel"}),
		EndToEnd: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_seconds",
			Help:    "Time from creation to sent, including queueing and retries.",
			Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900},
		}, []string{"channel", "priority"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker per channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
		Dispatched: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_enqueued_total", Help: "IDs moved from PostgreSQL into the Redis queues.",
		}),
		LeasesRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_leases_recovered_total", Help: "Expired leases returned to pending.",
		}),
		LeasesExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dispatcher_leases_exhausted_total", Help: "Expired leases with no attempts left, marked failed.",
		}),
	}
	reg.MustRegister(m.Attempts, m.ProviderLatency, m.EndToEnd, m.BreakerState, m.Dispatched, m.LeasesRecovered, m.LeasesExhausted)
	for _, c := range domain.Channels {
		m.BreakerState.WithLabelValues(string(c)).Set(0)
	}
	return m
}

// Lener reports a queue's length.
type Lener interface {
	Len(ctx context.Context, c domain.Channel, p domain.Priority) (int, error)
}

// queueDepth reads every Redis queue's length when Prometheus scrapes, so
// the value is always current and nothing has to keep it updated.
type queueDepth struct {
	q    Lener
	desc *prometheus.Desc
}

// RegisterQueueDepth exposes notification_queue_depth{channel, priority}.
func RegisterQueueDepth(reg prometheus.Registerer, q Lener) {
	reg.MustRegister(&queueDepth{q: q, desc: prometheus.NewDesc("notification_queue_depth",
		"IDs waiting in a Redis queue.", []string{"channel", "priority"}, nil)})
}

func (d *queueDepth) Describe(ch chan<- *prometheus.Desc) { ch <- d.desc }

func (d *queueDepth) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, c := range domain.Channels {
		for _, p := range domain.Priorities {
			n, err := d.q.Len(ctx, c, p)
			if err != nil {
				ch <- prometheus.NewInvalidMetric(d.desc, err)
				continue
			}
			ch <- prometheus.MustNewConstMetric(d.desc, prometheus.GaugeValue, float64(n), string(c), p.String())
		}
	}
}
