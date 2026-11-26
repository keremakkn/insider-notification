// Command worker runs the delivery side: the dispatcher that feeds the
// Redis queues from PostgreSQL, and the worker pools that drain them.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/keremakkn/insider-notification/internal/config"
	"github.com/keremakkn/insider-notification/internal/dispatcher"
	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/events"
	"github.com/keremakkn/insider-notification/internal/health"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/provider"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/repository"
	"github.com/keremakkn/insider-notification/internal/tracing"
	"github.com/keremakkn/insider-notification/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	if cfg.ProviderURL == "" {
		return errors.New("PROVIDER_URL is required: set it to your webhook.site URL (see .env.example)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Setup(ctx, "notifier-worker")
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx) // send the last spans before exiting
	}()

	db, err := repository.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	// One connection per worker that can block in BLPOP, plus room for the
	// dispatcher, metrics and status events.
	rdb, err := queue.Connect(ctx, cfg.RedisAddr, len(domain.Channels)*cfg.WorkersPerChannel+10)
	if err != nil {
		return err
	}
	defer rdb.Close()

	repo := repository.NewNotifications(db)
	q := queue.New(rdb, cfg.PriorityWeights, time.Second)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.NewDelivery(reg)
	ev := events.NewRedis(rdb, log)
	metrics.RegisterQueueDepth(reg, q)
	disp := dispatcher.New(repo, q, dispatcher.Config{
		Interval:          cfg.DispatchInterval,
		Thresholds:        cfg.QueueThresholds,
		VisibilityTimeout: cfg.VisibilityTimeout,
		Metrics:           m,
		Events:            ev,
	}, log)

	// Every channel uses the same provider here; each could get its own.
	sender := provider.NewHTTP(cfg.ProviderURL, cfg.ProviderTimeout)
	senders := map[domain.Channel]provider.Sender{
		domain.ChannelSMS: sender, domain.ChannelEmail: sender, domain.ChannelPush: sender,
	}
	pool := worker.NewPool(repo, q, senders, worker.Config{
		WorkersPerChannel: cfg.WorkersPerChannel,
		RatePerSecond:     cfg.RateLimitPerChannel,
		Lease:             cfg.LeaseDuration,
		Backoff:           worker.NewBackoff(cfg.RetryBase, cfg.RetryCap),
		BreakerThreshold:  cfg.BreakerThreshold,
		BreakerCooldown:   cfg.BreakerCooldown,
		Metrics:           m,
		Events:            ev,
	}, log)

	// The worker serves no API, only probes and metrics for operators.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", health.Live)
	mux.HandleFunc("/readyz", health.Ready(
		health.Check{Name: "postgres", Ping: db.Ping},
		health.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
	))
	ops := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "error", err)
		}
	}()
	defer ops.Close()

	log.Info("worker started", "workers_per_channel", cfg.WorkersPerChannel, "rate_per_channel", cfg.RateLimitPerChannel)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); disp.Run(ctx) }()
	go func() { defer wg.Done(); pool.Run(ctx) }() // finishes in-flight deliveries before returning
	wg.Wait()
	log.Info("worker stopped")
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})).With("service", "worker")
}
