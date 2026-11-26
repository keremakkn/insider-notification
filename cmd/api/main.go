// Command api runs the notification service's HTTP API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/keremakkn/insider-notification/internal/api"
	"github.com/keremakkn/insider-notification/internal/config"
	"github.com/keremakkn/insider-notification/internal/events"
	"github.com/keremakkn/insider-notification/internal/health"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/realtime"
	"github.com/keremakkn/insider-notification/internal/repository"
	"github.com/keremakkn/insider-notification/internal/service"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := repository.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb, err := queue.Connect(ctx, cfg.RedisAddr)
	if err != nil {
		return err
	}
	defer rdb.Close()

	repo := repository.NewNotifications(db)
	q := queue.New(rdb, cfg.PriorityWeights, time.Second)
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics.RegisterQueueDepth(reg, q)

	templates := repository.NewTemplates(db)
	ev := events.NewRedis(rdb, log)
	hub := realtime.NewHub()
	go func() {
		// Feed this process's WebSocket clients from every process's changes.
		for ctx.Err() == nil {
			if err := ev.Subscribe(ctx, hub.Deliver); err != nil {
				log.Error("status change subscription", "error", err)
				time.Sleep(time.Second)
			}
		}
	}()
	router := api.NewRouter(api.Options{
		Service:   service.NewNotifications(repo, templates, cfg.MaxAttempts).PublishTo(ev),
		Hub:       hub,
		WSOrigins: cfg.WSOrigins,
		Templates: service.NewTemplates(templates),
		Log:       log,
		Metrics:   metrics.NewAPI(reg),
		Gatherer:  reg,
		Ready: []health.Check{
			{Name: "postgres", Ping: db.Ping},
			{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
		Stats: service.NewStats(repo, q),
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("api started", "addr", cfg.HTTPAddr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Stop accepting connections and let in-flight requests finish.
	log.Info("api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("api stopped")
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})).With("service", "api")
}
