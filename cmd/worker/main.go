// Command worker runs the delivery side: the dispatcher that feeds the
// Redis queues from PostgreSQL, and the worker pools that drain them.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/keremakkn/insider-notification/internal/config"
	"github.com/keremakkn/insider-notification/internal/dispatcher"
	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/provider"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/repository"
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
	disp := dispatcher.New(repo, q, dispatcher.Config{
		Interval:          cfg.DispatchInterval,
		Thresholds:        cfg.QueueThresholds,
		VisibilityTimeout: cfg.VisibilityTimeout,
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
		RetryDelay:        cfg.RetryBase,
	}, log)

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
