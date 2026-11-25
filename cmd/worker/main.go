// Command worker runs the delivery side: the dispatcher that feeds the
// Redis queues from PostgreSQL, and (next) the worker pools that drain them.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/keremakkn/insider-notification/internal/config"
	"github.com/keremakkn/insider-notification/internal/dispatcher"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/repository"
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

	log.Info("worker started")
	disp.Run(ctx) // returns when ctx is cancelled
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
