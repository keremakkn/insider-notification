package events

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

func TestPublishReachesSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rdb, stop, err := testutil.StartRedis(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ev := NewRedis(rdb, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan domain.StatusChange, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = ev.Subscribe(ctx, func(_ []byte, c domain.StatusChange) { got <- c })
	}()
	<-ready

	want := domain.StatusChange{ID: uuid.New(), Status: domain.StatusSent, Attempts: 1, At: time.Now().UTC()}
	// The subscription may need a moment; Pub/Sub drops messages nobody hears.
	deadline := time.After(3 * time.Second)
	for {
		ev.Publish(ctx, want)
		select {
		case c := <-got:
			if c.ID != want.ID || c.Status != domain.StatusSent {
				t.Fatalf("got %+v", c)
			}
			return
		case <-deadline:
			t.Fatal("no change received")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
