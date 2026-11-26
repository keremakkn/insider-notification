package repository

import (
	"context"
	"testing"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func TestStats(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	finish := func(st domain.Status, http int, ms time.Duration) {
		n, _, _ := repo.Create(ctx, newNotification(t, "", time.Now().Add(-time.Second)))
		c, _, _ := repo.Claim(ctx, n.ID, 30*time.Second)
		if err := repo.Finish(ctx, n.ID, c.Attempts, domain.Outcome{Status: st, HTTPStatus: http, Duration: ms, Error: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		finish(domain.StatusSent, 202, 100*time.Millisecond)
	}
	finish(domain.StatusFailed, 400, 300*time.Millisecond)
	_, _, _ = repo.Create(ctx, newNotification(t, "", time.Now())) // still pending, due now
	later := newNotification(t, "", time.Now())
	at := time.Now().Add(time.Hour)
	later.ScheduledAt, later.NextAttemptAt = &at, at
	_, _, _ = repo.Create(ctx, later) // pending, but scheduled for later

	s, err := repo.Stats(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sms := s.Channels[domain.ChannelSMS]
	if sms.Pending != 2 || sms.NotYetDue != 1 || sms.Sent != 3 || sms.Failed != 1 || sms.AttemptsOK != 3 || sms.AttemptsFailed != 1 {
		t.Fatalf("counts: %+v", sms)
	}
	if *sms.SuccessRate() != 0.75 || sms.ProviderP50 != 100*time.Millisecond || sms.EndToEndP50 <= 0 {
		t.Fatalf("rates and latency: rate=%v p50=%s e2e=%s", *sms.SuccessRate(), sms.ProviderP50, sms.EndToEndP50)
	}
	if s.Channels[domain.ChannelEmail].Pending != 0 {
		t.Fatal("other channels must be present and empty")
	}
}
