package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/health"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/service"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

type fakeStats struct{ s domain.Stats }

func (f fakeStats) Stats(_ context.Context, w time.Duration) (domain.Stats, error) {
	f.s.Window = w
	return f.s, nil
}

func observedServer(t *testing.T, ready ...health.Check) (http.Handler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	sms := &domain.ChannelStats{QueueDepth: map[domain.Priority]int{domain.PriorityHigh: 12}, Pending: 40,
		AttemptsOK: 98, AttemptsFailed: 2, Sent: 95, ProviderP50: 80 * time.Millisecond, EndToEndP95: 1500 * time.Millisecond}
	srv := NewRouter(Options{
		Service:  service.NewNotifications(testutil.NewMemRepo(), testutil.NewMemTemplates(), 5),
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  metrics.NewAPI(reg),
		Gatherer: reg,
		Ready:    ready,
		Stats:    fakeStats{domain.Stats{Channels: map[domain.Channel]*domain.ChannelStats{domain.ChannelSMS: sms}}},
	})
	return srv, reg
}

func get(srv http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _ := observedServer(t)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/notifications", strings.NewReader(smsBody)))
	get(srv, "/api/v1/notifications/"+uuid.NewString()) // a 404 on a parameterised route

	body := get(srv, "/metrics").Body.String()
	for _, want := range []string{
		`notifications_created_total{channel="sms",priority="high"} 1`,
		`http_request_duration_seconds_count{method="POST",route="/api/v1/notifications",status="202"} 1`,
		// The route pattern, not the concrete ID: one series however many IDs are requested.
		`http_request_duration_seconds_count{method="GET",route="/api/v1/notifications/{id}",status="404"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}
}

func TestReadiness(t *testing.T) {
	down := health.Check{Name: "redis", Ping: func(context.Context) error { return errors.New("down") }}
	srv, _ := observedServer(t, down)
	if rec := get(srv, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("liveness must not depend on Redis: %d", rec.Code)
	}
	if rec := get(srv, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readiness must fail without Redis: %d", rec.Code)
	}
}

func TestStatsEndpoint(t *testing.T) {
	srv, _ := observedServer(t)
	rec := get(srv, "/api/v1/stats?window=15m")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := decode[statsResponse](t, rec)
	sms := body.Channels["sms"]
	if body.Window != "15m0s" || sms.QueueDepth["high"] != 12 || sms.Pending != 40 || sms.Attempts != 100 ||
		sms.SuccessRate == nil || *sms.SuccessRate != 0.98 || sms.ProviderLatencyMs.P50 != 80 || sms.EndToEndMs.P95 != 1500 {
		t.Fatalf("unexpected stats: %+v", sms)
	}
	if email := body.Channels["email"]; email.SuccessRate != nil {
		t.Errorf("no attempts means no rate (null), not 0%%: %+v", email)
	}
	if rec := get(srv, "/api/v1/stats?window=forever"); rec.Code != http.StatusBadRequest {
		t.Errorf("bad window: want 400, got %d", rec.Code)
	}
}
