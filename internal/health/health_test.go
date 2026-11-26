package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadyReportsEveryCheck(t *testing.T) {
	ok := Check{Name: "postgres", Ping: func(context.Context) error { return nil }}
	down := Check{Name: "redis", Ping: func(context.Context) error { return errors.New("connection refused") }}

	rec := httptest.NewRecorder()
	Ready(ok, down)(rec, httptest.NewRequest("GET", "/readyz", nil))
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusServiceUnavailable || body.Checks["postgres"] != "ok" || body.Checks["redis"] != "connection refused" {
		t.Fatalf("want 503 naming the failing dependency, got %d %+v", rec.Code, body)
	}

	rec = httptest.NewRecorder()
	Ready(ok)(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("all checks pass: want 200, got %d", rec.Code)
	}
}

func TestLiveNeverChecksDependencies(t *testing.T) {
	rec := httptest.NewRecorder()
	Live(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}
