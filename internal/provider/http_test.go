package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func server(t *testing.T, h http.HandlerFunc) *HTTP {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return NewHTTP(s.URL, 200*time.Millisecond)
}

var msg = Message{ID: uuid.New(), To: "+905551234567", Channel: "sms", Content: "Merhaba", CorrelationID: "corr-1"}

func TestSendsTheExpectedRequest(t *testing.T) {
	var (
		body    map[string]any
		headers http.Header
	)
	h := server(t, func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-11-26T00:00:00Z"}`))
	})

	res, err := h.Send(context.Background(), msg)
	if err != nil || res.MessageID != "m-1" || res.StatusCode != 202 {
		t.Fatalf("got %+v, %v", res, err)
	}
	if len(body) != 3 || body["to"] != msg.To || body["channel"] != "sms" || body["content"] != "Merhaba" {
		t.Errorf("body must be exactly {to, channel, content}, got %v", body)
	}
	if headers.Get("X-Idempotency-Key") != msg.ID.String() || headers.Get("X-Correlation-ID") != "corr-1" {
		t.Errorf("missing headers: %v", headers)
	}
}

func TestAnySuccessCodeIsSuccess(t *testing.T) {
	h := server(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if _, err := h.Send(context.Background(), msg); err != nil {
		t.Fatalf("200 without a body is still accepted: %v", err)
	}
}

func TestClassifiesFailures(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		status     int
		retryable  bool
		retryAfter time.Duration
	}{
		{"server error", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, 503, true, 0},
		{"rate limited with seconds", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
		}, 429, true, 7 * time.Second},
		{"bad request is permanent", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"invalid number"}`))
		}, 400, false, 0},
		{"timeout", func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(time.Second):
			case <-r.Context().Done():
			}
		}, 0, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server(t, tt.handler).Send(context.Background(), msg)
			var pe *Error
			if !errors.As(err, &pe) {
				t.Fatalf("want *Error, got %v", err)
			}
			if pe.StatusCode != tt.status || pe.Retryable != tt.retryable || pe.RetryAfter != tt.retryAfter {
				t.Fatalf("got status=%d retryable=%v retryAfter=%s", pe.StatusCode, pe.Retryable, pe.RetryAfter)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	if d := parseRetryAfter("Thu, 26 Nov 2026 12:00:30 GMT", now); d != 30*time.Second {
		t.Errorf("HTTP date: got %s", d)
	}
	for _, v := range []string{"", "-5", "soon", "Thu, 26 Nov 2026 11:00:00 GMT"} {
		if d := parseRetryAfter(v, now); d != 0 {
			t.Errorf("%q: want 0, got %s", v, d)
		}
	}
}

func TestConnectionRefusedIsRetryable(t *testing.T) {
	_, err := NewHTTP("http://127.0.0.1:1", time.Second).Send(context.Background(), msg)
	var pe *Error
	if !errors.As(err, &pe) || !pe.Retryable {
		t.Fatalf("want a retryable error, got %v", err)
	}
}
