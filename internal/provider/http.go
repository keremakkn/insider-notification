// Package provider sends notifications to the external provider and
// classifies its answers. It does not touch the database.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
)

// Message is what the provider receives.
type Message struct {
	ID            uuid.UUID // sent as X-Idempotency-Key, so the provider can drop a resend
	To            string
	Channel       string
	Content       string
	CorrelationID string
}

// Result is a successful delivery.
type Result struct {
	MessageID  string
	StatusCode int
}

// Error is a failed delivery, classified so the worker can decide what to do.
type Error struct {
	StatusCode int           // 0 when no response arrived (timeout, connection error)
	Retryable  bool          // 5xx, 408, 429, timeouts and connection errors
	RetryAfter time.Duration // from a Retry-After header, 0 if absent
	Err        error
}

func (e *Error) Error() string {
	if e.StatusCode == 0 {
		return e.Err.Error()
	}
	return fmt.Sprintf("provider answered %d: %v", e.StatusCode, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Sender is one provider. The worker holds one per channel (Strategy), so a
// channel can move to its own provider without touching delivery logic.
type Sender interface {
	Send(ctx context.Context, m Message) (Result, error)
}

// HTTP posts {to, channel, content} to a URL, as the assessment's provider expects.
type HTTP struct {
	url    string
	client *http.Client
}

func NewHTTP(url string, timeout time.Duration) *HTTP {
	// otelhttp records a client span per call and sends the traceparent
	// header, so a provider that traces can join the same trace. The
	// propagator is explicit rather than the global default, which writes
	// nothing unless someone remembered to configure it.
	transport := otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithPropagators(propagation.TraceContext{}))
	return &HTTP{url: url, client: &http.Client{Timeout: timeout, Transport: transport}}
}

const maxResponseBytes = 64 << 10

func (h *HTTP) Send(ctx context.Context, m Message) (Result, error) {
	body, _ := json.Marshal(struct {
		To      string `json:"to"`
		Channel string `json:"channel"`
		Content string `json:"content"`
	}{m.To, m.Channel, m.Content})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, &Error{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", m.ID.String())
	if m.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", m.CorrelationID)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		// No answer at all: the provider may or may not have accepted it.
		// Retrying is safe because of at-least-once plus the idempotency key.
		return Result{}, &Error{Retryable: true, Err: err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var ok struct {
			MessageID string `json:"messageId"`
		}
		_ = json.Unmarshal(raw, &ok) // a missing id does not undo an accepted message
		return Result{MessageID: ok.MessageID, StatusCode: resp.StatusCode}, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return Result{}, &Error{StatusCode: resp.StatusCode, Retryable: true,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), Err: errors.New("rate limited")}
	case resp.StatusCode >= 500, resp.StatusCode == http.StatusRequestTimeout:
		// 408: the provider gave up waiting for the request; it says nothing
		// about the message, so sending it again can work.
		return Result{}, &Error{StatusCode: resp.StatusCode, Retryable: true, Err: errors.New(snippet(raw))}
	default:
		// Other 4xx: the request itself is wrong; sending it again will not help.
		return Result{}, &Error{StatusCode: resp.StatusCode, Err: errors.New(snippet(raw))}
	}
}

// parseRetryAfter accepts both forms of the header: seconds or an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func snippet(b []byte) string {
	const n = 200
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	if len(b) == 0 {
		return "empty response"
	}
	return string(b)
}
