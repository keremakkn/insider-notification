package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremakkn/insider-notification/internal/tracing"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func TestCreateIsTracedAndCarriesItsTraceForward(t *testing.T) {
	spans := recordSpans(t)
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications", smsBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d", rec.Code)
	}
	n := decode[notificationResponse](t, rec)

	var server, create sdktrace.ReadOnlySpan
	for _, sp := range spans.Ended() {
		switch sp.Name() {
		case "POST /api/v1/notifications":
			server = sp
		case "notifications.create":
			create = sp
		}
	}
	if server == nil || create == nil {
		t.Fatalf("want a server span named by route and a service span, got %d spans", len(spans.Ended()))
	}
	if create.Parent().SpanID() != server.SpanContext().SpanID() {
		t.Fatal("the service span must be a child of the request span")
	}
	traceID := server.SpanContext().TraceID().String()
	if n.CorrelationID != traceID {
		t.Fatalf("without X-Correlation-ID the correlation ID is the trace ID: %s vs %s", n.CorrelationID, traceID)
	}

	stored, _ := s.repo.Get(context.Background(), uuid.MustParse(n.ID))
	parent := trace.SpanContextFromContext(tracing.Extract(context.Background(), stored.TraceParent))
	if parent.TraceID() != server.SpanContext().TraceID() || parent.SpanID() != create.SpanContext().SpanID() {
		t.Fatal("the stored traceparent must point at the create span, so the worker continues this trace")
	}
}

func TestClientCorrelationIDIsKeptAndSearchable(t *testing.T) {
	spans := recordSpans(t)
	s := newTestServer(t)
	n := decode[notificationResponse](t, s.do("POST", "/api/v1/notifications", smsBody, CorrelationHeader, "order-8812"))
	if n.CorrelationID != "order-8812" {
		t.Fatalf("the client's ID wins: %s", n.CorrelationID)
	}
	for _, sp := range spans.Ended() {
		for _, a := range sp.Attributes() {
			if a.Key == "correlation_id" && a.Value.AsString() == "order-8812" {
				return
			}
		}
	}
	t.Fatal("the client's correlation ID should be on the span, to find the trace by it")
}
