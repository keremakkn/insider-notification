package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "create")
	tp2 := span.SpanContext()

	header := Inject(ctx)
	if header == "" {
		t.Fatal("a span in ctx must produce a traceparent")
	}
	restored := trace.SpanContextFromContext(Extract(context.Background(), header))
	if restored.TraceID() != tp2.TraceID() || restored.SpanID() != tp2.SpanID() || !restored.IsRemote() {
		t.Fatalf("round trip lost the context: %v vs %v", restored, tp2)
	}
	if Inject(context.Background()) != "" {
		t.Fatal("no span, no traceparent")
	}
	if Extract(ctx, "") != ctx {
		t.Fatal("an empty traceparent leaves ctx alone")
	}
}
