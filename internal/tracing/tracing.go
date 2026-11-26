// Package tracing sets up OpenTelemetry and carries a trace across the
// queue: the creating request's context is stored on the notification and
// restored by the worker that delivers it.
package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/keremakkn/insider-notification"

// Tracer is the service's tracer. It follows the global provider, so tests
// can install a recording one.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// Setup exports spans over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT is
// set (Jaeger in docker-compose). Without it tracing stays a no-op, so
// tests and local runs need no collector. The returned function flushes
// buffered spans on shutdown.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx) // reads OTEL_EXPORTER_OTLP_* from the environment
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Inject returns the W3C traceparent of the span in ctx, or "" if there is none.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract returns ctx with the remote span described by traceparent as its
// parent, so spans started from it join the original trace.
func Extract(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
