package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremakkn/insider-notification/internal/metrics"
)

const CorrelationHeader = "X-Correlation-ID"

type ctxKey struct{}

// CorrelationIDFrom returns the request's correlation ID, or "" outside a request.
func CorrelationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// Only accept IDs that are safe to log and store; anything else is replaced.
var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// correlationID reuses the caller's X-Correlation-ID when it is well formed.
// Otherwise it is the request's trace ID, so one ID finds both the log lines
// and the trace. A caller's own ID is added to the span, so the trace can be
// found by it too.
func correlationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationHeader)
		span := trace.SpanFromContext(r.Context())
		switch {
		case validCorrelationID.MatchString(id):
			span.SetAttributes(attribute.String("correlation_id", id))
		case span.SpanContext().HasTraceID():
			id = span.SpanContext().TraceID().String()
		default:
			id = uuid.NewString()
		}
		w.Header().Set(CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

// quiet paths are polled by infrastructure every few seconds; logging them
// would drown the lines that matter.
var quiet = map[string]bool{"/healthz": true, "/readyz": true, "/metrics": true}

// requestLogger writes one structured line per request.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if quiet[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			level := slog.LevelInfo
			if ww.Status() >= 500 {
				level = slog.LevelError
			}
			log.LogAttrs(r.Context(), level, "http request",
				slog.String("correlation_id", CorrelationIDFrom(r.Context())),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", ww.Status()),
				slog.Int("bytes", ww.BytesWritten()),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}

// recoverer turns a panic into a 500 and logs it, instead of killing the connection.
func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if p := recover(); p != nil {
					if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						panic(p) // net/http's way to abort a response; let it through
					}
					log.ErrorContext(r.Context(), "panic in handler",
						"correlation_id", CorrelationIDFrom(r.Context()), "panic", p)
					writeJSON(w, http.StatusInternalServerError, errorBody("internal", "internal server error", nil))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// observe records request latency by route pattern (/api/v1/notifications/{id},
// not the concrete ID), so the number of time series stays bounded.
func observe(m *metrics.API) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			route := "unmatched"
			if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
				route = rc.RoutePattern()
			}
			// The route is only known after routing; name the span by it.
			trace.SpanFromContext(r.Context()).SetName(r.Method + " " + route)
			m.Requests.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).Observe(time.Since(start).Seconds())
		})
	}
}
