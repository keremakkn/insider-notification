package api

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
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

// correlationID reuses the caller's X-Correlation-ID when it is well formed,
// or generates one, and echoes it in the response.
func correlationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationHeader)
		if !validCorrelationID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set(CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

// requestLogger writes one structured line per request.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
					if p == http.ErrAbortHandler {
						panic(p)
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
