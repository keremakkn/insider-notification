// Package api is the HTTP transport: it parses requests, calls the service
// and maps results and errors to status codes. It holds no business rules.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/health"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/realtime"
	"github.com/keremakkn/insider-notification/internal/service"
)

// Service is what the handlers need. Declared here so tests can fake it if needed.
type Service interface {
	Create(ctx context.Context, req domain.CreateRequest, correlationID string) (domain.Notification, bool, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error)
	CreateBatch(ctx context.Context, reqs []domain.CreateRequest, correlationID string) (service.BatchResult, error)
	GetBatch(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error)
}

const (
	IdempotencyHeader = "Idempotency-Key"
	maxBodyBytes      = 1 << 20  // 1 MiB: a 100 KB email plus JSON overhead fits easily
	maxBatchBodyBytes = 32 << 20 // 32 MiB: 1000 items; very large emails belong in smaller batches
)

// StatsSource produces the /api/v1/stats snapshot.
type StatsSource interface {
	Stats(ctx context.Context, window time.Duration) (domain.Stats, error)
}

// Options are the router's dependencies. Only Service and Log are required.
type Options struct {
	Service   Service
	Log       *slog.Logger
	Metrics   *metrics.API        // records request latency and creations
	Gatherer  prometheus.Gatherer // served at /metrics
	Ready     []health.Check      // checked by /readyz
	Stats     StatsSource         // served at /api/v1/stats
	Templates TemplateService     // served at /api/v1/templates
	Hub       *realtime.Hub       // serves /api/v1/ws when set
	WSOrigins []string            // browser origins allowed to open a WebSocket; non-browser clients send none
}

// TemplateService manages message templates.
type TemplateService interface {
	Create(ctx context.Context, name, channel, body string) (domain.Template, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Template, error)
	List(ctx context.Context) ([]domain.Template, error)
	Update(ctx context.Context, id uuid.UUID, body string) (domain.Template, error)
}

type handlers struct {
	svc       Service
	log       *slog.Logger
	metrics   *metrics.API
	stats     StatsSource
	templates TemplateService
	hub       *realtime.Hub
	wsOrigins []string
}

// NewRouter wires routes and middleware.
func NewRouter(o Options) http.Handler {
	// The server span wraps everything, so the correlation middleware can
	// reuse its trace ID. Probes and scrapes would only add noise. An
	// incoming traceparent (from a caller that traces) makes this request
	// part of the caller's trace.
	return otelhttp.NewHandler(routes(o), "http",
		otelhttp.WithPropagators(propagation.TraceContext{}),
		otelhttp.WithFilter(func(r *http.Request) bool { return !quiet[r.URL.Path] }))
}

func routes(o Options) chi.Router {
	if o.Metrics == nil {
		o.Metrics = metrics.NewAPI(prometheus.NewRegistry())
	}
	h := &handlers{svc: o.Service, log: o.Log, metrics: o.Metrics, stats: o.Stats, templates: o.Templates, hub: o.Hub, wsOrigins: o.WSOrigins}
	r := chi.NewRouter()
	r.Use(correlationID, requestLogger(o.Log), observe(o.Metrics), recoverer(o.Log))

	r.Get("/healthz", health.Live)
	r.Get("/readyz", health.Ready(o.Ready...))
	if o.Gatherer != nil {
		r.Method(http.MethodGet, "/metrics", promhttp.HandlerFor(o.Gatherer, promhttp.HandlerOpts{}))
	}
	if o.Stats != nil {
		r.Get("/api/v1/stats", h.getStats)
	}

	r.Route("/api/v1/notifications", func(r chi.Router) {
		r.Post("/", h.create)
		r.Post("/batch", h.createBatch)
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Post("/{id}/cancel", h.cancel)
	})
	r.Get("/api/v1/batches/{id}", h.getBatch)
	if o.Hub != nil {
		r.Get("/api/v1/ws", h.websocket)
	}
	if o.Templates != nil {
		r.Route("/api/v1/templates", func(r chi.Router) {
			r.Post("/", h.createTemplate)
			r.Get("/", h.listTemplates)
			r.Get("/{id}", h.getTemplate)
			r.Put("/{id}", h.updateTemplate)
		})
	}
	r.Get("/openapi.yaml", serveSpec)
	r.Get("/docs", serveDocs)
	return r
}

// create answers 202 for a new notification and 200 for an idempotent replay.
func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	var body createRequest
	if !h.decode(w, r, &body, maxBodyBytes) {
		return
	}
	n, created, err := h.svc.Create(r.Context(), body.toDomain(r.Header.Get(IdempotencyHeader)), CorrelationIDFrom(r.Context()))
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	} else {
		h.metrics.Created.WithLabelValues(string(n.Channel), n.Priority.String()).Inc()
	}
	w.Header().Set("Location", "/api/v1/notifications/"+n.ID.String())
	writeJSON(w, status, toResponse(n))
}

func (h *handlers) get(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(n))
}

func (h *handlers) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(w, r)
	if !ok {
		return
	}
	n, err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err, &n)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(n))
}

func (h *handlers) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var (
		f domain.ListFilter
		v domain.ValidationError
	)
	if s := q.Get("status"); s != "" {
		st, err := domain.ParseStatus(s)
		if err != nil {
			v.Add("status", err.Error())
		}
		f.Status = &st
	}
	if s := q.Get("channel"); s != "" {
		ch, err := domain.ParseChannel(s)
		if err != nil {
			v.Add("channel", err.Error())
		}
		f.Channel = &ch
	}
	if s := q.Get("batchId"); s != "" {
		id, err := uuid.Parse(s)
		if err != nil {
			v.Add("batchId", "must be a UUID")
		}
		f.BatchID = &id
	}
	f.From = parseTime(q.Get("from"), "from", &v)
	f.To = parseTime(q.Get("to"), "to", &v)
	if s := q.Get("limit"); s != "" {
		if _, err := fmt.Sscan(s, &f.Limit); err != nil {
			v.Add("limit", "must be an integer")
		}
	}
	if s := q.Get("cursor"); s != "" {
		c, err := domain.DecodeCursor(s)
		if err != nil {
			v.Add("cursor", "is not a cursor returned by this API")
		}
		f.After = &c
	}
	if v.HasErrors() {
		h.writeError(w, r, &v, nil)
		return
	}

	page, next, err := h.svc.List(r.Context(), f)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	resp := listResponse{Items: make([]notificationResponse, len(page))}
	for i, n := range page {
		resp.Items[i] = toResponse(n)
	}
	if next != nil {
		resp.NextCursor = next.Encode()
	}
	writeJSON(w, http.StatusOK, resp)
}

// createBatch accepts valid items and reports invalid ones (202). If no item
// is accepted, nothing is stored and the answer is 400 with every reason.
func (h *handlers) createBatch(w http.ResponseWriter, r *http.Request) {
	var body batchRequest
	if !h.decode(w, r, &body, maxBatchBodyBytes) {
		return
	}
	reqs := make([]domain.CreateRequest, len(body.Items))
	for i, it := range body.Items {
		reqs[i] = it.toDomain(it.IdempotencyKey)
	}
	res, err := h.svc.CreateBatch(r.Context(), reqs, CorrelationIDFrom(r.Context()))
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}

	resp := batchResponse{Total: len(res.Items), Items: make([]batchItemResponse, len(res.Items))}
	for i, it := range res.Items {
		item := batchItemResponse{Index: it.Index}
		var v *domain.ValidationError
		switch {
		case errors.As(it.Err, &v):
			item.Error = &errorDetail{Code: "validation_failed", Message: "item is invalid", Fields: v.Fields}
		case errors.Is(it.Err, domain.ErrIdempotencyMismatch):
			item.Error = &errorDetail{Code: "idempotency_key_reused", Message: it.Err.Error()}
		default:
			item.ID = it.Notification.ID.String()
			item.Status = string(it.Notification.Status)
			item.Duplicate = !it.Created
			if it.Created {
				h.metrics.Created.WithLabelValues(string(it.Notification.Channel), it.Notification.Priority.String()).Inc()
			}
		}
		if item.Error != nil {
			resp.Rejected++
		} else {
			resp.Accepted++
		}
		resp.Items[i] = item
	}

	status := http.StatusAccepted
	if res.Batch == nil {
		status = http.StatusBadRequest
	} else {
		resp.BatchID = res.Batch.ID.String()
		w.Header().Set("Location", "/api/v1/batches/"+resp.BatchID)
	}
	writeJSON(w, status, resp)
}

func (h *handlers) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(w, r)
	if !ok {
		return
	}
	s, err := h.svc.GetBatch(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, toBatchSummary(s))
}

func parseTime(s, field string, v *domain.ValidationError) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		v.Add(field, "must be an RFC 3339 timestamp, for example 2026-11-26T10:00:00Z")
		return nil
	}
	return &t
}

func (h *handlers) pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody("invalid_request", "id must be a UUID", nil))
		return uuid.Nil, false
	}
	return id, true
}

// decode reads a JSON body strictly: size-limited, no unknown fields, one value.
func (h *handlers) decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("body must contain a single JSON object")
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorBody("body_too_large", fmt.Sprintf("body exceeds %d bytes", limit), nil))
			return false
		}
		writeJSON(w, http.StatusBadRequest, errorBody("invalid_request", "invalid JSON: "+err.Error(), nil))
		return false
	}
	return true
}

// writeError maps domain errors to HTTP. current is the notification to show
// on a conflict; unexpected errors are logged and hidden from the client.
func (h *handlers) writeError(w http.ResponseWriter, r *http.Request, err error, current *domain.Notification) {
	var v *domain.ValidationError
	switch {
	case errors.As(err, &v):
		writeJSON(w, http.StatusBadRequest, errorBody("validation_failed", "request is invalid", v.Fields))
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody("not_found", "not found", nil))
	case errors.Is(err, domain.ErrNotCancellable):
		body := errorBody("not_cancellable", "only pending notifications can be cancelled", nil)
		if current != nil {
			c := toResponse(*current)
			body.Error.Current = &c
		}
		writeJSON(w, http.StatusConflict, body)
	case errors.Is(err, domain.ErrTemplateNotFound):
		writeJSON(w, http.StatusNotFound, errorBody("not_found", "template not found", nil))
	case errors.Is(err, domain.ErrTemplateNameTaken):
		writeJSON(w, http.StatusConflict, errorBody("name_taken", err.Error(), nil))
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody("idempotency_key_reused", err.Error(), nil))
	default:
		h.log.ErrorContext(r.Context(), "request failed",
			"correlation_id", CorrelationIDFrom(r.Context()), "error", err)
		writeJSON(w, http.StatusInternalServerError, errorBody("internal", "internal server error", nil))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
