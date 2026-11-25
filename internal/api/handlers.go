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

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Service is what the handlers need. Declared here so tests can fake it if needed.
type Service interface {
	Create(ctx context.Context, req domain.CreateRequest, correlationID string) (domain.Notification, bool, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error)
}

const (
	IdempotencyHeader = "Idempotency-Key"
	maxBodyBytes      = 1 << 20 // 1 MiB: a 100 KB email plus JSON overhead fits easily
)

type handlers struct {
	svc Service
	log *slog.Logger
}

// NewRouter wires routes and middleware.
func NewRouter(svc Service, log *slog.Logger) http.Handler {
	h := &handlers{svc: svc, log: log}
	r := chi.NewRouter()
	r.Use(correlationID, requestLogger(log), recoverer(log))

	r.Route("/api/v1/notifications", func(r chi.Router) {
		r.Post("/", h.create)
		r.Get("/", h.list)
		r.Get("/{id}", h.get)
		r.Post("/{id}/cancel", h.cancel)
	})
	return r
}

// create answers 202 for a new notification and 200 for an idempotent replay.
func (h *handlers) create(w http.ResponseWriter, r *http.Request) {
	var body createRequest
	if !h.decode(w, r, &body) {
		return
	}
	n, created, err := h.svc.Create(r.Context(), domain.CreateRequest{
		Recipient:      body.Recipient,
		Channel:        body.Channel,
		Content:        body.Content,
		Priority:       body.Priority,
		ScheduledAt:    body.ScheduledAt,
		IdempotencyKey: r.Header.Get(IdempotencyHeader),
	}, CorrelationIDFrom(r.Context()))
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
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
func (h *handlers) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("body must contain a single JSON object")
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorBody("body_too_large", fmt.Sprintf("body exceeds %d bytes", maxBodyBytes), nil))
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
		writeJSON(w, http.StatusNotFound, errorBody("not_found", "notification not found", nil))
	case errors.Is(err, domain.ErrNotCancellable):
		body := errorBody("not_cancellable", "only pending notifications can be cancelled", nil)
		if current != nil {
			c := toResponse(*current)
			body.Error.Current = &c
		}
		writeJSON(w, http.StatusConflict, body)
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
