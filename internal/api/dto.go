package api

import (
	"time"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type createRequest struct {
	Recipient   string            `json:"recipient"`
	Channel     string            `json:"channel"`
	Content     string            `json:"content"`
	Priority    string            `json:"priority"`
	ScheduledAt *time.Time        `json:"scheduledAt"`
	TemplateID  *uuid.UUID        `json:"templateId"`
	Variables   map[string]string `json:"variables"`
}

func (c createRequest) toDomain(idempotencyKey string) domain.CreateRequest {
	return domain.CreateRequest{
		Recipient: c.Recipient, Channel: c.Channel, Content: c.Content, Priority: c.Priority,
		ScheduledAt: c.ScheduledAt, IdempotencyKey: idempotencyKey,
		TemplateID: c.TemplateID, Variables: c.Variables,
	}
}

type notificationResponse struct {
	ID                string     `json:"id"`
	BatchID           string     `json:"batchId,omitempty"`
	TemplateID        string     `json:"templateId,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           string     `json:"channel"`
	Content           string     `json:"content"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"maxAttempts"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	NextAttemptAt     *time.Time `json:"nextAttemptAt,omitempty"`
	ProviderMessageID string     `json:"providerMessageId,omitempty"`
	LastError         string     `json:"lastError,omitempty"`
	CorrelationID     string     `json:"correlationId"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	SentAt            *time.Time `json:"sentAt,omitempty"`
}

func toResponse(n domain.Notification) notificationResponse {
	r := notificationResponse{
		ID:                n.ID.String(),
		Recipient:         n.Recipient,
		Channel:           string(n.Channel),
		Content:           n.Content,
		Priority:          n.Priority.String(),
		Status:            string(n.Status),
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		ScheduledAt:       n.ScheduledAt,
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CorrelationID:     n.CorrelationID,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
		SentAt:            n.SentAt,
	}
	if n.BatchID != nil {
		r.BatchID = n.BatchID.String()
	}
	if n.TemplateID != nil {
		r.TemplateID = n.TemplateID.String()
	}
	// Only meaningful while the notification can still be attempted.
	if n.Status == domain.StatusPending {
		at := n.NextAttemptAt
		r.NextAttemptAt = &at
	}
	return r
}

type listResponse struct {
	Items      []notificationResponse `json:"items"`
	NextCursor string                 `json:"nextCursor,omitempty"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Fields  []domain.FieldError `json:"fields,omitempty"`
	// Current is set on a 409, so the client sees why the request conflicted.
	Current *notificationResponse `json:"current,omitempty"`
}

func errorBody(code, message string, fields []domain.FieldError) errorResponse {
	return errorResponse{Error: errorDetail{Code: code, Message: message, Fields: fields}}
}

type batchRequest struct {
	Items []batchItemRequest `json:"items"`
}

type batchItemRequest struct {
	createRequest
	IdempotencyKey string `json:"idempotencyKey"`
}

type batchResponse struct {
	BatchID  string              `json:"batchId,omitempty"`
	Total    int                 `json:"total"`
	Accepted int                 `json:"accepted"`
	Rejected int                 `json:"rejected"`
	Items    []batchItemResponse `json:"items"`
}

type batchItemResponse struct {
	Index     int          `json:"index"`
	ID        string       `json:"id,omitempty"`
	Status    string       `json:"status,omitempty"`
	Duplicate bool         `json:"duplicate,omitempty"` // replay of an earlier request with the same key
	Error     *errorDetail `json:"error,omitempty"`
}

type batchSummaryResponse struct {
	ID           string         `json:"id"`
	Total        int            `json:"total"`
	Accepted     int            `json:"accepted"`
	Rejected     int            `json:"rejected"`
	StatusCounts map[string]int `json:"statusCounts"`
	CreatedAt    time.Time      `json:"createdAt"`
}

func toBatchSummary(s domain.BatchSummary) batchSummaryResponse {
	counts := map[string]int{}
	// Always list every status, so clients do not have to treat a missing key as zero.
	for _, st := range []domain.Status{domain.StatusPending, domain.StatusProcessing, domain.StatusSent, domain.StatusFailed, domain.StatusCancelled} {
		counts[string(st)] = s.StatusCounts[st]
	}
	return batchSummaryResponse{
		ID: s.ID.String(), Total: s.Total, Accepted: s.Accepted, Rejected: s.Rejected,
		StatusCounts: counts, CreatedAt: s.CreatedAt,
	}
}

type templateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

type templateUpdateRequest struct {
	Body string `json:"body"`
}

type templateResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func toTemplateResponse(t domain.Template) templateResponse {
	vars := t.Variables()
	if vars == nil {
		vars = []string{}
	}
	return templateResponse{ID: t.ID.String(), Name: t.Name, Channel: string(t.Channel), Body: t.Body,
		Variables: vars, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
}
