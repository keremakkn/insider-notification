package api

import (
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

type createRequest struct {
	Recipient   string     `json:"recipient"`
	Channel     string     `json:"channel"`
	Content     string     `json:"content"`
	Priority    string     `json:"priority"`
	ScheduledAt *time.Time `json:"scheduledAt"`
}

type notificationResponse struct {
	ID                string     `json:"id"`
	BatchID           string     `json:"batchId,omitempty"`
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
