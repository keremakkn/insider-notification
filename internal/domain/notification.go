// Package domain holds the notification model, its state machine and its
// validation rules. It imports no other package of this service.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	}
	return "", fmt.Errorf("must be one of sms, email, push")
}

// Priority is stored as 0 (high), 1 (normal), 2 (low), so that ascending
// order is the order in which work should be done.
type Priority int16

const (
	PriorityHigh Priority = iota
	PriorityNormal
	PriorityLow
)

var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

// ParsePriority accepts "high", "normal" or "low"; empty means normal.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal", "":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	}
	return 0, fmt.Errorf("must be one of high, normal, low")
}

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("Priority(%d)", int16(p))
}

type Notification struct {
	ID             uuid.UUID
	BatchID        *uuid.UUID
	IdempotencyKey string // empty when the client sent none
	RequestHash    string // empty exactly when IdempotencyKey is empty
	CorrelationID  string
	TraceParent    string // W3C trace context of the creating request; the worker continues it

	Recipient  string
	Channel    Channel
	Content    string // for a template, the rendered text
	Priority   Priority
	TemplateID *uuid.UUID // set when Content was rendered from a template

	Status      Status
	Attempts    int
	MaxAttempts int

	NextAttemptAt  time.Time
	ScheduledAt    *time.Time
	EnqueuedAt     *time.Time
	LeaseExpiresAt *time.Time

	ProviderMessageID string
	LastError         string

	CreatedAt time.Time
	UpdatedAt time.Time
	SentAt    *time.Time
}

// CreateRequest is a notification as the client describes it, before validation.
type CreateRequest struct {
	Recipient      string
	Channel        string
	Content        string
	Priority       string
	ScheduledAt    *time.Time
	IdempotencyKey string

	// A request may name a template instead of sending content. The service
	// renders it into Content before calling NewNotification, so content
	// rules (such as the SMS length) apply to the final text.
	TemplateID *uuid.UUID
	Variables  map[string]string
}

const maxIdempotencyKeyLen = 255

// NewNotification validates req and builds a pending notification.
// It returns a *ValidationError listing every invalid field, not just the first.
func NewNotification(req CreateRequest, maxAttempts int, now time.Time) (Notification, error) {
	var v ValidationError

	channel, err := ParseChannel(req.Channel)
	if err != nil {
		v.Add("channel", err.Error())
	}
	priority, err := ParsePriority(req.Priority)
	if err != nil {
		v.Add("priority", err.Error())
	}
	if channel != "" {
		if err := ValidateRecipient(channel, req.Recipient); err != nil {
			v.Add("recipient", err.Error())
		}
		if err := ValidateContent(channel, req.Content); err != nil {
			v.Add("content", err.Error())
		}
	}
	if req.ScheduledAt != nil && !req.ScheduledAt.After(now) {
		v.Add("scheduledAt", "must be in the future")
	}
	if len(req.IdempotencyKey) > maxIdempotencyKeyLen {
		v.Add("idempotencyKey", fmt.Sprintf("must be at most %d characters", maxIdempotencyKeyLen))
	}
	if v.HasErrors() {
		return Notification{}, &v
	}

	id, err := uuid.NewV7() // time-ordered: new rows land at the end of the primary key index
	if err != nil {
		return Notification{}, fmt.Errorf("generate id: %w", err)
	}
	n := Notification{
		ID:             id,
		IdempotencyKey: req.IdempotencyKey,
		Recipient:      req.Recipient,
		Channel:        channel,
		Content:        req.Content,
		Priority:       priority,
		Status:         StatusPending,
		MaxAttempts:    maxAttempts,
		NextAttemptAt:  now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if req.ScheduledAt != nil {
		at := req.ScheduledAt.UTC()
		n.ScheduledAt = &at
		n.NextAttemptAt = at
	}
	if req.TemplateID != nil {
		tid := *req.TemplateID
		n.TemplateID = &tid
	}
	if n.IdempotencyKey != "" {
		n.RequestHash = RequestHash(n)
		if req.TemplateID != nil {
			// Hash what the client sent (template and values), not the
			// rendered text: if the template is edited and the client replays
			// the identical request, it must get its original notification
			// back, not a 422 for "different content".
			n.RequestHash = hash(n.Recipient, string(n.Channel), "template:"+req.TemplateID.String(),
				canonical(req.Variables), n.Priority.String(), scheduledString(n.ScheduledAt))
		}
	}
	return n, nil
}

func canonical(vars map[string]string) string {
	b, _ := json.Marshal(vars) // encoding/json sorts map keys
	return string(b)
}

func scheduledString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// RequestHash fingerprints the fields a client controls. The same
// idempotency key with a different hash means the key was reused for a
// different request. JSON gives an unambiguous encoding: "ab"+"c" and
// "a"+"bc" hash differently.
func RequestHash(n Notification) string {
	return hash(n.Recipient, string(n.Channel), n.Content, n.Priority.String(), scheduledString(n.ScheduledAt))
}

func hash(fields ...string) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
