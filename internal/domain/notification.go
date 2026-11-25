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

	Recipient string
	Channel   Channel
	Content   string
	Priority  Priority

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
	if n.IdempotencyKey != "" {
		n.RequestHash = RequestHash(n)
	}
	return n, nil
}

// RequestHash fingerprints the fields a client controls. The same
// idempotency key with a different hash means the key was reused for a
// different request. JSON gives an unambiguous encoding: "ab"+"c" and
// "a"+"bc" hash differently.
func RequestHash(n Notification) string {
	var scheduled string
	if n.ScheduledAt != nil {
		scheduled = n.ScheduledAt.UTC().Format(time.RFC3339Nano)
	}
	b, _ := json.Marshal([]string{n.Recipient, string(n.Channel), n.Content, n.Priority.String(), scheduled})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
