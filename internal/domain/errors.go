package domain

import (
	"errors"
	"strings"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrNotCancellable: only pending notifications can be cancelled.
	ErrNotCancellable = errors.New("notification is no longer pending")
	// ErrIdempotencyMismatch: an idempotency key was reused with a different request.
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")

	errInvalidStatus = errors.New("must be one of pending, processing, sent, failed, cancelled")
)

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError collects every invalid field so the client can fix them in one go.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Add(field, message string) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: message})
}

func (e *ValidationError) HasErrors() bool { return len(e.Fields) > 0 }

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid notification: " + strings.Join(parts, "; ")
}
