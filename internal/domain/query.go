package domain

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ListFilter selects notifications for listing. Nil fields do not filter.
type ListFilter struct {
	Status  *Status
	Channel *Channel
	From    *time.Time // inclusive, on created_at
	To      *time.Time // exclusive, on created_at
	After   *Cursor    // continue after this position
	Limit   int
}

// Cursor is a keyset position: the (created_at, id) of the last row on the
// previous page. Unlike an offset, it stays correct while rows are inserted.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"i"`
}

// Encode returns an opaque token, so clients do not build cursors themselves.
func (c Cursor) Encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

var ErrInvalidCursor = errors.New("invalid cursor")

func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil || c.ID == uuid.Nil || c.CreatedAt.IsZero() {
		return Cursor{}, ErrInvalidCursor
	}
	return c, nil
}
