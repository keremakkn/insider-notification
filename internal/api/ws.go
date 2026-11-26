package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
)

const (
	maxWSSubscriptions = 50
	wsWriteTimeout     = 5 * time.Second
	wsPingInterval     = 30 * time.Second
)

// websocket streams status changes for the notifications and batches named
// in the query: /api/v1/ws?notificationId=…&batchId=… (each may repeat).
//
// The client first gets a snapshot of each one's current state, then every
// change. Subscribing happens before the snapshot is read: the other way
// round, a change landing between the read and the subscription would be
// lost. Changes that arrive meanwhile wait in the subscription's buffer.
func (h *handlers) websocket(w http.ResponseWriter, r *http.Request) {
	nids, bids, ok := h.wsTargets(w, r)
	if !ok {
		return
	}
	sub := h.hub.Subscribe(nids, bids)
	defer sub.Close()

	var snapshots [][]byte
	for _, id := range nids {
		n, err := h.svc.Get(r.Context(), id)
		if err != nil {
			h.writeError(w, r, err, nil)
			return
		}
		snapshots = append(snapshots, mustJSON(map[string]any{"type": "snapshot", "notification": toResponse(n)}))
	}
	for _, id := range bids {
		s, err := h.svc.GetBatch(r.Context(), id)
		if err != nil {
			h.writeError(w, r, err, nil)
			return
		}
		snapshots = append(snapshots, mustJSON(map[string]any{"type": "snapshot", "batch": toBatchSummary(s)}))
	}

	// The HTTP server's read and write timeouts are meant for short
	// requests; a WebSocket lives as long as the client wants.
	rc := http.NewResponseController(w) //nolint:bodyclose // controls the writer; there is no response body here
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.wsOrigins})
	if err != nil {
		return // Accept has answered the client
	}
	defer conn.CloseNow()
	ctx := conn.CloseRead(r.Context()) // clients only listen; this notices when they leave

	write := func(b []byte) error {
		wctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}
	for _, s := range snapshots {
		if write(s) != nil {
			return
		}
	}

	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Dropped:
			conn.Close(websocket.StatusPolicyViolation, "client too slow; reconnect and read the current state with GET")
			return
		case raw := <-sub.C:
			if write(append(append([]byte(`{"type":"status","change":`), raw...), '}')) != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *handlers) wsTargets(w http.ResponseWriter, r *http.Request) (nids, bids []uuid.UUID, ok bool) {
	q := r.URL.Query()
	parse := func(vals []string) ([]uuid.UUID, error) {
		out := make([]uuid.UUID, 0, len(vals))
		for _, v := range vals {
			id, err := uuid.Parse(v)
			if err != nil {
				return nil, errors.New("ids must be UUIDs")
			}
			out = append(out, id)
		}
		return out, nil
	}
	var err error
	if nids, err = parse(q["notificationId"]); err == nil {
		bids, err = parse(q["batchId"])
	}
	if err == nil {
		if n := len(nids) + len(bids); n == 0 || n > maxWSSubscriptions {
			err = errors.New("give between 1 and 50 notificationId or batchId parameters")
		}
	}
	if err != nil {
		var v domain.ValidationError
		v.Add("query", err.Error())
		h.writeError(w, r, &v, nil)
		return nil, nil, false
	}
	return nids, bids, true
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
