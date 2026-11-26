package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/realtime"
	"github.com/keremakkn/insider-notification/internal/service"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

type wsMessage struct {
	Type         string                `json:"type"`
	Notification *notificationResponse `json:"notification"`
	Batch        *batchSummaryResponse `json:"batch"`
	Change       *domain.StatusChange  `json:"change"`
}

func wsServer(t *testing.T) (*httptest.Server, *realtime.Hub, *testutil.MemRepo) {
	repo, hub := testutil.NewMemRepo(), realtime.NewHub()
	srv := httptest.NewServer(NewRouter(Options{
		Service: service.NewNotifications(repo, testutil.NewMemTemplates(), 5),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Hub:     hub,
	}))
	t.Cleanup(srv.Close)
	return srv, hub, repo
}

func read(t *testing.T, c *websocket.Conn) wsMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m wsMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return m
}

func TestWebSocketSnapshotThenChanges(t *testing.T) {
	srv, hub, _ := wsServer(t)
	resp, _ := http.Post(srv.URL+"/api/v1/notifications", "application/json", strings.NewReader(smsBody))
	var n notificationResponse
	_ = json.NewDecoder(resp.Body).Decode(&n)
	resp.Body.Close()

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/v1/ws?notificationId="+n.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()

	if m := read(t, c); m.Type != "snapshot" || m.Notification == nil || m.Notification.Status != "pending" {
		t.Fatalf("first message should be the current state: %+v", m)
	}

	id := uuid.MustParse(n.ID)
	for _, st := range []domain.Status{domain.StatusProcessing, domain.StatusSent} {
		ch := domain.StatusChange{ID: id, Status: st, Attempts: 1}
		b, _ := json.Marshal(ch)
		hub.Deliver(b, ch)
	}
	other := domain.StatusChange{ID: uuid.New(), Status: domain.StatusSent}
	ob, _ := json.Marshal(other)
	hub.Deliver(ob, other)

	if m := read(t, c); m.Type != "status" || m.Change.Status != domain.StatusProcessing {
		t.Fatalf("want processing, got %+v", m)
	}
	if m := read(t, c); m.Change.Status != domain.StatusSent || m.Change.ID != id {
		t.Fatalf("want sent for our notification, got %+v", m.Change)
	}
}

func TestWebSocketBatchSnapshot(t *testing.T) {
	srv, _, _ := wsServer(t)
	resp, _ := http.Post(srv.URL+"/api/v1/notifications/batch", "application/json",
		strings.NewReader(batch(item("+905551111111", "a", ""), item("+905552222222", "b", ""))))
	var b batchResponse
	_ = json.NewDecoder(resp.Body).Decode(&b)
	resp.Body.Close()

	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/v1/ws?batchId="+b.BatchID, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if m := read(t, c); m.Batch == nil || m.Batch.StatusCounts["pending"] != 2 {
		t.Fatalf("batch snapshot: %+v", m)
	}
}

func TestWebSocketRejectsBadTargetsBeforeUpgrading(t *testing.T) {
	srv, hub, _ := wsServer(t)
	for path, code := range map[string]int{
		"/api/v1/ws":                                    http.StatusBadRequest,
		"/api/v1/ws?notificationId=nope":                http.StatusBadRequest,
		"/api/v1/ws?notificationId=" + uuid.NewString(): http.StatusNotFound,
	} {
		_, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+path, nil)
		if err == nil || resp == nil || resp.StatusCode != code {
			t.Errorf("%s: want HTTP %d, got %v", path, code, resp)
		}
	}
	if hub.Count() != 0 {
		t.Fatalf("failed requests must not leave subscriptions behind: %d", hub.Count())
	}
}

func TestWebSocketRejectsForeignBrowserOrigins(t *testing.T) {
	srv, _, repo := wsServer(t)
	n, _ := domain.NewNotification(domain.CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: "x"}, 5, time.Now())
	_, _, _ = repo.Create(context.Background(), n)

	h := http.Header{"Origin": []string{"https://evil.example"}}
	_, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http")+"/api/v1/ws?notificationId="+n.ID.String(),
		&websocket.DialOptions{HTTPHeader: h})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a page on another site must not open the stream: %v", resp)
	}
}
