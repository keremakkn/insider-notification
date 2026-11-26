package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/service"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

type testServer struct {
	t    *testing.T
	repo *testutil.MemRepo
	srv  http.Handler
}

func newTestServer(t *testing.T) *testServer {
	repo := testutil.NewMemRepo()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &testServer{t: t, repo: repo, srv: NewRouter(Options{Service: service.NewNotifications(repo, 5), Log: log})}
}

func (s *testServer) do(method, path, body string, headers ...string) *httptest.ResponseRecorder {
	s.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	s.srv.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

const smsBody = `{"recipient":"+905551234567","channel":"sms","content":"Merhaba","priority":"high"}`

func TestCreate(t *testing.T) {
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications", smsBody, CorrelationHeader, "abc-123")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	n := decode[notificationResponse](t, rec)
	if n.Status != "pending" || n.Priority != "high" || n.CorrelationID != "abc-123" || n.NextAttemptAt == nil {
		t.Errorf("unexpected body: %+v", n)
	}
	if rec.Header().Get("Location") != "/api/v1/notifications/"+n.ID {
		t.Errorf("Location = %q", rec.Header().Get("Location"))
	}
	if rec.Header().Get(CorrelationHeader) != "abc-123" {
		t.Error("correlation ID should be echoed")
	}
}

func TestCreateIdempotency(t *testing.T) {
	s := newTestServer(t)
	first := s.do("POST", "/api/v1/notifications", smsBody, IdempotencyHeader, "order-42")
	replay := s.do("POST", "/api/v1/notifications", smsBody, IdempotencyHeader, "order-42")

	if first.Code != http.StatusAccepted || replay.Code != http.StatusOK {
		t.Fatalf("want 202 then 200, got %d then %d", first.Code, replay.Code)
	}
	if decode[notificationResponse](t, first).ID != decode[notificationResponse](t, replay).ID {
		t.Fatal("replay must return the original notification")
	}

	changed := strings.Replace(smsBody, "Merhaba", "Selam", 1)
	rec := s.do("POST", "/api/v1/notifications", changed, IdempotencyHeader, "order-42")
	if rec.Code != http.StatusUnprocessableEntity || decode[errorResponse](t, rec).Error.Code != "idempotency_key_reused" {
		t.Fatalf("same key with different content: want 422, got %d %s", rec.Code, rec.Body)
	}
}

func TestCreateValidation(t *testing.T) {
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications",
		`{"recipient":"0555","channel":"sms","content":"`+strings.Repeat("ş", 71)+`","priority":"urgent"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
	body := decode[errorResponse](t, rec)
	if body.Error.Code != "validation_failed" || len(body.Error.Fields) != 3 {
		t.Fatalf("want 3 field errors (priority, recipient, content), got %+v", body.Error.Fields)
	}
}

func TestCreateRejectsMalformedBodies(t *testing.T) {
	s := newTestServer(t)
	tests := []struct {
		name string
		body string
		code int
	}{
		{"not JSON", `{`, http.StatusBadRequest},
		{"unknown field", `{"to":"+905551234567"}`, http.StatusBadRequest},
		{"two objects", smsBody + smsBody, http.StatusBadRequest},
		{"too large", `{"content":"` + strings.Repeat("a", 2<<20) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := s.do("POST", "/api/v1/notifications", tt.body); rec.Code != tt.code {
				t.Fatalf("want %d, got %d: %s", tt.code, rec.Code, rec.Body)
			}
		})
	}
}

func TestGet(t *testing.T) {
	s := newTestServer(t)
	created := decode[notificationResponse](t, s.do("POST", "/api/v1/notifications", smsBody))

	if rec := s.do("GET", "/api/v1/notifications/"+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}
	if rec := s.do("GET", "/api/v1/notifications/"+uuid.NewString(), ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id: want 404, got %d", rec.Code)
	}
	if rec := s.do("GET", "/api/v1/notifications/not-a-uuid", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: want 400, got %d", rec.Code)
	}
}

func TestCancel(t *testing.T) {
	s := newTestServer(t)
	created := decode[notificationResponse](t, s.do("POST", "/api/v1/notifications", smsBody))
	path := "/api/v1/notifications/" + created.ID + "/cancel"

	rec := s.do("POST", path, "")
	if rec.Code != http.StatusOK || decode[notificationResponse](t, rec).Status != "cancelled" {
		t.Fatalf("cancel pending: %d %s", rec.Code, rec.Body)
	}

	rec = s.do("POST", path, "")
	body := decode[errorResponse](t, rec)
	if rec.Code != http.StatusConflict || body.Error.Current == nil || body.Error.Current.Status != "cancelled" {
		t.Fatalf("second cancel: want 409 with current status, got %d %s", rec.Code, rec.Body)
	}

	if rec := s.do("POST", "/api/v1/notifications/"+uuid.NewString()+"/cancel", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id: want 404, got %d", rec.Code)
	}
}

func TestCancelWhileProcessing(t *testing.T) {
	s := newTestServer(t)
	created := decode[notificationResponse](t, s.do("POST", "/api/v1/notifications", smsBody))
	s.repo.SetStatus(uuid.MustParse(created.ID), domain.StatusProcessing)

	rec := s.do("POST", "/api/v1/notifications/"+created.ID+"/cancel", "")
	if rec.Code != http.StatusConflict || decode[errorResponse](t, rec).Error.Current.Status != "processing" {
		t.Fatalf("want 409 showing processing, got %d %s", rec.Code, rec.Body)
	}
}

func TestListPaginationAndFilters(t *testing.T) {
	s := newTestServer(t)
	for range 5 {
		s.do("POST", "/api/v1/notifications", smsBody)
	}
	s.do("POST", "/api/v1/notifications", `{"recipient":"a@b.com","channel":"email","content":"Hi"}`)

	seen := map[string]bool{}
	path := "/api/v1/notifications?limit=4"
	for pages := 0; ; pages++ {
		page := decode[listResponse](t, s.do("GET", path, ""))
		for _, n := range page.Items {
			seen[n.ID] = true
		}
		if page.NextCursor == "" {
			if pages != 1 {
				t.Fatalf("want 2 pages, got %d", pages+1)
			}
			break
		}
		path = "/api/v1/notifications?limit=4&cursor=" + page.NextCursor
	}
	if len(seen) != 6 {
		t.Fatalf("want 6 distinct notifications, got %d", len(seen))
	}

	emails := decode[listResponse](t, s.do("GET", "/api/v1/notifications?channel=email", ""))
	if len(emails.Items) != 1 || emails.Items[0].Channel != "email" {
		t.Fatalf("channel filter: %+v", emails.Items)
	}
}

func TestListRejectsBadParameters(t *testing.T) {
	s := newTestServer(t)
	for _, q := range []string{
		"status=delivered", "channel=fax", "limit=0x", "limit=500", "from=yesterday",
		"cursor=garbage", "from=2026-11-26T10:00:00Z&to=2026-11-26T09:00:00Z",
	} {
		if rec := s.do("GET", "/api/v1/notifications?"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: want 400, got %d", q, rec.Code)
		}
	}
}

func TestCorrelationIDIsReplacedWhenUnsafe(t *testing.T) {
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications", smsBody, CorrelationHeader, "bad id\nwith newline")
	got := rec.Header().Get(CorrelationHeader)
	if _, err := uuid.Parse(got); err != nil {
		t.Fatalf("unsafe correlation ID should be replaced by a UUID, got %q", got)
	}
}

type panicService struct{ Service }

func (panicService) Get(context.Context, uuid.UUID) (domain.Notification, error) { panic("boom") }

func TestPanicBecomes500(t *testing.T) {
	var logs bytes.Buffer
	srv := NewRouter(Options{Service: panicService{}, Log: slog.New(slog.NewTextHandler(&logs, nil))})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/notifications/"+uuid.NewString(), nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rec.Code)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Error("panic should be logged")
	}
}
