package api

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/keremakkn/insider-notification/internal/service"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

func newTemplateServer(t *testing.T) *testServer {
	repo, templates := testutil.NewMemRepo(), testutil.NewMemTemplates()
	srv := NewRouter(Options{
		Service:   service.NewNotifications(repo, templates, 5),
		Templates: service.NewTemplates(templates),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return &testServer{t: t, repo: repo, srv: srv}
}

func (s *testServer) createTemplate(body string) templateResponse {
	s.t.Helper()
	rec := s.do("POST", "/api/v1/templates", body)
	if rec.Code != http.StatusCreated {
		s.t.Fatalf("create template: %d %s", rec.Code, rec.Body)
	}
	return decode[templateResponse](s.t, rec)
}

const shippedTemplate = `{"name":"order_shipped","channel":"sms","body":"Merhaba {{name}}, {{order}} kargoda."}`

func TestTemplateLifecycle(t *testing.T) {
	s := newTemplateServer(t)
	tpl := s.createTemplate(shippedTemplate)
	if strings.Join(tpl.Variables, ",") != "name,order" {
		t.Fatalf("variables: %v", tpl.Variables)
	}
	if rec := s.do("POST", "/api/v1/templates", shippedTemplate); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate name: want 409, got %d", rec.Code)
	}
	if rec := s.do("POST", "/api/v1/templates", `{"name":"bad","channel":"sms","body":"Hi {{name"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed placeholder: want 400, got %d", rec.Code)
	}
	if got := decode[map[string][]templateResponse](t, s.do("GET", "/api/v1/templates", ""))["items"]; len(got) != 1 {
		t.Fatalf("list: %v", got)
	}
}

func TestNotificationFromTemplate(t *testing.T) {
	s := newTemplateServer(t)
	tpl := s.createTemplate(shippedTemplate)
	req := `{"recipient":"+905551234567","channel":"sms","templateId":"` + tpl.ID + `","variables":{"name":"Ayşe","order":"A1"}}`

	rec := s.do("POST", "/api/v1/notifications", req, IdempotencyHeader, "ship-A1")
	n := decode[notificationResponse](t, rec)
	if rec.Code != http.StatusAccepted || n.Content != "Merhaba Ayşe, A1 kargoda." || n.TemplateID != tpl.ID {
		t.Fatalf("got %d %+v", rec.Code, n)
	}

	// Editing the template does not touch the notification already created...
	if rec := s.do("PUT", "/api/v1/templates/"+tpl.ID, `{"body":"Selam {{name}}! {{order}} yolda."}`); rec.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rec.Code, rec.Body)
	}
	if got := decode[notificationResponse](t, s.do("GET", "/api/v1/notifications/"+n.ID, "")); got.Content != n.Content {
		t.Fatalf("existing notification changed to %q", got.Content)
	}
	// ...and replaying the identical request returns the original, not a 422.
	replay := s.do("POST", "/api/v1/notifications", req, IdempotencyHeader, "ship-A1")
	if replay.Code != http.StatusOK || decode[notificationResponse](t, replay).ID != n.ID {
		t.Fatalf("replay after edit: want 200 with the original, got %d %s", replay.Code, replay.Body)
	}
	// A new request uses the new text.
	fresh := decode[notificationResponse](t, s.do("POST", "/api/v1/notifications", req))
	if fresh.Content != "Selam Ayşe! A1 yolda." {
		t.Fatalf("new notification: %q", fresh.Content)
	}
}

func TestTemplateRequestErrors(t *testing.T) {
	s := newTemplateServer(t)
	tpl := s.createTemplate(shippedTemplate)
	long := strings.Repeat("ş", 70)
	tests := []struct {
		name, body, field string
	}{
		{"missing value", `{"recipient":"+905551234567","channel":"sms","templateId":"` + tpl.ID + `","variables":{"name":"Ayşe"}}`, "variables"},
		{"wrong channel", `{"recipient":"a@b.com","channel":"email","templateId":"` + tpl.ID + `","variables":{"name":"A","order":"1"}}`, "templateId"},
		{"content and template", `{"recipient":"+905551234567","channel":"sms","content":"x","templateId":"` + tpl.ID + `"}`, "content"},
		{"variables without template", `{"recipient":"+905551234567","channel":"sms","content":"x","variables":{"a":"b"}}`, "variables"},
		{"unknown template", `{"recipient":"+905551234567","channel":"sms","templateId":"0193a2b4-0000-7000-8000-000000000000"}`, "templateId"},
		// The SMS limit applies to the rendered text.
		{"too long once rendered", `{"recipient":"+905551234567","channel":"sms","templateId":"` + tpl.ID + `","variables":{"name":"` + long + `","order":"1"}}`, "content"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := s.do("POST", "/api/v1/notifications", tt.body)
			body := decode[errorResponse](t, rec)
			if rec.Code != http.StatusBadRequest || len(body.Error.Fields) == 0 || body.Error.Fields[0].Field != tt.field {
				t.Fatalf("want 400 on %s, got %d %s", tt.field, rec.Code, rec.Body)
			}
		})
	}
}

func TestBatchWithTemplate(t *testing.T) {
	s := newTemplateServer(t)
	tpl := s.createTemplate(shippedTemplate)
	item := func(name string) string {
		return `{"recipient":"+905551234567","channel":"sms","templateId":"` + tpl.ID + `","variables":{"name":"` + name + `","order":"A1"}}`
	}
	rec := s.do("POST", "/api/v1/notifications/batch", batch(item("Ayşe"), item("Mehmet"), `{"recipient":"+905551234567","channel":"sms","templateId":"`+tpl.ID+`"}`))
	body := decode[batchResponse](t, rec)
	if rec.Code != http.StatusAccepted || body.Accepted != 2 || body.Items[2].Error == nil {
		t.Fatalf("want 2 rendered and 1 rejected for missing values: %d %+v", rec.Code, body)
	}
	got := decode[notificationResponse](t, s.do("GET", "/api/v1/notifications/"+body.Items[1].ID, ""))
	if got.Content != "Merhaba Mehmet, A1 kargoda." {
		t.Fatalf("batch item content: %q", got.Content)
	}
}
