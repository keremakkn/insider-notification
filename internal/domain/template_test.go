package domain

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNewTemplateValidation(t *testing.T) {
	tests := []struct {
		name, channel, body string
		badField            string
	}{
		{"order_shipped", "sms", "Hi {{name}}", ""},
		{"Order Shipped", "sms", "Hi", "name"},
		{"ok", "fax", "Hi", "channel"},
		{"ok", "sms", "  ", "body"},
		{"ok", "sms", "Hi {{ 1name }}", "body"}, // a placeholder cannot start with a digit
		{"ok", "sms", "Hi {{name", "body"},      // unclosed
	}
	for _, tt := range tests {
		_, err := NewTemplate(tt.name, tt.channel, tt.body, now)
		var v *ValidationError
		switch {
		case tt.badField == "" && err != nil:
			t.Errorf("%q: unexpected error %v", tt.body, err)
		case tt.badField != "" && (!errors.As(err, &v) || v.Fields[0].Field != tt.badField):
			t.Errorf("%q/%q/%q: want error on %s, got %v", tt.name, tt.channel, tt.body, tt.badField, err)
		}
	}
}

func TestRender(t *testing.T) {
	tpl, _ := NewTemplate("x", "sms", "Merhaba {{name}}, {{ order }} kargoda. Tekrar: {{name}}", now)
	if got := strings.Join(tpl.Variables(), ","); got != "name,order" {
		t.Fatalf("Variables = %s", got)
	}
	out, err := tpl.Render(map[string]string{"name": "Ayşe", "order": "A1", "unused": "ignored"})
	if err != nil || out != "Merhaba Ayşe, A1 kargoda. Tekrar: Ayşe" {
		t.Fatalf("Render = %q, %v", out, err)
	}
	if _, err := tpl.Render(map[string]string{"name": "Ayşe"}); err == nil || !strings.Contains(err.Error(), "order") {
		t.Fatalf("a missing value must be named, got %v", err)
	}
	// Values are inserted as text, never interpreted: a value that looks
	// like a placeholder stays as it is.
	out, _ = tpl.Render(map[string]string{"name": "{{order}}", "order": "A1"})
	if !strings.HasPrefix(out, "Merhaba {{order}}, A1") {
		t.Fatalf("values must not be expanded again: %q", out)
	}
}

func TestTemplateRequestHashIgnoresRenderedText(t *testing.T) {
	tid := uuid.New()
	req := CreateRequest{Recipient: "+905551234567", Channel: "sms", Priority: "high", IdempotencyKey: "k",
		TemplateID: &tid, Variables: map[string]string{"name": "Ayşe", "order": "A1"}}

	req.Content = "rendered with the old body"
	a, _ := NewNotification(req, 5, now)
	req.Content = "rendered after the template was edited"
	b, _ := NewNotification(req, 5, now)
	if a.RequestHash != b.RequestHash {
		t.Fatal("the same template request must hash the same even if the template changed")
	}
	req.Variables = map[string]string{"name": "Mehmet", "order": "A1"}
	c, _ := NewNotification(req, 5, now)
	if c.RequestHash == a.RequestHash {
		t.Fatal("different values are a different request")
	}
	if a.TemplateID == nil || *a.TemplateID != tid {
		t.Fatal("the notification should remember its template")
	}
}
