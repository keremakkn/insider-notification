package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

func TestMeasureSMS(t *testing.T) {
	tests := []struct {
		name    string
		content string
		enc     SMSEncoding
		length  int
	}{
		{"plain ASCII", "Your code is 1234", EncodingGSM7, 17},
		{"Turkish letters that are in GSM-7", "Ürün Özel Ç", EncodingGSM7, 11},
		{"ş forces UCS-2", "Siparişiniz", EncodingUCS2, 11},
		{"dotless ı forces UCS-2", "Kargo yolda, kapıda ödeme", EncodingUCS2, 25},
		{"lowercase ç is not in GSM-7", "çay", EncodingUCS2, 3},
		{"extension characters take two slots", "Total: 10€ {x}", EncodingGSM7, 17},
		{"emoji takes two UTF-16 units", "Hi 🎉", EncodingUCS2, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MeasureSMS(tt.content)
			if got.Encoding != tt.enc || got.Length != tt.length {
				t.Fatalf("MeasureSMS(%q) = %s/%d, want %s/%d", tt.content, got.Encoding, got.Length, tt.enc, tt.length)
			}
		})
	}
}

func TestSMSLimits(t *testing.T) {
	tests := []struct {
		name    string
		content string
		ok      bool
	}{
		{"160 GSM-7 characters fit", strings.Repeat("a", 160), true},
		{"161 GSM-7 characters do not", strings.Repeat("a", 161), false},
		{"80 extension characters fill 160 slots", strings.Repeat("€", 80), true},
		{"81 extension characters do not fit", strings.Repeat("€", 81), false},
		{"70 characters with ş fit", "ş" + strings.Repeat("a", 69), true},
		{"71 characters with ş do not", "ş" + strings.Repeat("a", 70), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateContent(ChannelSMS, tt.content)
			if (err == nil) != tt.ok {
				t.Fatalf("ValidateContent ok=%v, err=%v", tt.ok, err)
			}
		})
	}
}

func TestValidateRecipient(t *testing.T) {
	tests := []struct {
		channel   Channel
		recipient string
		ok        bool
	}{
		{ChannelSMS, "+905551234567", true},
		{ChannelSMS, "05551234567", false},       // no country code
		{ChannelSMS, "+90 555 123", false},       // spaces
		{ChannelSMS, "+0905551234567", false},    // country code cannot start with 0
		{ChannelSMS, "+1234567890123456", false}, // 16 digits
		{ChannelEmail, "ayse@example.com", true},
		{ChannelEmail, "Ayşe <ayse@example.com>", false},
		{ChannelEmail, "not-an-email", false},
		{ChannelPush, "fcm:APA91bH_device-token", true},
		{ChannelPush, "has space", false},
		{ChannelPush, strings.Repeat("x", 4097), false},
		{ChannelSMS, "  ", false},
	}
	for _, tt := range tests {
		err := ValidateRecipient(tt.channel, tt.recipient)
		if (err == nil) != tt.ok {
			t.Errorf("ValidateRecipient(%s, %.30q): ok=%v, err=%v", tt.channel, tt.recipient, tt.ok, err)
		}
	}
}

func TestValidateContentSizes(t *testing.T) {
	if err := ValidateContent(ChannelEmail, strings.Repeat("a", 100*1024)); err != nil {
		t.Errorf("100 KB email should pass: %v", err)
	}
	if err := ValidateContent(ChannelEmail, strings.Repeat("a", 100*1024+1)); err == nil {
		t.Error("email over 100 KB should fail")
	}
	if err := ValidateContent(ChannelPush, strings.Repeat("a", 4*1024+1)); err == nil {
		t.Error("push over 4 KB should fail")
	}
	if err := ValidateContent(ChannelPush, " \n "); err == nil {
		t.Error("blank content should fail")
	}
}

func TestNewNotification(t *testing.T) {
	n, err := NewNotification(CreateRequest{
		Recipient: "+905551234567", Channel: "sms", Content: "Merhaba", Priority: "high", IdempotencyKey: "k1",
	}, 5, now)
	if err != nil {
		t.Fatal(err)
	}
	if n.Status != StatusPending || n.Priority != PriorityHigh || n.MaxAttempts != 5 {
		t.Errorf("unexpected fields: %+v", n)
	}
	if !n.NextAttemptAt.Equal(now) {
		t.Errorf("NextAttemptAt = %v, want now", n.NextAttemptAt)
	}
	if n.ID.Version() != 7 {
		t.Errorf("want a UUIDv7, got version %d", n.ID.Version())
	}
	if n.RequestHash == "" {
		t.Error("a request with an idempotency key needs a hash")
	}
}

func TestNewNotificationDefaultsAndScheduling(t *testing.T) {
	at := now.Add(time.Hour)
	n, err := NewNotification(CreateRequest{
		Recipient: "a@b.com", Channel: "email", Content: "Hi", ScheduledAt: &at,
	}, 5, now)
	if err != nil {
		t.Fatal(err)
	}
	if n.Priority != PriorityNormal {
		t.Errorf("empty priority should default to normal, got %s", n.Priority)
	}
	if !n.NextAttemptAt.Equal(at) || n.ScheduledAt == nil || !n.ScheduledAt.Equal(at) {
		t.Errorf("scheduled notification should first be attempted at %v, got %v", at, n.NextAttemptAt)
	}
	if n.RequestHash != "" {
		t.Error("no idempotency key, so no hash")
	}
}

func TestNewNotificationReportsEveryInvalidField(t *testing.T) {
	past := now.Add(-time.Minute)
	_, err := NewNotification(CreateRequest{
		Recipient: "05551234567", Channel: "sms", Content: "", Priority: "urgent", ScheduledAt: &past,
	}, 5, now)

	var v *ValidationError
	if !errors.As(err, &v) {
		t.Fatalf("want *ValidationError, got %v", err)
	}
	got := map[string]bool{}
	for _, f := range v.Fields {
		got[f.Field] = true
	}
	for _, field := range []string{"priority", "recipient", "content", "scheduledAt"} {
		if !got[field] {
			t.Errorf("missing error for %s; got %v", field, v.Fields)
		}
	}
}

func TestUnknownChannel(t *testing.T) {
	_, err := NewNotification(CreateRequest{Recipient: "x", Channel: "fax", Content: "hi"}, 5, now)
	var v *ValidationError
	if !errors.As(err, &v) || v.Fields[0].Field != "channel" {
		t.Fatalf("want channel error, got %v", err)
	}
}

func TestRequestHash(t *testing.T) {
	base := Notification{Recipient: "+905551234567", Channel: ChannelSMS, Content: "ab", Priority: PriorityNormal}

	same := base
	if RequestHash(base) != RequestHash(same) {
		t.Error("identical requests must hash the same")
	}
	changed := base
	changed.Content = "abc"
	if RequestHash(base) == RequestHash(changed) {
		t.Error("different content must hash differently")
	}
	// Field boundaries must not blur: recipient "x" + content "ab" vs recipient "xa" + content "b".
	a := Notification{Recipient: "x", Content: "ab"}
	b := Notification{Recipient: "xa", Content: "b"}
	if RequestHash(a) == RequestHash(b) {
		t.Error("hash must not be ambiguous across field boundaries")
	}
}

func TestStateMachine(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusProcessing}: true,
		{StatusPending, StatusCancelled}:  true,
		{StatusProcessing, StatusSent}:    true,
		{StatusProcessing, StatusPending}: true,
		{StatusProcessing, StatusFailed}:  true,
	}
	all := []Status{StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled}
	for _, from := range all {
		for _, to := range all {
			if got := from.CanTransitionTo(to); got != allowed[[2]Status{from, to}] {
				t.Errorf("%s -> %s: got %v", from, to, got)
			}
		}
	}
	for _, s := range all {
		final := s == StatusSent || s == StatusFailed || s == StatusCancelled
		if s.IsFinal() != final {
			t.Errorf("%s.IsFinal() = %v", s, s.IsFinal())
		}
	}
}

func TestParsers(t *testing.T) {
	if _, err := ParsePriority("urgent"); err == nil {
		t.Error("unknown priority should fail")
	}
	if _, err := ParseStatus("delivered"); err == nil {
		t.Error("delivered is not a status; we use sent")
	}
	for _, p := range Priorities {
		got, err := ParsePriority(p.String())
		if err != nil || got != p {
			t.Errorf("round trip %s failed", p)
		}
	}
}
