package api

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func item(recipient, content, key string) string {
	return fmt.Sprintf(`{"recipient":%q,"channel":"sms","content":%q,"idempotencyKey":%q}`, recipient, content, key)
}

func batch(items ...string) string { return `{"items":[` + strings.Join(items, ",") + `]}` }

func TestBatchPartialAcceptance(t *testing.T) {
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications/batch", batch(
		item("+905551111111", "Merhaba", "k1"),
		item("0555", "Merhaba", "k2"), // invalid phone
		item("+905552222222", "Merhaba", "k3"),
	))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body)
	}
	body := decode[batchResponse](t, rec)
	if body.Total != 3 || body.Accepted != 2 || body.Rejected != 1 || body.BatchID == "" {
		t.Fatalf("counts: %+v", body)
	}
	if body.Items[1].Error == nil || body.Items[1].Error.Fields[0].Field != "recipient" || body.Items[0].ID == "" {
		t.Fatalf("items: %+v", body.Items)
	}

	// The client fixes item 1 and resends all three: 2 replays, 1 new.
	rec = s.do("POST", "/api/v1/notifications/batch", batch(
		item("+905551111111", "Merhaba", "k1"),
		item("+905553333333", "Merhaba", "k2"),
		item("+905552222222", "Merhaba", "k3"),
	))
	retry := decode[batchResponse](t, rec)
	if retry.Accepted != 3 || !retry.Items[0].Duplicate || retry.Items[1].Duplicate || !retry.Items[2].Duplicate {
		t.Fatalf("resend: want k1 and k3 as duplicates and k2 new, got %+v", retry.Items)
	}
	if retry.Items[0].ID != body.Items[0].ID {
		t.Fatal("a duplicate must point to the original notification")
	}

	summary := decode[batchSummaryResponse](t, s.do("GET", "/api/v1/batches/"+body.BatchID, ""))
	if summary.Accepted != 2 || summary.StatusCounts["pending"] != 2 || summary.StatusCounts["sent"] != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	list := decode[listResponse](t, s.do("GET", "/api/v1/notifications?batchId="+body.BatchID, ""))
	if len(list.Items) != 2 {
		t.Fatalf("list by batch: want 2, got %d", len(list.Items))
	}
}

func TestBatchKeyReusedWithDifferentContent(t *testing.T) {
	s := newTestServer(t)
	s.do("POST", "/api/v1/notifications/batch", batch(item("+905551111111", "Merhaba", "k1")))
	rec := s.do("POST", "/api/v1/notifications/batch", batch(
		item("+905551111111", "Selam", "k1"),
		item("+905552222222", "Merhaba", "k2"),
	))
	body := decode[batchResponse](t, rec)
	if rec.Code != http.StatusAccepted || body.Items[0].Error == nil || body.Items[0].Error.Code != "idempotency_key_reused" || body.Accepted != 1 {
		t.Fatalf("want item 0 rejected as reused key, got %d %+v", rec.Code, body)
	}
}

func TestBatchWithNoValidItems(t *testing.T) {
	s := newTestServer(t)
	rec := s.do("POST", "/api/v1/notifications/batch", batch(item("0555", "x", ""), item("+90", "", "")))
	body := decode[batchResponse](t, rec)
	if rec.Code != http.StatusBadRequest || body.BatchID != "" || body.Rejected != 2 {
		t.Fatalf("want 400 without a batch, got %d %+v", rec.Code, body)
	}
}

func TestBatchSizeLimits(t *testing.T) {
	s := newTestServer(t)
	if rec := s.do("POST", "/api/v1/notifications/batch", `{"items":[]}`); rec.Code != http.StatusBadRequest {
		t.Errorf("empty batch: want 400, got %d", rec.Code)
	}

	items := make([]string, 1001)
	for i := range items {
		items[i] = item("+905551234567", "x", "")
	}
	if rec := s.do("POST", "/api/v1/notifications/batch", batch(items...)); rec.Code != http.StatusBadRequest {
		t.Errorf("1001 items: want 400, got %d", rec.Code)
	}
	if rec := s.do("POST", "/api/v1/notifications/batch", batch(items[:1000]...)); rec.Code != http.StatusAccepted {
		t.Errorf("1000 items: want 202, got %d", rec.Code)
	}
}

func TestGetBatchUnknown(t *testing.T) {
	s := newTestServer(t)
	if rec := s.do("GET", "/api/v1/batches/0193a2b4-0000-7000-8000-000000000000", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
