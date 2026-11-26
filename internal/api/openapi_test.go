package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"

	apispec "github.com/keremakkn/insider-notification/api"
	"github.com/keremakkn/insider-notification/internal/realtime"
	"github.com/keremakkn/insider-notification/internal/service"
	"github.com/keremakkn/insider-notification/internal/testutil"
)

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromData(apispec.OpenAPI)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("api/openapi.yaml is not valid OpenAPI 3: %v", err)
	}
	return doc
}

// Every route the router serves must be documented, and every documented
// operation must exist, so the spec cannot drift from the code.
func TestSpecMatchesRouter(t *testing.T) {
	doc := loadSpec(t)
	templates := testutil.NewMemTemplates()
	reg := prometheus.NewRegistry()
	r := routes(Options{
		Service:   service.NewNotifications(testutil.NewMemRepo(), templates, 5),
		Templates: service.NewTemplates(templates),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Gatherer:  reg,
		Stats:     fakeStats{},
		Hub:       realtime.NewHub(),
	})

	served := map[string]bool{}
	err := chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route != "/" {
			route = strings.TrimSuffix(route, "/") // chi writes sub-router roots with a trailing slash
		}
		served[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	documented := map[string]bool{}
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			documented[method+" "+path] = true
		}
	}

	var missing, stale []string
	for op := range served {
		if !documented[op] {
			missing = append(missing, op)
		}
	}
	for op := range documented {
		if !served[op] {
			stale = append(stale, op)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("served but not in api/openapi.yaml: %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("in api/openapi.yaml but not served: %v", stale)
	}
	if len(missing)+len(stale) == 0 {
		t.Logf("%d operations, all documented", len(served))
	}
}

func TestSpecAndDocsAreServed(t *testing.T) {
	s := newTestServer(t)
	if rec := s.do("GET", "/openapi.yaml", ""); rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), "openapi: 3") {
		t.Fatalf("/openapi.yaml: %d", rec.Code)
	}
	if rec := s.do("GET", "/docs", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "swagger-ui") {
		t.Fatalf("/docs: %d", rec.Code)
	}
}
