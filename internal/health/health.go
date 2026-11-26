// Package health serves liveness and readiness probes.
//
// Liveness (/healthz) only says the process is running: restarting it
// would not fix a database outage, so it never checks dependencies.
// Readiness (/readyz) says whether the process can do useful work right now;
// while it fails, a load balancer stops sending traffic but nothing restarts.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

func Live(w http.ResponseWriter, _ *http.Request) {
	write(w, http.StatusOK, map[string]any{"status": "ok"})
}

// Ready runs every check in parallel with a short timeout.
func Ready(checks ...Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		results := make(map[string]string, len(checks))
		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)
		ready := true
		for _, c := range checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := "ok"
				if err := c.Ping(ctx); err != nil {
					res = err.Error()
				}
				mu.Lock()
				defer mu.Unlock()
				results[c.Name] = res
				if res != "ok" {
					ready = false
				}
			}()
		}
		wg.Wait()

		if !ready {
			write(w, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "checks": results})
			return
		}
		write(w, http.StatusOK, map[string]any{"status": "ready", "checks": results})
	}
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
