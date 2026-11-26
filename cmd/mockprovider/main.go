// Command mockprovider stands in for the external provider, so that
// `docker compose up` works without a webhook.site URL and failures can be
// staged on purpose:
//
//	MOCK_FAILURE_RATE=0.3   answer 503 to 30% of requests
//	MOCK_LATENCY=150ms      wait before answering
//	MOCK_STATUS=429         answer every request with this status
//
// It answers like the assessment's provider: 202 with
// {"messageId", "status": "accepted", "timestamp"}.
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func main() {
	failureRate, _ := strconv.ParseFloat(os.Getenv("MOCK_FAILURE_RATE"), 64)
	latency, _ := time.ParseDuration(os.Getenv("MOCK_LATENCY"))
	fixedStatus, _ := strconv.Atoi(os.Getenv("MOCK_STATUS"))
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockprovider")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ To, Channel, Content string }
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.To == "" {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		time.Sleep(latency)

		status := http.StatusAccepted
		switch {
		case fixedStatus != 0:
			status = fixedStatus
		case rand.Float64() < failureRate:
			status = http.StatusServiceUnavailable
		}
		log.Info("request", "to", body.To, "channel", body.Channel, "status", status,
			"idempotency_key", r.Header.Get("X-Idempotency-Key"), "traceparent", r.Header.Get("traceparent"))

		w.Header().Set("Content-Type", "application/json")
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "2")
		}
		w.WriteHeader(status)
		if status == http.StatusAccepted {
			_ = json.NewEncoder(w).Encode(map[string]string{
				"messageId": uuid.NewString(), "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339),
			})
		}
	})
	log.Info("mock provider listening", "addr", addr, "failure_rate", failureRate, "latency", latency.String())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("stopped", "error", err)
		os.Exit(1)
	}
}
