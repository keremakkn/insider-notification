package api

import (
	"net/http"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

const (
	defaultStatsWindow = 5 * time.Minute
	maxStatsWindow     = 24 * time.Hour
)

type statsResponse struct {
	Window   string                          `json:"window"`
	Channels map[string]channelStatsResponse `json:"channels"`
}

type channelStatsResponse struct {
	QueueDepth map[string]int `json:"queueDepth"`
	Pending    int            `json:"pending"`
	NotYetDue  int            `json:"notYetDue"`
	Processing int            `json:"processing"`

	Sent        int      `json:"sent"`
	Failed      int      `json:"failed"`
	Attempts    int      `json:"attempts"`
	SuccessRate *float64 `json:"successRate"` // share of provider calls that succeeded; null without calls

	ProviderLatencyMs latency `json:"providerLatencyMs"`
	EndToEndMs        latency `json:"endToEndLatencyMs"`
}

type latency struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
}

// getStats is the human-readable metrics view: what is waiting now, and
// how delivery went over the window (?window=15m, default 5m).
func (h *handlers) getStats(w http.ResponseWriter, r *http.Request) {
	window := defaultStatsWindow
	if s := r.URL.Query().Get("window"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 || d > maxStatsWindow {
			writeJSON(w, http.StatusBadRequest, errorBody("invalid_request", "window must be a duration up to 24h, for example 15m", nil))
			return
		}
		window = d
	}
	s, err := h.stats.Stats(r.Context(), window)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}

	resp := statsResponse{Window: window.String(), Channels: map[string]channelStatsResponse{}}
	for _, c := range domain.Channels {
		cs := s.Channels[c]
		if cs == nil {
			cs = &domain.ChannelStats{}
		}
		depth := map[string]int{}
		for _, p := range domain.Priorities {
			depth[p.String()] = cs.QueueDepth[p]
		}
		resp.Channels[string(c)] = channelStatsResponse{
			QueueDepth: depth, Pending: cs.Pending, NotYetDue: cs.NotYetDue, Processing: cs.Processing,
			Sent: cs.Sent, Failed: cs.Failed, Attempts: cs.AttemptsOK + cs.AttemptsFailed, SuccessRate: cs.SuccessRate(),
			ProviderLatencyMs: latency{cs.ProviderP50.Milliseconds(), cs.ProviderP95.Milliseconds()},
			EndToEndMs:        latency{cs.EndToEndP50.Milliseconds(), cs.EndToEndP95.Milliseconds()},
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
