package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

// Stats fills everything except queue depths (those live in Redis). Each
// query is served by an index: the partial pending index, and the two
// time indexes from migration 000002.
func (r *Notifications) Stats(ctx context.Context, window time.Duration) (domain.Stats, error) {
	s := domain.Stats{Window: window, Channels: map[domain.Channel]*domain.ChannelStats{}}
	for _, c := range domain.Channels {
		s.Channels[c] = &domain.ChannelStats{QueueDepth: map[domain.Priority]int{}}
	}
	secs := window.Seconds()

	rows, err := r.db.Query(ctx, `
		SELECT channel, status, count(*), count(*) FILTER (WHERE next_attempt_at > now())
		FROM notifications
		WHERE status IN ('pending', 'processing') GROUP BY channel, status`)
	if err != nil {
		return s, fmt.Errorf("stats: current: %w", err)
	}
	for rows.Next() {
		var (
			ch, st    string
			n, notDue int
		)
		if err := rows.Scan(&ch, &st, &n, &notDue); err != nil {
			rows.Close()
			return s, err
		}
		if cs := s.Channels[domain.Channel(ch)]; cs != nil {
			if st == string(domain.StatusPending) {
				cs.Pending, cs.NotYetDue = n, notDue
			} else {
				cs.Processing = n
			}
		}
	}
	rows.Close()

	rows, err = r.db.Query(ctx, `
		SELECT n.channel,
		       count(*) FILTER (WHERE a.http_status BETWEEN 200 AND 299),
		       count(*) FILTER (WHERE a.http_status IS NULL OR a.http_status NOT BETWEEN 200 AND 299),
		       coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY a.duration_ms), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms), 0)
		FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
		WHERE a.created_at > now() - make_interval(secs => $1)
		GROUP BY n.channel`, secs)
	if err != nil {
		return s, fmt.Errorf("stats: attempts: %w", err)
	}
	for rows.Next() {
		var (
			ch       string
			ok, fail int
			p50, p95 float64
		)
		if err := rows.Scan(&ch, &ok, &fail, &p50, &p95); err != nil {
			rows.Close()
			return s, err
		}
		if cs := s.Channels[domain.Channel(ch)]; cs != nil {
			cs.AttemptsOK, cs.AttemptsFailed = ok, fail
			cs.ProviderP50, cs.ProviderP95 = ms(p50), ms(p95)
		}
	}
	rows.Close()

	rows, err = r.db.Query(ctx, `
		SELECT channel,
		       count(*) FILTER (WHERE status = 'sent'),
		       count(*) FILTER (WHERE status = 'failed'),
		       coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
		                FILTER (WHERE status = 'sent'), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
		                FILTER (WHERE status = 'sent'), 0)
		FROM notifications
		WHERE status IN ('sent', 'failed') AND updated_at > now() - make_interval(secs => $1)
		GROUP BY channel`, secs)
	if err != nil {
		return s, fmt.Errorf("stats: finished: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			ch           string
			sent, failed int
			p50, p95     float64
		)
		if err := rows.Scan(&ch, &sent, &failed, &p50, &p95); err != nil {
			return s, err
		}
		if cs := s.Channels[domain.Channel(ch)]; cs != nil {
			cs.Sent, cs.Failed = sent, failed
			cs.EndToEndP50, cs.EndToEndP95 = ms(p50), ms(p95)
		}
	}
	return s, rows.Err()
}

func ms(v float64) time.Duration { return time.Duration(v * float64(time.Millisecond)) }
