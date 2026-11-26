-- /api/v1/stats reads recent activity by time. Without these indexes each
-- request would scan every delivery attempt and every finished notification.

CREATE INDEX delivery_attempts_created_idx ON delivery_attempts (created_at);

-- Finished notifications, by when they finished (updated_at is set by trigger).
CREATE INDEX notifications_finished_idx
    ON notifications (updated_at) WHERE status IN ('sent', 'failed');
