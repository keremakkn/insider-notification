-- The W3C trace context of the request that created a notification. The
-- worker continues that trace when it delivers, across the asynchronous gap
-- the queue introduces.
ALTER TABLE notifications ADD COLUMN traceparent TEXT;
