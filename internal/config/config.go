// Package config loads service settings from environment variables and
// rejects combinations that would break the delivery guarantees in docs/DESIGN.md.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Priorities are indexed 0 (high), 1 (normal), 2 (low) everywhere in the service.
const NumPriorities = 3

// NumChannels is sms, email and push. They all share PROVIDER_RATE_LIMIT.
const NumChannels = 3

// queueSeconds is how much work each Redis queue holds by default: enough to
// keep workers busy between dispatcher ticks, short enough that the backlog
// of a burst waits in PostgreSQL (docs/DESIGN.md §5.3).
const queueSeconds = 10

type Config struct {
	HTTPAddr    string
	MetricsAddr string   // worker: /metrics and health probes
	WSOrigins   []string // browser origins allowed to open WebSockets
	DatabaseURL string
	RedisAddr   string
	LogLevel    string

	ProviderURL     string
	ProviderTimeout time.Duration
	// ProviderRateLimit caps requests per second to the provider across all
	// channels and worker processes; 0 means no cap beyond the channel limits.
	// It is for providers with their own quota, such as free webhook.site.
	ProviderRateLimit int

	RateLimitPerChannel int
	WorkersPerChannel   int
	PriorityWeights     [NumPriorities]int
	QueueThresholds     [NumPriorities]int

	DispatchInterval  time.Duration
	VisibilityTimeout time.Duration
	LeaseDuration     time.Duration

	RetryBase   time.Duration
	RetryCap    time.Duration
	MaxAttempts int

	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// Load reads the configuration from the environment, applies defaults and validates it.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}
	c := Config{
		HTTPAddr:    p.str("HTTP_ADDR", ":8080"),
		MetricsAddr: p.str("METRICS_ADDR", ":9091"),
		WSOrigins:   splitList(p.str("WS_ALLOWED_ORIGINS", "localhost:*")),
		DatabaseURL: p.str("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"),
		RedisAddr:   p.str("REDIS_ADDR", "localhost:6379"),
		LogLevel:    p.str("LOG_LEVEL", "info"),

		ProviderURL:       p.str("PROVIDER_URL", ""),
		ProviderTimeout:   p.duration("PROVIDER_TIMEOUT", 5*time.Second),
		ProviderRateLimit: p.int("PROVIDER_RATE_LIMIT", 0),

		RateLimitPerChannel: p.int("RATE_LIMIT_PER_CHANNEL", 100),
		WorkersPerChannel:   p.int("WORKERS_PER_CHANNEL", 20),
		PriorityWeights:     p.triple("PRIORITY_WEIGHTS", [NumPriorities]int{6, 3, 1}),

		DispatchInterval:  p.duration("DISPATCH_INTERVAL", time.Second),
		VisibilityTimeout: p.duration("VISIBILITY_TIMEOUT", 5*time.Minute),
		LeaseDuration:     p.duration("LEASE_DURATION", 30*time.Second),

		RetryBase:   p.duration("RETRY_BASE", time.Second),
		RetryCap:    p.duration("RETRY_CAP", 5*time.Minute),
		MaxAttempts: p.int("MAX_ATTEMPTS", 5),

		BreakerThreshold: p.int("BREAKER_THRESHOLD", 5),
		BreakerCooldown:  p.duration("BREAKER_COOLDOWN", 30*time.Second),
	}
	if err := errors.Join(p.errs...); err != nil {
		return Config{}, err
	}
	if err := c.validateRates(); err != nil {
		return Config{}, err
	}
	// By default each queue holds queueSeconds of its own drain rate (600,
	// 300, 100 at 100/s), so a lower rate gets shorter queues by itself.
	c.QueueThresholds = p.triple("QUEUE_THRESHOLDS", c.defaultThresholds())
	if err := errors.Join(p.errs...); err != nil {
		return Config{}, err
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ChannelDrainRate is how many messages per second one channel can send
// when every channel is busy: its own limit, or its share of the provider
// limit if that is lower.
func (c Config) ChannelDrainRate() float64 {
	rate := float64(c.RateLimitPerChannel)
	if c.ProviderRateLimit > 0 {
		rate = min(rate, float64(c.ProviderRateLimit)/NumChannels)
	}
	return rate
}

// priorityDrainRate is one priority's share of its channel's drain rate.
func (c Config) priorityDrainRate(i int) float64 {
	total := 0
	for _, w := range c.PriorityWeights {
		total += w
	}
	return c.ChannelDrainRate() * float64(c.PriorityWeights[i]) / float64(total)
}

func (c Config) defaultThresholds() [NumPriorities]int {
	var out [NumPriorities]int
	for i := range NumPriorities {
		out[i] = max(1, int(math.Round(queueSeconds*c.priorityDrainRate(i))))
	}
	return out
}

// MaxQueueWait is the longest time an ID can wait in a full Redis queue under
// load: threshold divided by that queue's share of the channel's drain rate.
func (c Config) MaxQueueWait() time.Duration {
	var longest time.Duration
	for i := range NumPriorities {
		wait := time.Duration(float64(c.QueueThresholds[i]) / c.priorityDrainRate(i) * float64(time.Second))
		longest = max(longest, wait)
	}
	return longest
}

// MaxLimiterWait is how long a claimed notification can wait for its turn
// at the provider limit in one worker process: every worker of every
// channel is ahead of it. Without a provider limit it is the channel's own
// workers at the channel rate.
func (c Config) MaxLimiterWait() time.Duration {
	waiting, rate := float64(c.WorkersPerChannel), float64(c.RateLimitPerChannel)
	if c.ProviderRateLimit > 0 {
		waiting, rate = float64(NumChannels*c.WorkersPerChannel), min(float64(NumChannels)*rate, float64(c.ProviderRateLimit))
	}
	return time.Duration(waiting / rate * float64(time.Second))
}

// validateRates checks what the derived defaults are computed from, so they
// are never computed from nonsense.
func (c Config) validateRates() error {
	var errs []error
	positive := map[string]int{
		"RATE_LIMIT_PER_CHANNEL": c.RateLimitPerChannel,
		"WORKERS_PER_CHANNEL":    c.WorkersPerChannel,
		"MAX_ATTEMPTS":           c.MaxAttempts,
		"BREAKER_THRESHOLD":      c.BreakerThreshold,
	}
	for name, v := range positive {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %d", name, v))
		}
	}
	if c.ProviderRateLimit < 0 {
		errs = append(errs, fmt.Errorf("PROVIDER_RATE_LIMIT must be positive, or 0 for no limit, got %d", c.ProviderRateLimit))
	}
	for i := range NumPriorities {
		if c.PriorityWeights[i] <= 0 {
			errs = append(errs, fmt.Errorf("PRIORITY_WEIGHTS: every weight must be positive, or that priority starves"))
		}
	}
	return errors.Join(errs...)
}

func (c Config) validate() error {
	var errs []error
	for i := range NumPriorities {
		if c.QueueThresholds[i] <= 0 {
			errs = append(errs, fmt.Errorf("QUEUE_THRESHOLDS: every threshold must be positive"))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// A lease shorter than the provider timeout would expire while the request
	// is still in flight, and a second worker would send the same message.
	if c.LeaseDuration <= c.ProviderTimeout {
		errs = append(errs, fmt.Errorf("LEASE_DURATION (%s) must be longer than PROVIDER_TIMEOUT (%s)",
			c.LeaseDuration, c.ProviderTimeout))
	} else if wait := c.MaxLimiterWait(); c.LeaseDuration <= c.ProviderTimeout+wait {
		// The lease starts at the claim, before the rate-limit wait. A slow
		// provider limit with many workers could outlast it.
		errs = append(errs, fmt.Errorf("LEASE_DURATION (%s) must be longer than PROVIDER_TIMEOUT (%s) plus the longest rate-limit wait (%s); lower WORKERS_PER_CHANNEL or raise LEASE_DURATION",
			c.LeaseDuration, c.ProviderTimeout, wait))
	}
	// A visibility timeout shorter than the normal queue wait would make the
	// dispatcher re-enqueue messages that are not lost, only waiting.
	if wait := c.MaxQueueWait(); c.VisibilityTimeout <= wait {
		errs = append(errs, fmt.Errorf("VISIBILITY_TIMEOUT (%s) must be longer than the slowest queue's drain time (%s); lower QUEUE_THRESHOLDS or raise the timeout",
			c.VisibilityTimeout, wait))
	}
	if c.RetryCap < c.RetryBase {
		errs = append(errs, fmt.Errorf("RETRY_CAP (%s) must not be shorter than RETRY_BASE (%s)", c.RetryCap, c.RetryBase))
	}
	return errors.Join(errs...)
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) str(key, def string) string {
	if v := p.getenv(key); v != "" {
		return v
	}
	return def
}

func (p *parser) int(key string, def int) int {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.errs = append(p.errs, fmt.Errorf("%s: %q is not a duration (examples: 500ms, 5s, 5m)", key, v))
		return def
	}
	return d
}

// triple parses "high,normal,low", for example "6,3,1".
func (p *parser) triple(key string, def [NumPriorities]int) [NumPriorities]int {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	if len(parts) != NumPriorities {
		p.errs = append(p.errs, fmt.Errorf("%s: want 3 comma-separated values (high,normal,low), got %q", key, v))
		return def
	}
	var out [NumPriorities]int
	for i, s := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("%s: %q is not an integer", key, s))
			return def
		}
		out[i] = n
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
