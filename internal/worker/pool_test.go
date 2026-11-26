package worker

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremakkn/insider-notification/internal/dispatcher"
	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/metrics"
	"github.com/keremakkn/insider-notification/internal/provider"
	"github.com/keremakkn/insider-notification/internal/queue"
	"github.com/keremakkn/insider-notification/internal/ratelimit"
	"github.com/keremakkn/insider-notification/internal/repository"
	"github.com/keremakkn/insider-notification/internal/testutil"
	"github.com/keremakkn/insider-notification/internal/tracing"
)

var (
	pool *pgxpool.Pool
	rdb  *redis.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	p, stopPG, err := testutil.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	r, stopRedis, err := testutil.StartRedis(ctx)
	if err != nil {
		stopPG()
		fmt.Fprintln(os.Stderr, "integration tests need Docker:", err)
		os.Exit(1)
	}
	pool, rdb = p, r
	code := m.Run()
	stopRedis()
	stopPG()
	os.Exit(code)
}

// fakeProvider records every request and answers with handler.
type fakeProvider struct {
	mu       sync.Mutex
	requests []time.Time
	keys     []string
	handler  func(w http.ResponseWriter, r *http.Request)
}

func (f *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, time.Now())
	f.keys = append(f.keys, r.Header.Get("X-Idempotency-Key"))
	f.mu.Unlock()
	if f.handler != nil {
		f.handler(w, r)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, `{"messageId":%q,"status":"accepted"}`, uuid.NewString())
}

func (f *fakeProvider) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// recordingEvents keeps every published status change.
type recordingEvents struct {
	mu      sync.Mutex
	changes []domain.StatusChange
}

func (r *recordingEvents) Publish(_ context.Context, c domain.StatusChange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, c)
}

func (r *recordingEvents) statuses(id uuid.UUID) []domain.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Status
	for _, c := range r.changes {
		if c.ID == id {
			out = append(out, c.Status)
		}
	}
	return out
}

// recordingSender notes when the worker hands a message to the provider,
// which is the moment the rate limiter controls. Times taken where the
// request arrives at the fake server also include network and scheduler
// delays; under a busy machine those bunch requests together and make a
// correct limiter look like it let 52 or 53 through in one second.
type recordingSender struct {
	inner provider.Sender
	mu    sync.Mutex
	at    []time.Time
}

func (r *recordingSender) Send(ctx context.Context, m provider.Message) (provider.Result, error) {
	r.mu.Lock()
	r.at = append(r.at, time.Now())
	r.mu.Unlock()
	return r.inner.Send(ctx, m)
}

type system struct {
	sent     *recordingSender
	t        *testing.T
	repo     *repository.Notifications
	provider *fakeProvider
	cancel   context.CancelFunc
	done     chan struct{}
	attempts int // max attempts for notifications this test creates
	metrics  *metrics.Delivery
	events   *recordingEvents
}

// slowClaims makes every claim take a random 0–200 ms, as on a busy
// database, so a test can tell whether anything with a variable duration
// sits between the rate limiter and the provider call.
type slowClaims struct{ Store }

func (s slowClaims) Claim(ctx context.Context, id uuid.UUID, lease time.Duration) (domain.Notification, bool, error) {
	time.Sleep(time.Duration(rand.IntN(200)) * time.Millisecond)
	return s.Store.Claim(ctx, id, lease)
}

type options struct {
	slowClaims   bool
	breaker      int // consecutive failures that open the breaker; 0 means effectively off
	rate         int
	workers      int
	maxAttempt   int
	processes    int           // worker pools running side by side, like worker containers; 0 means 1
	redisLimit   bool          // share the rate limit through Redis instead of per pool
	providerRate int           // provider quota shared by all channels and pools; 0 means none
	lease        time.Duration // 0 means 10 s
	handler      func(w http.ResponseWriter, r *http.Request)
}

// start runs a dispatcher and a worker pool against real PostgreSQL and
// Redis, and a fake provider, until the test ends.
func start(t *testing.T, o options) *system {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	testutil.Truncate(t, pool)
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if o.rate == 0 {
		o.rate = 100
	}
	if o.workers == 0 {
		o.workers = 5
	}
	if o.maxAttempt == 0 {
		o.maxAttempt = 5
	}
	if o.breaker == 0 {
		o.breaker = 1_000_000
	}
	if o.processes == 0 {
		o.processes = 1
	}
	if o.lease == 0 {
		o.lease = 10 * time.Second
	}
	fp := &fakeProvider{handler: o.handler}
	srv := httptest.NewServer(fp)
	t.Cleanup(srv.Close)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.NewDelivery(prometheus.NewRegistry())
	ev := &recordingEvents{}
	repo := repository.NewNotifications(pool)
	q := queue.New(rdb, [3]int{6, 3, 1}, time.Second)
	sender := &recordingSender{inner: provider.NewHTTP(srv.URL, 2*time.Second)}
	senders := map[domain.Channel]provider.Sender{domain.ChannelSMS: sender, domain.ChannelEmail: sender, domain.ChannelPush: sender}

	disp := dispatcher.New(repo, q, dispatcher.Config{
		Interval: 50 * time.Millisecond, Thresholds: [3]int{600, 300, 100}, VisibilityTimeout: 5 * time.Minute,
	}, log)
	var store Store = repo
	if o.slowClaims {
		store = slowClaims{repo}
	}
	var pools []*Pool
	for range o.processes {
		cfg := Config{
			WorkersPerChannel: o.workers, RatePerSecond: o.rate, Lease: o.lease,
			Backoff:          NewBackoff(25*time.Millisecond, 100*time.Millisecond),
			BreakerThreshold: o.breaker, BreakerCooldown: 300 * time.Millisecond, Metrics: m, Events: ev,
		}
		if o.providerRate > 0 {
			cfg.ProviderLimiter = ratelimit.NewRedis(rdb, "ratelimit:provider", o.providerRate, log)
		}
		if o.redisLimit {
			cfg.NewLimiter = func(c domain.Channel) Limiter {
				return ratelimit.NewRedis(rdb, "ratelimit:"+string(c), o.rate, log)
			}
		}
		pools = append(pools, NewPool(store, q, senders, cfg, log))
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &system{sent: sender, t: t, repo: repo, provider: fp, cancel: cancel, done: make(chan struct{}), attempts: o.maxAttempt, metrics: m, events: ev}
	go disp.Run(ctx)
	go func() {
		var wg sync.WaitGroup
		for _, p := range pools {
			wg.Add(1)
			go func() { defer wg.Done(); p.Run(ctx) }()
		}
		wg.Wait()
		close(s.done)
	}()
	t.Cleanup(s.stop)
	return s
}

func (s *system) stop() {
	s.cancel()
	<-s.done
}

func (s *system) create(c domain.Channel, n int) []uuid.UUID {
	s.t.Helper()
	recipient := map[domain.Channel]string{domain.ChannelSMS: "+905551234567", domain.ChannelEmail: "a@b.com", domain.ChannelPush: "token"}[c]
	ids := make([]uuid.UUID, n)
	for i := range ids {
		nt, err := domain.NewNotification(domain.CreateRequest{Recipient: recipient, Channel: string(c), Content: "hi"},
			s.attempts, time.Now().Add(-time.Second))
		if err != nil {
			s.t.Fatal(err)
		}
		nt.CorrelationID = "test"
		if _, _, err := s.repo.Create(context.Background(), nt); err != nil {
			s.t.Fatal(err)
		}
		ids[i] = nt.ID
	}
	return ids
}

// waitFor polls until every notification has reached a final status.
func (s *system) waitFor(ids []uuid.UUID, timeout time.Duration) map[uuid.UUID]domain.Notification {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := map[uuid.UUID]domain.Notification{}
		for _, id := range ids {
			n, err := s.repo.Get(context.Background(), id)
			if err != nil {
				s.t.Fatal(err)
			}
			if n.Status.IsFinal() {
				out[id] = n
			}
		}
		if len(out) == len(ids) {
			return out
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("only %d of %d notifications finished within %s", len(out), len(ids), timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDeliversEndToEnd(t *testing.T) {
	s := start(t, options{})
	ids := append(s.create(domain.ChannelSMS, 3), s.create(domain.ChannelEmail, 2)...)

	for id, n := range s.waitFor(ids, 5*time.Second) {
		if n.Status != domain.StatusSent || n.ProviderMessageID == "" || n.Attempts != 1 {
			t.Fatalf("%s: %+v", id, n)
		}
	}
	keys := map[string]bool{}
	for _, k := range s.provider.keys {
		keys[k] = true
	}
	for _, id := range ids {
		if !keys[id.String()] {
			t.Fatalf("provider never saw X-Idempotency-Key %s", id)
		}
	}
}

// Every transition is announced, in order, so a WebSocket client can follow it.
func TestStatusChangesArePublished(t *testing.T) {
	var calls atomic.Int32
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelSMS, 1)
	s.waitFor(ids, 5*time.Second)

	got := fmt.Sprint(s.events.statuses(ids[0]))
	if got != "[processing pending processing sent]" {
		t.Fatalf("want claim, retry, claim, sent; got %s", got)
	}
}

// The trace started by the API request continues in the worker, across the
// queue, and reaches the provider in the traceparent header.
func TestDeliveryContinuesTheCreatingTrace(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	var header atomic.Value
	s := start(t, options{handler: func(w http.ResponseWriter, r *http.Request) {
		header.Store(r.Header.Get("traceparent"))
		w.WriteHeader(http.StatusAccepted)
	}})

	// Stand-in for the API request that created the notification.
	ctx, create := tp.Tracer("api").Start(context.Background(), "notifications.create")
	nt, _ := domain.NewNotification(domain.CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: "hi"}, 5, time.Now().Add(-time.Second))
	nt.CorrelationID, nt.TraceParent = "c", tracing.Inject(ctx)
	create.End()
	if _, _, err := s.repo.Create(context.Background(), nt); err != nil {
		t.Fatal(err)
	}
	s.waitFor([]uuid.UUID{nt.ID}, 5*time.Second)

	var deliver sdktrace.ReadOnlySpan
	for _, sp := range spans.Ended() {
		if sp.Name() == "notification.deliver" {
			deliver = sp
		}
	}
	if deliver == nil {
		t.Fatal("no delivery span")
	}
	want := create.SpanContext()
	if deliver.SpanContext().TraceID() != want.TraceID() || deliver.Parent().SpanID() != want.SpanID() {
		t.Fatal("delivery must be a child of the creating span, in the same trace")
	}
	got := trace.SpanContextFromContext(tracing.Extract(context.Background(), header.Load().(string)))
	if got.TraceID() != want.TraceID() {
		t.Fatalf("the provider should receive the same trace in traceparent, got %q", header.Load())
	}
}

func TestDeliveryMetrics(t *testing.T) {
	var calls atomic.Int32
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelSMS, 3)
	s.waitFor(ids, 5*time.Second)

	sent := promtest.ToFloat64(s.metrics.Attempts.WithLabelValues("sms", "sent"))
	retry := promtest.ToFloat64(s.metrics.Attempts.WithLabelValues("sms", "retry"))
	if sent != 3 || retry != 1 {
		t.Fatalf("want 3 sent and 1 retry, got %v and %v", sent, retry)
	}
	if n := promtest.CollectAndCount(s.metrics.EndToEnd); n != 1 {
		t.Fatalf("end-to-end latency should have one series (sms, normal), got %d", n)
	}
}

func TestRetryableFailureIsRetriedThenFails(t *testing.T) {
	s := start(t, options{maxAttempt: 3, handler: func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}})
	ids := s.create(domain.ChannelSMS, 1)

	n := s.waitFor(ids, 5*time.Second)[ids[0]]
	if n.Status != domain.StatusFailed || n.Attempts != 3 || s.provider.count() != 3 {
		t.Fatalf("want 3 attempts then failed, got %s after %d attempts (%d calls): %s", n.Status, n.Attempts, s.provider.count(), n.LastError)
	}
	var recorded int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM delivery_attempts WHERE notification_id = $1 AND http_status = 503`, ids[0]).Scan(&recorded)
	if recorded != 3 {
		t.Fatalf("every attempt should be recorded, got %d", recorded)
	}
}

func TestRecoversAfterTransientFailure(t *testing.T) {
	var calls atomic.Int32
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelPush, 1)
	if n := s.waitFor(ids, 5*time.Second)[ids[0]]; n.Status != domain.StatusSent || n.Attempts != 2 {
		t.Fatalf("want sent on attempt 2, got %s on %d", n.Status, n.Attempts)
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"unknown number"}`))
	}})
	ids := s.create(domain.ChannelSMS, 1)
	n := s.waitFor(ids, 5*time.Second)[ids[0]]
	time.Sleep(200 * time.Millisecond) // give a wrong retry the chance to happen
	if n.Status != domain.StatusFailed || s.provider.count() != 1 {
		t.Fatalf("want failed after one call, got %s after %d calls", n.Status, s.provider.count())
	}
}

// A notification cancelled after it entered the queue must never be sent.
func TestCancelledInTheQueueIsNeverSent(t *testing.T) {
	s := start(t, options{rate: 1, workers: 1}) // slow: the first message holds the only worker
	ids := s.create(domain.ChannelEmail, 3)
	time.Sleep(150 * time.Millisecond) // all three are in Redis by now
	if _, err := s.repo.Cancel(context.Background(), ids[2]); err != nil {
		t.Fatal(err)
	}
	got := s.waitFor(ids, 6*time.Second)
	if got[ids[2]].Status != domain.StatusCancelled || s.provider.count() != 2 {
		t.Fatalf("want the cancelled one skipped: status %s, provider calls %d", got[ids[2]].Status, s.provider.count())
	}
}

// Workers sit idle on an empty queue, then a burst arrives, and claims take
// a variable time. In no one-second window may the workers send more than
// the limit (plus the single token of burst the bucket allows). This fails
// if the token is taken before the dequeue (idle workers hoard tokens) or
// before the claim (claim delays bunch requests). Times are taken where the
// worker sends; see recordingSender for why not where the request arrives.
func TestRateLimitHoldsAfterIdle(t *testing.T) {
	const limit = 50
	s := start(t, options{rate: limit, workers: 20, slowClaims: true})
	time.Sleep(1500 * time.Millisecond) // idle workers block on BLPOP
	ids := s.create(domain.ChannelSMS, 150)

	begin := time.Now()
	s.waitFor(ids, 10*time.Second)
	elapsed := time.Since(begin)

	worst := s.busiestSecond()
	t.Logf("150 messages at %d/s took %s; busiest one-second window: %d requests", limit, elapsed.Round(time.Millisecond), worst)
	// The bucket guarantees at most limit+1 tokens (burst 1) per second of
	// its own schedule. Wall-clock times add timer wake-up delays, which are
	// never early but can be tens of milliseconds late on a loaded machine
	// with the race detector: a late request at a window's start shifts the
	// window and lets one or two more in at its end.
	const schedulerAllowance = 2
	if worst > limit+1+schedulerAllowance {
		t.Fatalf("rate limit exceeded: %d requests in one second", worst)
	}
}

// Two worker processes share one limit through Redis. With the in-process
// limiter each would get its own and together they would send twice the rate.
func TestRateLimitHoldsAcrossWorkerProcesses(t *testing.T) {
	const limit = 50
	s := start(t, options{rate: limit, workers: 10, processes: 2, redisLimit: true, slowClaims: true})
	ids := s.create(domain.ChannelSMS, 150)

	begin := time.Now()
	s.waitFor(ids, 10*time.Second)
	worst := s.busiestSecond()
	t.Logf("150 messages at %d/s over two processes took %s; busiest one-second window: %d requests",
		limit, time.Since(begin).Round(time.Millisecond), worst)
	const schedulerAllowance = 2 // see TestRateLimitHoldsAfterIdle
	if worst > limit+1+schedulerAllowance {
		t.Fatalf("rate limit exceeded: %d requests in one second", worst)
	}
}

// A provider quota holds across all channels and processes together, while
// each channel alone could send far more.
func TestProviderLimitCapsAllChannelsTogether(t *testing.T) {
	const quota = 20
	s := start(t, options{rate: 100, workers: 5, processes: 2, redisLimit: true, providerRate: quota})
	var ids []uuid.UUID
	for _, c := range domain.Channels {
		ids = append(ids, s.create(c, 25)...)
	}

	begin := time.Now()
	sent := s.waitFor(ids, 15*time.Second)
	worst := s.busiestSecond()
	t.Logf("75 messages over 3 channels and two processes at a %d/s quota took %s; busiest one-second window: %d requests",
		quota, time.Since(begin).Round(time.Millisecond), worst)
	for id, n := range sent {
		if n.Status != domain.StatusSent {
			t.Fatalf("%s ended %s", id, n.Status)
		}
	}
	// The channel wait after the provider slot can shift a request by a few
	// milliseconds, so the quota gets the same small allowance as the channel limit.
	const schedulerAllowance = 2
	if worst > quota+1+schedulerAllowance {
		t.Fatalf("provider quota exceeded: %d requests in one second", worst)
	}
}

// Waiting for the provider quota must not hold a lease. Three workers at a
// 2/s quota queue up for 0, 0.5 and 1 s; with a 500 ms lease, a worker that
// claimed first and waited second would lose its lease mid-wait, another
// worker would claim the message again, and both would send it.
func TestProviderQuotaWaitHoldsNoLease(t *testing.T) {
	s := start(t, options{workers: 3, providerRate: 2, lease: 500 * time.Millisecond})
	ids := s.create(domain.ChannelSMS, 6)
	for id, n := range s.waitFor(ids, 15*time.Second) {
		if n.Status != domain.StatusSent {
			t.Fatalf("%s ended %s", id, n.Status)
		}
	}
	time.Sleep(time.Second) // a duplicate still waiting for its slot would arrive now

	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	seen := map[string]int{}
	for _, k := range s.provider.keys {
		seen[k]++
	}
	for k, n := range seen {
		if n > 1 {
			t.Errorf("notification %s reached the provider %d times", k, n)
		}
	}
	if len(s.provider.keys) != len(ids) {
		t.Fatalf("provider got %d requests for %d notifications", len(s.provider.keys), len(ids))
	}
}

// busiestSecond is the most provider calls inside any one-second window.
func (s *system) busiestSecond() int {
	s.sent.mu.Lock()
	times := append([]time.Time(nil), s.sent.at...)
	s.sent.mu.Unlock()
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	worst := 0
	for i := range times {
		j := i
		for j < len(times) && times[j].Sub(times[i]) < time.Second {
			j++
		}
		worst = max(worst, j-i)
	}
	return worst
}

// On shutdown, a delivery already in flight completes instead of being
// abandoned (which would only cause a resend after the lease).
func TestShutdownFinishesInFlightDelivery(t *testing.T) {
	arrived := make(chan struct{}, 1)
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelSMS, 1)
	<-arrived
	s.stop() // SIGTERM while the provider is still answering

	n, _ := s.repo.Get(context.Background(), ids[0])
	if n.Status != domain.StatusSent {
		t.Fatalf("in-flight delivery should finish on shutdown, got %s", n.Status)
	}
}

// The provider honours Retry-After: the second attempt waits at least as
// long as the provider asked, far beyond our own 25ms backoff.
func TestRetryAfterIsHonoured(t *testing.T) {
	var calls atomic.Int32
	s := start(t, options{handler: func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelSMS, 1)
	if n := s.waitFor(ids, 5*time.Second)[ids[0]]; n.Status != domain.StatusSent || n.Attempts != 2 {
		t.Fatalf("want sent on attempt 2, got %s on %d", n.Status, n.Attempts)
	}
	s.provider.mu.Lock()
	gap := s.provider.requests[1].Sub(s.provider.requests[0])
	s.provider.mu.Unlock()
	t.Logf("second attempt came %s after the 429", gap.Round(time.Millisecond))
	if gap < time.Second {
		t.Fatalf("Retry-After: 1 was ignored, retried after %s", gap)
	}
}

// The provider is down for a while. Without a breaker, 30 messages with 10
// attempts each would hammer it with up to 300 requests. With the breaker
// the outage costs a handful of requests and no message runs out of
// attempts; once the provider is back, everything is delivered.
func TestBreakerShieldsAProviderThatIsDown(t *testing.T) {
	var (
		down         atomic.Bool
		duringOutage atomic.Int32
	)
	down.Store(true)
	s := start(t, options{breaker: 3, maxAttempt: 10, handler: func(w http.ResponseWriter, _ *http.Request) {
		if down.Load() {
			duringOutage.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}})
	ids := s.create(domain.ChannelEmail, 30)

	time.Sleep(1500 * time.Millisecond)
	down.Store(false)
	got := s.waitFor(ids, 10*time.Second)

	sent := 0
	for _, n := range got {
		if n.Status == domain.StatusSent {
			sent++
		}
	}
	t.Logf("1.5 s outage: %d provider calls during the outage, %d/30 delivered afterwards", duringOutage.Load(), sent)
	if sent != 30 {
		t.Fatalf("every message should survive the outage, %d were delivered", sent)
	}
	// Threshold 3, plus requests already in flight when it opened (5
	// workers), plus about one probe per 300 ms cool-down.
	if duringOutage.Load() > 20 {
		t.Fatalf("breaker did not shield the provider: %d calls during the outage", duringOutage.Load())
	}
}
