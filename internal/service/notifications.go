// Package service holds the business rules that sit between HTTP and SQL.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremakkn/insider-notification/internal/domain"
	"github.com/keremakkn/insider-notification/internal/tracing"
)

// endSpan marks unexpected failures on the span. A validation error is the
// client's mistake, not the service's, so it does not turn the span red.
func endSpan(span trace.Span, err error) {
	var v *domain.ValidationError
	if err != nil && !errors.As(err, &v) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// Repository is what the service needs from storage. It is declared here,
// where it is used, so tests can pass an in-memory fake.
type Repository interface {
	Create(ctx context.Context, n domain.Notification) (domain.Notification, bool, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error)
	CreateBatch(ctx context.Context, b domain.Batch, items []domain.Notification,
		isReplay func(item, stored domain.Notification) bool) ([]domain.CreateOutcome, domain.Batch, error)
	GetBatch(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error)
}

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// TemplateGetter loads the template a request names.
type TemplateGetter interface {
	Get(ctx context.Context, id uuid.UUID) (domain.Template, error)
}

// Publisher announces status changes made here (cancellation).
type Publisher interface {
	Publish(ctx context.Context, c domain.StatusChange)
}

type Notifications struct {
	repo        Repository
	events      Publisher
	templates   TemplateGetter
	maxAttempts int
	now         func() time.Time
}

func NewNotifications(repo Repository, templates TemplateGetter, maxAttempts int) *Notifications {
	return &Notifications{repo: repo, templates: templates, maxAttempts: maxAttempts, now: time.Now}
}

// PublishTo makes the service announce the status changes it makes.
func (s *Notifications) PublishTo(p Publisher) *Notifications {
	s.events = p
	return s
}

// render turns a template request into content, in place. Rendering happens
// here, when the notification is created, so that the content rules (such
// as the SMS length) run on the final text and a too-long message is a 400
// now rather than a failure later, and so that editing the template does not
// change notifications that already exist. cache avoids reading the same
// template once per item of a batch.
func (s *Notifications) render(ctx context.Context, req *domain.CreateRequest, cache map[uuid.UUID]domain.Template) error {
	var v domain.ValidationError
	if req.TemplateID == nil {
		if len(req.Variables) > 0 {
			v.Add("variables", "are only used with templateId")
			return &v
		}
		return nil
	}
	if req.Content != "" {
		v.Add("content", "send either content or templateId, not both")
		return &v
	}
	t, ok := cache[*req.TemplateID]
	if !ok {
		var err error
		t, err = s.templates.Get(ctx, *req.TemplateID)
		if errors.Is(err, domain.ErrTemplateNotFound) {
			v.Add("templateId", "no template with this id")
			return &v
		}
		if err != nil {
			return err
		}
		cache[t.ID] = t
	}
	if req.Channel != "" && string(t.Channel) != req.Channel {
		v.Add("templateId", fmt.Sprintf("is a %s template, but the notification channel is %s", t.Channel, req.Channel))
		return &v
	}
	content, err := t.Render(req.Variables)
	if err != nil {
		v.Add("variables", err.Error())
		return &v
	}
	req.Content = content
	return nil
}

// Create validates and stores a notification. created is false when the
// idempotency key was seen before with the same request; the original is
// returned. The same key with a different request is ErrIdempotencyMismatch.
func (s *Notifications) Create(ctx context.Context, req domain.CreateRequest, correlationID string) (n domain.Notification, created bool, err error) {
	ctx, span := tracing.Tracer().Start(ctx, "notifications.create")
	defer func() { endSpan(span, err) }()

	if err := s.render(ctx, &req, map[uuid.UUID]domain.Template{}); err != nil {
		return domain.Notification{}, false, err
	}
	n, err = domain.NewNotification(req, s.maxAttempts, s.now())
	if err != nil {
		return domain.Notification{}, false, err
	}
	n.CorrelationID = correlationID
	n.TraceParent = tracing.Inject(ctx) // the worker continues this trace
	span.SetAttributes(attribute.String("notification.id", n.ID.String()),
		attribute.String("notification.channel", string(n.Channel)), attribute.String("notification.priority", n.Priority.String()))

	stored, created, err := s.repo.Create(ctx, n)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if !created && !isReplay(n, stored) {
		return domain.Notification{}, false, domain.ErrIdempotencyMismatch
	}
	return stored, created, nil
}

func (s *Notifications) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.repo.Get(ctx, id)
}

// Cancel cancels a pending notification. On ErrNotCancellable the current
// notification is returned too, so the caller can show its status.
func (s *Notifications) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.repo.Cancel(ctx, id)
	if err == nil && s.events != nil {
		s.events.Publish(ctx, domain.ChangeOf(n, s.now()))
	}
	return n, err
}

func (s *Notifications) List(ctx context.Context, f domain.ListFilter) ([]domain.Notification, *domain.Cursor, error) {
	var v domain.ValidationError
	switch {
	case f.Limit == 0:
		f.Limit = DefaultPageSize
	case f.Limit < 0 || f.Limit > MaxPageSize:
		v.Add("limit", fmt.Sprintf("must be between 1 and %d", MaxPageSize))
	}
	if f.From != nil && f.To != nil && !f.From.Before(*f.To) {
		v.Add("from", "must be before to")
	}
	if v.HasErrors() {
		return nil, nil, &v
	}
	return s.repo.List(ctx, f)
}

// isReplay is the idempotency rule: a stored notification with the same key
// is a replay only if the request was identical.
func isReplay(item, stored domain.Notification) bool {
	return item.RequestHash == stored.RequestHash
}

// ItemResult is the outcome of one batch item, in request order.
type ItemResult struct {
	Index        int
	Notification *domain.Notification // nil when rejected
	Created      bool                 // false for an idempotent replay
	Err          error                // *domain.ValidationError or domain.ErrIdempotencyMismatch
}

type BatchResult struct {
	Batch *domain.Batch // nil when no item was accepted and nothing was stored
	Items []ItemResult
}

// CreateBatch validates every item and stores the valid ones together.
// Invalid items are reported, not fatal: one bad phone number should not
// block the other 999 messages. Items are idempotent through their own keys.
func (s *Notifications) CreateBatch(ctx context.Context, reqs []domain.CreateRequest, correlationID string) (_ BatchResult, err error) {
	ctx, span := tracing.Tracer().Start(ctx, "notifications.create_batch",
		trace.WithAttributes(attribute.Int("batch.size", len(reqs))))
	defer func() { endSpan(span, err) }()
	traceparent := tracing.Inject(ctx)

	if len(reqs) == 0 || len(reqs) > domain.MaxBatchSize {
		var v domain.ValidationError
		v.Add("items", fmt.Sprintf("must contain between 1 and %d notifications", domain.MaxBatchSize))
		return BatchResult{}, &v
	}

	batchID, err := uuid.NewV7()
	if err != nil {
		return BatchResult{}, err
	}
	now := s.now()
	result := BatchResult{Items: make([]ItemResult, len(reqs))}
	var (
		valid   []domain.Notification
		indexes []int // position in reqs of each valid notification
	)
	cache := map[uuid.UUID]domain.Template{}
	for i, req := range reqs {
		result.Items[i].Index = i
		if err := s.render(ctx, &req, cache); err != nil {
			var v *domain.ValidationError
			if !errors.As(err, &v) {
				return BatchResult{}, err // the database failed, not the item
			}
			result.Items[i].Err = err
			continue
		}
		n, err := domain.NewNotification(req, s.maxAttempts, now)
		if err != nil {
			result.Items[i].Err = err
			continue
		}
		n.BatchID = &batchID
		n.CorrelationID = correlationID
		n.TraceParent = traceparent
		valid = append(valid, n)
		indexes = append(indexes, i)
	}
	if len(valid) == 0 {
		return result, nil
	}

	outcomes, batch, err := s.repo.CreateBatch(ctx, domain.Batch{
		ID: batchID, Total: len(reqs), Accepted: len(valid), Rejected: len(reqs) - len(valid), CreatedAt: now,
	}, valid, isReplay)
	if err != nil {
		return BatchResult{}, err
	}
	for j, o := range outcomes {
		item := &result.Items[indexes[j]]
		if !o.Created && !isReplay(valid[j], o.Stored) {
			item.Err = domain.ErrIdempotencyMismatch
			continue
		}
		stored := o.Stored
		item.Notification = &stored
		item.Created = o.Created
	}
	if batch.ID != uuid.Nil {
		result.Batch = &batch
	}
	return result, nil
}

func (s *Notifications) GetBatch(ctx context.Context, id uuid.UUID) (domain.BatchSummary, error) {
	return s.repo.GetBatch(ctx, id)
}
