package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/keremakkn/insider-notification/internal/domain"
)

func TestTemplatesCRUD(t *testing.T) {
	setup(t)
	ctx := context.Background()
	repo := NewTemplates(pool)

	tpl, _ := domain.NewTemplate("order_shipped", "sms", "Hi {{name}}", time.Now())
	if _, err := repo.Create(ctx, tpl); err != nil {
		t.Fatal(err)
	}
	dup, _ := domain.NewTemplate("order_shipped", "email", "Other", time.Now())
	if _, err := repo.Create(ctx, dup); !errors.Is(err, domain.ErrTemplateNameTaken) {
		t.Fatalf("duplicate name: want ErrTemplateNameTaken, got %v", err)
	}

	updated, err := repo.UpdateBody(ctx, tpl.ID, "Hello {{name}}")
	if err != nil || updated.Body != "Hello {{name}}" || !updated.UpdatedAt.After(updated.CreatedAt) {
		t.Fatalf("update: %+v %v", updated, err)
	}
	all, _ := repo.List(ctx)
	if len(all) != 1 || all[0].Name != "order_shipped" {
		t.Fatalf("list: %+v", all)
	}
	if _, err := repo.Get(ctx, dup.ID); !errors.Is(err, domain.ErrTemplateNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
}

func TestNotificationKeepsItsTemplateID(t *testing.T) {
	notifications := setup(t)
	ctx := context.Background()
	tpl, _ := domain.NewTemplate("welcome", "sms", "Hi {{name}}", time.Now())
	_, _ = NewTemplates(pool).Create(ctx, tpl)

	n := newNotification(t, "", time.Now())
	n.TemplateID = &tpl.ID
	stored, _, err := notifications.Create(ctx, n)
	if err != nil || stored.TemplateID == nil || *stored.TemplateID != tpl.ID {
		t.Fatalf("template_id not stored: %+v %v", stored.TemplateID, err)
	}
}
