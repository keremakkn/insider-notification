package domain

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrTemplateNameTaken = errors.New("a template with this name already exists")
	ErrTemplateNotFound  = errors.New("template not found")
)

// Template is message text with {{variable}} placeholders. Rendering only
// substitutes values: there is no logic or code execution, so whoever
// writes a template cannot make the service do anything else.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

var (
	templateName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,99}$`)
	placeholder  = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)
)

// NewTemplate validates and builds a template.
func NewTemplate(name, channel, body string, now time.Time) (Template, error) {
	var v ValidationError
	if !templateName.MatchString(name) {
		v.Add("name", "must be 1-100 characters of a-z, 0-9, _ or -, starting with a letter or digit")
	}
	c, err := ParseChannel(channel)
	if err != nil {
		v.Add("channel", err.Error())
	}
	if err := validateBody(body); err != nil {
		v.Add("body", err.Error())
	}
	if v.HasErrors() {
		return Template{}, &v
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Template{}, err
	}
	return Template{ID: id, Name: name, Channel: c, Body: body, CreatedAt: now, UpdatedAt: now}, nil
}

func validateBody(body string) error {
	if strings.TrimSpace(body) == "" {
		return errors.New("is required")
	}
	// Every "{{" must open a well-formed placeholder; "{{ 1x }}" or a missing
	// "}}" is a typo the author should hear about now, not at send time.
	if strings.Count(body, "{{") != len(placeholder.FindAllStringIndex(body, -1)) {
		return errors.New("has a malformed placeholder; use {{name}} with letters, digits and _")
	}
	return nil
}

// Variables lists the placeholder names, sorted and without duplicates.
func (t Template) Variables() []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range placeholder.FindAllStringSubmatch(t.Body, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// Render substitutes vars into the body. Every placeholder must have a
// value; unused values are ignored.
func (t Template) Render(vars map[string]string) (string, error) {
	var missing []string
	for _, name := range t.Variables() {
		if _, ok := vars[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("missing values for %s", strings.Join(missing, ", "))
	}
	return placeholder.ReplaceAllStringFunc(t.Body, func(m string) string {
		return vars[placeholder.FindStringSubmatch(m)[1]]
	}), nil
}

// Update changes a template's body. Notifications already created from it
// keep the text they were rendered with.
func (t *Template) Update(body string, now time.Time) error {
	if err := validateBody(body); err != nil {
		var v ValidationError
		v.Add("body", err.Error())
		return &v
	}
	t.Body, t.UpdatedAt = body, now
	return nil
}
