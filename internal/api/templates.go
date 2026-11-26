package api

import "net/http"

func (h *handlers) createTemplate(w http.ResponseWriter, r *http.Request) {
	var body templateRequest
	if !h.decode(w, r, &body, maxBodyBytes) {
		return
	}
	t, err := h.templates.Create(r.Context(), body.Name, body.Channel, body.Body)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	w.Header().Set("Location", "/api/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, toTemplateResponse(t))
}

func (h *handlers) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := h.templates.List(r.Context())
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	out := make([]templateResponse, len(ts))
	for i, t := range ts {
		out[i] = toTemplateResponse(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (h *handlers) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(w, r)
	if !ok {
		return
	}
	t, err := h.templates.Get(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateResponse(t))
}

func (h *handlers) updateTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(w, r)
	if !ok {
		return
	}
	var body templateUpdateRequest
	if !h.decode(w, r, &body, maxBodyBytes) {
		return
	}
	t, err := h.templates.Update(r.Context(), id, body.Body)
	if err != nil {
		h.writeError(w, r, err, nil)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateResponse(t))
}
