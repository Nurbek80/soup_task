package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"soupqueue/internal/store"
)

type API struct {
	Store *store.Store
	Log   *slog.Logger
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/jobs", a.create)
	mux.HandleFunc("GET /v1/jobs", a.list)
	mux.HandleFunc("GET /v1/jobs/{id}", a.get)
	mux.HandleFunc("DELETE /v1/jobs/{id}", a.cancel)
	return logging(a.Log, mux)
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 {
		problem(w, http.StatusBadRequest, "Idempotency-Key header is required and must be <= 200 characters")
		return
	}
	var body struct {
		DurationMS int  `json:"duration_ms"`
		Fail       bool `json:"fail"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if body.DurationMS == 0 {
		body.DurationMS = 50
	}
	if body.DurationMS < 1 || body.DurationMS > 300000 {
		problem(w, http.StatusBadRequest, "duration_ms must be between 1 and 300000")
		return
	}
	payload, _ := json.Marshal(body)
	result, err := a.Store.CreateJob(r.Context(), key, payload)
	if errors.Is(err, store.ErrIdempotencyConflict) {
		problem(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		a.internal(w, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/v1/jobs/"+result.Job.ID)
	writeJSON(w, status, result.Job)
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	j, err := a.Store.GetJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		a.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 200 {
			problem(w, http.StatusBadRequest, "limit must be between 1 and 200")
			return
		}
		limit = v
	}
	page, err := a.Store.ListJobs(r.Context(), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	j, changed, err := a.Store.CancelJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		a.internal(w, err)
		return
	}
	if !changed {
		problem(w, http.StatusConflict, "job is already terminal: "+j.Status)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (a *API) internal(w http.ResponseWriter, err error) {
	a.Log.Error("request failed", "error", err)
	problem(w, http.StatusInternalServerError, "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]any{"status": status, "error": detail})
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info("request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
