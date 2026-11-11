package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"soupqueue/internal/store"
)

type Worker struct {
	Store *store.Store
	ID    string
	Lease time.Duration
	Poll  time.Duration
	Log   *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	w.Log.Info("worker started", "worker_id", w.ID, "lease", w.Lease)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		claim, err := w.Store.ClaimJob(ctx, w.ID, w.Lease)
		if err != nil {
			w.Log.Error("claim failed", "error", err)
			if !sleep(ctx, w.Poll) {
				return ctx.Err()
			}
			continue
		}
		if claim == nil {
			if !sleep(ctx, w.Poll) {
				return ctx.Err()
			}
			continue
		}
		w.execute(ctx, claim)
	}
}

func (w *Worker) execute(ctx context.Context, claim *store.Claim) {
	var payload struct {
		DurationMS int  `json:"duration_ms"`
		Fail       bool `json:"fail"`
	}
	if err := json.Unmarshal(claim.Job.Payload, &payload); err != nil {
		_, _ = w.Store.Fail(ctx, claim.Job.ID, claim.LeaseToken, "invalid stored payload")
		return
	}
	work := time.NewTimer(time.Duration(payload.DurationMS) * time.Millisecond)
	heartbeat := time.NewTicker(w.Lease / 3)
	defer work.Stop()
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			ok, err := w.Store.Heartbeat(ctx, claim.Job.ID, claim.LeaseToken, w.Lease)
			if err != nil {
				w.Log.Error("heartbeat failed", "job_id", claim.Job.ID, "error", err)
			}
			if !ok {
				w.Log.Info("lease lost; abandoning execution", "job_id", claim.Job.ID)
				return
			}
		case <-work.C:
			if payload.Fail {
				_, _ = w.Store.Fail(ctx, claim.Job.ID, claim.LeaseToken, "requested failure")
				return
			}
			ok, err := w.Store.Complete(ctx, claim.Job.ID, claim.LeaseToken, w.ID)
			if err != nil {
				w.Log.Error("completion failed", "job_id", claim.Job.ID, "error", err)
				return
			}
			if !ok {
				w.Log.Info("stale completion fenced", "job_id", claim.Job.ID)
			}
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
