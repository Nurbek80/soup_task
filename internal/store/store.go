package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")

type Store struct{ Pool *pgxpool.Pool }

type Job struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          *string         `json:"error,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

type Page struct {
	Jobs       []Job  `json:"jobs"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type CreateResult struct {
	Job     Job
	Created bool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(20261106)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return tx.Commit(ctx)
}

func NewUUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

const jobColumns = `id::text, idempotency_key, payload, status, worker_id, attempt_count,
result, error, lease_expires_at, created_at, updated_at, started_at, finished_at`

const claimedJobColumns = `j.id::text, j.idempotency_key, j.payload, j.status, j.worker_id, j.attempt_count,
j.result, j.error, j.lease_expires_at, j.created_at, j.updated_at, j.started_at, j.finished_at`

type rowScanner interface{ Scan(...any) error }

func scanJob(row rowScanner) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.Payload, &j.Status, &j.WorkerID, &j.AttemptCount,
		&j.Result, &j.Error, &j.LeaseExpiresAt, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	return j, err
}

func (s *Store) CreateJob(ctx context.Context, key string, payload json.RawMessage) (CreateResult, error) {
	id, err := NewUUID()
	if err != nil {
		return CreateResult{}, err
	}
	hash := sha256.Sum256(payload)
	row := s.Pool.QueryRow(ctx, `INSERT INTO jobs (id, idempotency_key, request_hash, payload, status)
		VALUES ($1, $2, $3, $4, 'queued') ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns, id, key, hash[:], payload)
	j, err := scanJob(row)
	if err == nil {
		return CreateResult{Job: j, Created: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateResult{}, err
	}
	var existingHash []byte
	row = s.Pool.QueryRow(ctx, `SELECT `+jobColumns+`, request_hash FROM jobs WHERE idempotency_key=$1`, key)
	var existing Job
	err = row.Scan(&existing.ID, &existing.IdempotencyKey, &existing.Payload, &existing.Status, &existing.WorkerID,
		&existing.AttemptCount, &existing.Result, &existing.Error, &existing.LeaseExpiresAt, &existing.CreatedAt,
		&existing.UpdatedAt, &existing.StartedAt, &existing.FinishedAt, &existingHash)
	if err != nil {
		return CreateResult{}, err
	}
	if string(existingHash) != string(hash[:]) {
		return CreateResult{}, ErrIdempotencyConflict
	}
	return CreateResult{Job: existing, Created: false}, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	return scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id))
}

type cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func EncodeCursor(t time.Time, id string) string {
	b, _ := json.Marshal(cursor{CreatedAt: t, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(value string) (time.Time, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return time.Time{}, "", err
	}
	var c cursor
	if err = json.Unmarshal(b, &c); err != nil {
		return time.Time{}, "", err
	}
	if c.CreatedAt.IsZero() || c.ID == "" {
		return time.Time{}, "", errors.New("incomplete cursor")
	}
	return c.CreatedAt, c.ID, nil
}

func (s *Store) ListJobs(ctx context.Context, limit int, cursorValue string) (Page, error) {
	args := []any{limit + 1}
	where := ""
	if cursorValue != "" {
		t, id, err := DecodeCursor(cursorValue)
		if err != nil {
			return Page{}, err
		}
		where = "WHERE (created_at, id) < ($2, $3)"
		args = append(args, t, id)
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs `+where+` ORDER BY created_at DESC, id DESC LIMIT $1`, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Jobs: make([]Job, 0, limit)}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return Page{}, err
		}
		page.Jobs = append(page.Jobs, j)
	}
	if err = rows.Err(); err != nil {
		return Page{}, err
	}
	if len(page.Jobs) > limit {
		last := page.Jobs[limit-1]
		page.NextCursor = EncodeCursor(last.CreatedAt, last.ID)
		page.Jobs = page.Jobs[:limit]
	}
	return page, nil
}

func (s *Store) CancelJob(ctx context.Context, id string) (Job, bool, error) {
	j, err := scanJob(s.Pool.QueryRow(ctx, `UPDATE jobs SET status='cancelled', lease_token=NULL,
		lease_expires_at=NULL, worker_id=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE id=$1 AND status IN ('queued','running') RETURNING `+jobColumns, id))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}
	j, err = s.GetJob(ctx, id)
	return j, false, err
}

type Claim struct {
	Job        Job
	LeaseToken string
}

func (s *Store) ClaimJob(ctx context.Context, workerID string, lease time.Duration) (*Claim, error) {
	token, err := NewUUID()
	if err != nil {
		return nil, err
	}
	ms := lease.Milliseconds()
	row := s.Pool.QueryRow(ctx, `WITH candidate AS (
		SELECT id FROM jobs
		WHERE status='queued' OR (status='running' AND lease_expires_at < clock_timestamp())
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
	) UPDATE jobs j SET status='running', lease_token=$1, worker_id=$2,
		lease_expires_at=clock_timestamp()+($3 * interval '1 millisecond'),
		attempt_count=j.attempt_count+1, started_at=COALESCE(j.started_at, clock_timestamp()),
		updated_at=clock_timestamp(), finished_at=NULL
		FROM candidate c WHERE j.id=c.id RETURNING `+claimedJobColumns, token, workerID, ms)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Claim{Job: j, LeaseToken: token}, nil
}

func (s *Store) Heartbeat(ctx context.Context, jobID, token string, lease time.Duration) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()+($3 * interval '1 millisecond'),
		updated_at=clock_timestamp() WHERE id=$1 AND status='running' AND lease_token=$2
		AND lease_expires_at >= clock_timestamp()`, jobID, token, lease.Milliseconds())
	return err == nil && tag.RowsAffected() == 1, err
}

// Complete inserts the observable effect and marks success in one transaction.
// The row lock and lease token are a fencing check: a stale worker cannot write.
func (s *Store) Complete(ctx context.Context, jobID, token, workerID string) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var valid bool
	err = tx.QueryRow(ctx, `SELECT status='running' AND lease_token=$2 AND lease_expires_at >= clock_timestamp()
		FROM jobs WHERE id=$1 FOR UPDATE`, jobID, token).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil || !valid {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO job_effects(job_id, lease_token, worker_id) VALUES($1,$2,$3)`, jobID, token, workerID); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status='succeeded', result=jsonb_build_object('effect','written'),
		lease_token=NULL, lease_expires_at=NULL, worker_id=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE id=$1 AND status='running' AND lease_token=$2`, jobID, token)
	if err != nil || tag.RowsAffected() != 1 {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) Fail(ctx context.Context, jobID, token, message string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs SET status='failed', error=$3, lease_token=NULL,
		lease_expires_at=NULL, worker_id=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE id=$1 AND status='running' AND lease_token=$2 AND lease_expires_at >= clock_timestamp()`, jobID, token, message)
	return err == nil && tag.RowsAffected() == 1, err
}
