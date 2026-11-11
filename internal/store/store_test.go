package store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewUUID(t *testing.T) {
	id, err := NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 36 || id[14] != '4' {
		t.Fatalf("invalid v4 UUID %q", id)
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(context.Background(), "TRUNCATE job_effects, jobs"); err != nil {
		t.Fatal(err)
	}
	return s
}

func createTestJob(t *testing.T, s *Store, key string) Job {
	t.Helper()
	r, err := s.CreateJob(context.Background(), key, json.RawMessage(`{"duration_ms":10,"fail":false}`))
	if err != nil {
		t.Fatal(err)
	}
	return r.Job
}

func TestConcurrentClaimHasOneWinner(t *testing.T) {
	s := testStore(t)
	createTestJob(t, s, "double-claim")
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.ClaimJob(context.Background(), "worker", time.Second)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if c != nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("claim winners=%d, want 1", got)
	}
}

func TestExpiredZombieIsFenced(t *testing.T) {
	s := testStore(t)
	j := createTestJob(t, s, "zombie")
	old, err := s.ClaimJob(context.Background(), "old-worker", 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(70 * time.Millisecond)
	fresh, err := s.ClaimJob(context.Background(), "fresh-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == nil {
		t.Fatal("expired job was not reclaimed")
	}
	if ok, err := s.Complete(context.Background(), j.ID, old.LeaseToken, "old-worker"); err != nil || ok {
		t.Fatalf("stale completion: ok=%v err=%v", ok, err)
	}
	if ok, err := s.Complete(context.Background(), j.ID, fresh.LeaseToken, "fresh-worker"); err != nil || !ok {
		t.Fatalf("fresh completion: ok=%v err=%v", ok, err)
	}
	var effects int
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM job_effects WHERE job_id=$1", j.ID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("effects=%d, want 1", effects)
	}
}

func TestCancelCompleteRaceIsConsistent(t *testing.T) {
	s := testStore(t)
	for i := 0; i < 50; i++ {
		j := createTestJob(t, s, "race-"+time.Now().Format("150405.000000000"))
		claim, err := s.ClaimJob(context.Background(), "worker", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _, _ = s.CancelJob(context.Background(), j.ID) }()
		go func() { defer wg.Done(); _, _ = s.Complete(context.Background(), j.ID, claim.LeaseToken, "worker") }()
		wg.Wait()
		final, err := s.GetJob(context.Background(), j.ID)
		if err != nil {
			t.Fatal(err)
		}
		var effects int
		_ = s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM job_effects WHERE job_id=$1", j.ID).Scan(&effects)
		if !((final.Status == "cancelled" && effects == 0) || (final.Status == "succeeded" && effects == 1)) {
			t.Fatalf("inconsistent final state status=%s effects=%d", final.Status, effects)
		}
	}
}

func TestConcurrentIdempotencyReturnsOneJob(t *testing.T) {
	s := testStore(t)
	payload := json.RawMessage(`{"duration_ms":10,"fail":false}`)
	ids := make(chan string, 32)
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.CreateJob(context.Background(), "same-key", payload)
			if err != nil {
				errs <- err
				return
			}
			ids <- r.Job.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("different ids %s and %s", first, id)
		}
	}
	var count int
	_ = s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM jobs WHERE idempotency_key='same-key'").Scan(&count)
	if count != 1 {
		t.Fatalf("rows=%d, want 1", count)
	}
}

func TestDifferentRequestWithSameKeyConflicts(t *testing.T) {
	s := testStore(t)
	_, err := s.CreateJob(context.Background(), "conflict", json.RawMessage(`{"duration_ms":10,"fail":false}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateJob(context.Background(), "conflict", json.RawMessage(`{"duration_ms":11,"fail":false}`))
	if err != ErrIdempotencyConflict {
		t.Fatalf("error=%v, want ErrIdempotencyConflict", err)
	}
}

func TestDatabaseClockControlsLease(t *testing.T) {
	s := testStore(t)
	createTestJob(t, s, "db-clock")
	claim, err := s.ClaimJob(context.Background(), "worker", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var delta float64
	err = s.Pool.QueryRow(context.Background(), "SELECT extract(epoch FROM (lease_expires_at-clock_timestamp())) FROM jobs WHERE id=$1", claim.Job.ID).Scan(&delta)
	if err != nil {
		t.Fatal(err)
	}
	if delta < 1.0 || delta > 2.1 {
		t.Fatalf("database-clock lease delta=%f", delta)
	}
}

func TestDocumentedIsolationLevelIsAvailable(t *testing.T) {
	s := testStore(t)
	var level string
	if err := s.Pool.QueryRow(context.Background(), "SHOW default_transaction_isolation").Scan(&level); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(level, "read committed") {
		t.Fatalf("isolation=%q, expected read committed", level)
	}
}
