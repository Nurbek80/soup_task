package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"soupqueue/internal/store"
)

type job struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
type summary struct {
	RunID               string `json:"run_id"`
	Requested           int    `json:"requested_jobs"`
	Rows                int    `json:"job_rows"`
	Queued              int    `json:"queued"`
	Running             int    `json:"running"`
	Succeeded           int    `json:"succeeded"`
	Failed              int    `json:"failed"`
	Cancelled           int    `json:"cancelled"`
	Effects             int    `json:"effects"`
	DuplicateEffects    int    `json:"jobs_with_duplicate_effects"`
	Inconsistent        int    `json:"inconsistent_final_states"`
	Attempts            int    `json:"worker_attempts"`
	Kills               int64  `json:"worker_sigkills"`
	DuplicatePosts      int64  `json:"duplicate_posts"`
	RequestErrors       int64  `json:"request_errors"`
	DuplicateMismatches int64  `json:"duplicate_id_mismatches"`
	Duration            string `json:"duration"`
	Verdict             string `json:"verdict"`
}

func main() {
	jobs := flag.Int("jobs", 10000, "number of unique jobs")
	workers := flag.Int("workers", 8, "expected worker containers")
	rate := flag.Int("rate", 100, "unique POST requests per second; 0 is unlimited")
	killInterval := flag.Duration("kill-interval", 2*time.Second, "random worker SIGKILL interval; 0 disables")
	timeout := flag.Duration("timeout", 10*time.Minute, "terminal-state deadline")
	apiURL := flag.String("api", "http://localhost:8080", "API base URL")
	dbURL := flag.String("database-url", env("DATABASE_URL", "postgres://soup:soup@localhost:55432/soup?sslmode=disable"), "PostgreSQL URL")
	durationMS := flag.Int("duration-ms", 100, "emulated execution duration")
	flag.Parse()
	if *jobs < 1 || *workers < 1 {
		fatal("jobs and workers must be positive")
	}
	runID := "chaos-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	db, err := store.Open(ctx, *dbURL)
	if err != nil {
		fatal(err.Error())
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		fatal(err.Error())
	}
	if err = waitHealth(ctx, *apiURL); err != nil {
		fatal(err.Error())
	}
	fmt.Printf("%s run_started run_id=%s jobs=%d workers=%d rate=%d kill_interval=%s\n", stamp(), runID, *jobs, *workers, *rate, *killInterval)

	var kills, duplicatePosts, requestErrors, mismatches atomic.Int64
	killCtx, stopKills := context.WithCancel(ctx)
	if *killInterval > 0 {
		if got := workerIDs(); len(got) != *workers {
			fatal(fmt.Sprintf("expected %d running workers, found %d", *workers, len(got)))
		}
		go killer(killCtx, *killInterval, &kills)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	work := make(chan int)
	var wg sync.WaitGroup
	var throttle <-chan time.Time
	if *rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		throttle = ticker.C
	}
	for n := 0; n < 32; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				if throttle != nil {
					select {
					case <-ctx.Done():
						return
					case <-throttle:
					}
				}
				key := runID + "-" + fmt.Sprintf("%05d", i)
				created, err := post(ctx, client, *apiURL, key, *durationMS)
				if err != nil {
					requestErrors.Add(1)
					fmt.Printf("%s create_error key=%s error=%q\n", stamp(), key, err)
					continue
				}
				if i%3 == 0 {
					duplicatePosts.Add(1)
					duplicate, err := post(ctx, client, *apiURL, key, *durationMS)
					if err != nil {
						requestErrors.Add(1)
					} else if duplicate.ID != created.ID {
						mismatches.Add(1)
					}
				}
				if i%10 == 0 {
					if err := cancelJob(ctx, client, *apiURL, created.ID); err != nil && !strings.Contains(err.Error(), "409") {
						requestErrors.Add(1)
						fmt.Printf("%s cancel_error id=%s error=%q\n", stamp(), created.ID, err)
					}
				}
			}
		}()
	}
submitLoop:
	for i := 0; i < *jobs; i++ {
		select {
		case <-ctx.Done():
			break submitLoop
		case work <- i:
		}
	}
	close(work)
	wg.Wait()
	fmt.Printf("%s submissions_finished request_errors=%d duplicate_posts=%d\n", stamp(), requestErrors.Load(), duplicatePosts.Load())

	for {
		var terminal int
		err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key LIKE $1 AND status IN ('succeeded','failed','cancelled')`, runID+"-%").Scan(&terminal)
		if err != nil {
			fatal(err.Error())
		}
		if terminal == *jobs {
			break
		}
		fmt.Printf("%s progress terminal=%d/%d kills=%d\n", stamp(), terminal, *jobs, kills.Load())
		select {
		case <-ctx.Done():
			stopKills()
			fmt.Println("terminal wait timed out")
		case <-time.After(2 * time.Second):
		}
		if ctx.Err() != nil {
			break
		}
	}
	stopKills()
	time.Sleep(250 * time.Millisecond)
	s := collect(context.Background(), db, runID, *jobs)
	s.Kills = kills.Load()
	s.DuplicatePosts = duplicatePosts.Load()
	s.RequestErrors = requestErrors.Load()
	s.DuplicateMismatches = mismatches.Load()
	s.Duration = time.Since(start).Round(time.Millisecond).String()
	if s.Rows == *jobs && s.Queued == 0 && s.Running == 0 && s.DuplicateEffects == 0 && s.Inconsistent == 0 && s.RequestErrors == 0 && s.DuplicateMismatches == 0 {
		s.Verdict = "PASS"
	} else {
		s.Verdict = "FAIL"
	}
	b, _ := json.Marshal(s)
	fmt.Printf("%s summary %s\n", stamp(), b)
	if s.Verdict != "PASS" {
		os.Exit(1)
	}
}

func post(ctx context.Context, c *http.Client, base, key string, duration int) (job, error) {
	body, _ := json.Marshal(map[string]any{"duration_ms": duration, "fail": false})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := c.Do(req)
	if err != nil {
		return job{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return job{}, fmt.Errorf("status %d: %s", resp.StatusCode, data)
	}
	var j job
	err = json.Unmarshal(data, &j)
	return j, err
}
func cancelJob(ctx context.Context, c *http.Client, base, id string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, base+"/v1/jobs/"+id, nil)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
func waitHealth(ctx context.Context, base string) error {
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func workerIDs() []string {
	out, err := exec.Command("docker", "compose", "ps", "-q", "worker").Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}
func killer(ctx context.Context, interval time.Duration, count *atomic.Int64) {
	timer := time.NewTimer(jitter(interval))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			ids := workerIDs()
			if len(ids) > 0 {
				id := ids[rand.IntN(len(ids))]
				out, err := exec.Command("docker", "kill", "--signal=KILL", id).CombinedOutput()
				if err != nil {
					fmt.Printf("%s kill_error id=%s error=%q output=%q\n", stamp(), id, err, out)
				} else {
					n := count.Add(1)
					fmt.Printf("%s worker_sigkill number=%d container=%s\n", stamp(), n, id[:min(12, len(id))])
					// Compose is not a scheduler: an explicit kill suppresses its restart
					// policy. Start the same container as the replacement an orchestrator
					// would create; the old process is irrecoverably gone.
					select {
					case <-ctx.Done():
						return
					case <-time.After(250 * time.Millisecond):
					}
					if restartOut, restartErr := exec.Command("docker", "start", id).CombinedOutput(); restartErr != nil {
						fmt.Printf("%s restart_error id=%s error=%q output=%q\n", stamp(), id, restartErr, restartOut)
					} else {
						fmt.Printf("%s worker_replacement_started container=%s\n", stamp(), id[:min(12, len(id))])
					}
				}
			}
			timer.Reset(jitter(interval))
		}
	}
}
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d*8/10 + time.Duration(rand.Int64N(int64(d*4/10)))
}

func collect(ctx context.Context, db *store.Store, prefix string, requested int) summary {
	s := summary{RunID: prefix, Requested: requested}
	err := db.Pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE status='queued'),count(*) FILTER(WHERE status='running'),count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='failed'),count(*) FILTER(WHERE status='cancelled'),COALESCE(sum(attempt_count),0) FROM jobs WHERE idempotency_key LIKE $1`, prefix+"-%").Scan(&s.Rows, &s.Queued, &s.Running, &s.Succeeded, &s.Failed, &s.Cancelled, &s.Attempts)
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("collect_error=%q\n", err)
	}
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM job_effects e JOIN jobs j ON j.id=e.job_id WHERE j.idempotency_key LIKE $1`, prefix+"-%").Scan(&s.Effects)
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT e.job_id FROM job_effects e JOIN jobs j ON j.id=e.job_id WHERE j.idempotency_key LIKE $1 GROUP BY e.job_id HAVING count(*)>1)x`, prefix+"-%").Scan(&s.DuplicateEffects)
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs j WHERE j.idempotency_key LIKE $1 AND ((j.status='succeeded' AND NOT EXISTS(SELECT 1 FROM job_effects e WHERE e.job_id=j.id)) OR (j.status<>'succeeded' AND EXISTS(SELECT 1 FROM job_effects e WHERE e.job_id=j.id)))`, prefix+"-%").Scan(&s.Inconsistent)
	return s
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func stamp() string  { return time.Now().UTC().Format(time.RFC3339Nano) }
func fatal(s string) { fmt.Fprintln(os.Stderr, "fatal:", s); os.Exit(1) }
