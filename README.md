# Soup training-job queue

A small Go/PostgreSQL queue that is deliberately designed around failure behavior, not only the happy path. It provides an HTTP API, leased workers, idempotent creation, cancellation, cursor pagination, a transactionally recorded side effect, integration tests for the silent races, and a destructive worker chaos harness.

## Run it

Requirements: Docker with Compose v2. The default stack starts PostgreSQL, one API, and four workers.

```bash
docker compose up --build
```

The API is at `http://localhost:8080`. PostgreSQL is exposed on host port `55432` to avoid colliding with a common local installation. Set `POSTGRES_PORT` to override it.

```bash
# Create (201); repeating the identical request/key returns the same job (200).
curl -i -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"duration_ms":1000,"fail":false}'

# Status
curl http://localhost:8080/v1/jobs/JOB_ID

# Cursor pagination (1 <= limit <= 200)
curl 'http://localhost:8080/v1/jobs?limit=50'
curl 'http://localhost:8080/v1/jobs?limit=50&cursor=NEXT_CURSOR'

# Cancel a queued/running job; terminal jobs return 409.
curl -X DELETE http://localhost:8080/v1/jobs/JOB_ID
```

The state machine is `queued -> running -> succeeded | failed | cancelled`. A running job can also be reclaimed as `running` with a new lease after a worker dies. Cancellation is immediate in storage; a worker that is still computing loses its fence and cannot write an effect.

## Tests and evidence

```bash
# Compile, unit test, and vet (database tests skip without TEST_DATABASE_URL).
make test

# Start PostgreSQL, stop app workers that could consume test rows, and run
# the race/invariant integration tests (this truncates the queue tables).
make integration-test

# Required destructive run: 10,000 jobs, 8 workers, 100 unique POST/s,
# duplicate retries and cancellation, random SIGKILL about every 2 seconds.
make chaos
```

The chaos command creates a new append-only log under `artifacts/chaos/` on every invocation and does not overwrite failures. Tunable environment variables are `JOBS`, `WORKERS`, `RATE`, `DURATION_MS`, `KILL_INTERVAL`, and `TIMEOUT`.

## Map of the submission

- `DESIGN.md` - claim/lease/cancel/effect protocol and delivery guarantee.
- `SILENT_FAILURES.md` - how each named silent failure was checked.
- `CHAOS.md` - harness, all run results, limitations, and production verdict.
- `AI_USE.md` - where AI was used and what was independently verified or corrected.
- `internal/store/schema.sql` - database invariants and unique effect constraint.
- `internal/store/store.go` - queue transactions and fencing checks.
- `internal/store/store_test.go` - concurrent database tests.
- `cmd/chaos/main.go` and `scripts/chaos.sh` - workload, kills, and invariant queries.

## What surprised me and what still concerns me

The most useful failure was not in queue logic: Compose did not replace containers after an explicit `docker kill`, despite a restart policy, so the first kill-enabled diagnostic stopped at 20/200 terminal jobs. The harness now explicitly starts a replacement, modeling the action a real scheduler would take, and the failure remains in the raw log. What still concerns me is the boundary of the exactly-once claim: it is exactly once only because the demonstrative effect and job transition share one PostgreSQL transaction. A real training system writes to object stores, model registries, and GPUs outside that transaction; without sink-side idempotency or an outbox/deduplication protocol, it is only at-least-once execution with possible external duplicate effects.
