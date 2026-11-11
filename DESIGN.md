# Design note

## Choice and guarantee

I chose PostgreSQL rather than SQLite. The queue has multiple worker processes and must prove behavior under concurrent claims, cancellation, and worker death. PostgreSQL supplies row locks, `FOR UPDATE SKIP LOCKED`, database-clock timestamps, explicit transaction isolation, constraints, and a realistic path to more than one API instance. SQLite can serialize writers correctly, but that serialization would hide the contention and failure behavior this exercise is asking about.

The system provides **at-least-once execution attempts and exactly-once PostgreSQL effect**. A killed worker can cause the same job computation to run again, so claiming/execution is not exactly once. The observable effect is a row in `job_effects`, keyed uniquely by `job_id`, and it is inserted in the same transaction that changes the job to `succeeded`. The transaction first locks the job and validates its current random lease token and database-time expiry. Therefore an expired/zombie worker cannot insert an effect, and a crash cannot commit the effect without the final state (or vice versa). This guarantee does not extend to a real external sink; that requires an idempotent sink keyed by job ID or a transactional outbox plus sink deduplication.

## Claim and lease protocol

Each worker asks PostgreSQL for one eligible row: `queued`, or `running` with `lease_expires_at < clock_timestamp()`. The candidate subquery orders by creation time, locks with `FOR UPDATE SKIP LOCKED`, and updates that row atomically to `running` with a fresh cryptographically random lease token, worker ID, expiry, and incremented attempt count. Concurrent workers skip the locked row, so only one receives each current lease.

The worker heartbeats every one-third of the three-second lease. A heartbeat extends the expiry only when job ID, `running` state, token, and unexpired database-time lease all still match. All lease decisions use PostgreSQL `clock_timestamp()`; worker clocks never supply an absolute time.

If a worker is killed, its connection/transaction is rolled back and heartbeats stop. After the lease expires, another worker may claim the same row with a new token. The old process may later return from a five-minute freeze, but its heartbeat, success, and failure updates all require its old token and therefore affect zero rows. In the success path, expiry is checked again while holding the row lock before the effect is written.

## Completion, failure, and cancellation

On success, the worker opens a `READ COMMITTED` transaction, locks the job, checks state/token/expiry, inserts the unique effect, changes the job to `succeeded`, clears the lease, and commits. Requested test failures change a valid current lease to `failed` and create no effect.

`DELETE /v1/jobs/{id}` atomically changes `queued` or `running` to `cancelled`, clears the lease, and records a finish time. The worker may continue CPU work until its next heartbeat or completion, but both operations are fenced and no effect can be committed. Cancellation racing completion is linearized by the job row lock: if completion commits first, cancellation sees a terminal job and returns 409; if cancellation updates first, completion's fence fails. A terminal state is never rewritten.

## API idempotency and pagination

Creation requires `Idempotency-Key`. PostgreSQL has a unique constraint on it. The normalized request body is SHA-256 hashed; a retry with the same key and body returns the original job, while the same key with a different body returns 409. This check is safe under concurrent inserts because the unique constraint is the serialization point.

Listing uses a stable keyset cursor over `(created_at, id)` in descending order rather than offsets, so inserts do not shift later pages. Limits are bounded to 200. Request bodies are size-limited and reject unknown fields.

## Remaining production risks

The database is a single container with no HA, backups, authentication strategy, TLS, rate limiting, metrics, alerting, or migration rollback. There is no dead-letter/retry policy for real failure classes. The claim index will accumulate and query shape must be observed at larger cardinalities. Most importantly, the exactly-once proof covers a database row, not an external training effect. These limitations drive the `DON'T SHIP` verdict in `CHAOS.md`.
