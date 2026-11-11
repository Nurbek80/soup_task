# Silent-failure checks

These checks focus on states that can look green to an API client while corrupting queue semantics. The automated database tests live in `internal/store/store_test.go` and run with `make integration-test`.

| Risk | Prevention | Evidence |
|---|---|---|
| Double claim | Candidate row is locked and updated in one `FOR UPDATE SKIP LOCKED` statement; every claim gets a fresh token. | `TestConcurrentClaimHasOneWinner` launches 24 claimers for one job and requires exactly one winner. The full chaos run also requires no duplicate effect. |
| Zombie after lease expiry | Heartbeat and terminal writes match the random token. Completion rechecks token and database-time expiry while the job row is locked. | `TestExpiredZombieIsFenced` lets lease A expire, claims lease B, proves A cannot complete, proves B can, and counts one effect. |
| Cancel/complete race | Cancellation and completion both update/lock the same job row; only one terminal transition can win. | `TestCancelCompleteRaceIsConsistent` runs 50 real concurrent races and accepts only `cancelled + 0 effects` or `succeeded + 1 effect`. |
| Duplicate request retry | Unique `idempotency_key` is the serialization point; a stored hash binds the key to one normalized request. | `TestConcurrentIdempotencyReturnsOneJob` sends 32 concurrent creates and requires one ID/row. `TestDifferentRequestWithSameKeyConflicts` requires 409 semantics. Chaos sent 3,334 duplicates with zero ID mismatches. |
| Transaction isolation | Claim correctness comes from the row lock, not a fragile prior read. Success effect/state are in one explicit `READ COMMITTED` transaction with a row lock. | `TestDocumentedIsolationLevelIsAvailable` verifies the deployed default. Claim and cancel/complete concurrency tests exercise the protocol at that level. `SERIALIZABLE` is unnecessary for these row-local invariants and would add retry handling. |
| Clock skew | Lease creation, expiry, heartbeat, and completion all compare PostgreSQL `clock_timestamp()`; no worker wall clock is accepted. | `TestDatabaseClockControlsLease` measures the lease relative to the database clock. Source review confirms no `time.Now()` enters lease SQL. This does **not** test a discontinuous database-host clock jump. |

Schema checks additionally require lease fields if and only if a job is `running`, allow only the five declared states, and make `job_effects.job_id` a primary key. The chaos invariant query cross-checks terminal status against effect presence instead of trusting only the uniqueness constraint.
