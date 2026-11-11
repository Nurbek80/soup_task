# Chaos evidence and verdict

## Harness

`make chaos` starts the stack with eight workers and runs `cmd/chaos` with these defaults:

- 10,000 distinct idempotency keys at 100 unique POSTs/second;
- an identical retry for every third key (3,334 duplicates);
- cancellation for every tenth job (1,000 attempts) in the same client pool;
- a randomly chosen running worker sent `SIGKILL` at a jittered interval of 1.6-2.4 seconds;
- a replacement started after 250 ms because Compose is not a scheduler;
- polling until every job is terminal, then direct SQL invariant checks.

The checks require: exactly 10,000 rows for 10,000 keys; no queued/running jobs; no job with more than one effect; every succeeded job has one effect; no non-succeeded job has an effect; no request error; and no duplicate POST returning a different ID. `worker_attempts` is the sum of claims, so values above the number succeeded/cancelled are direct evidence of reclaimed work.

## Raw runs (none hidden)

| UTC log | Size | Outcome | What happened |
|---|---:|---|---|
| `artifacts/chaos/run-20261109T212657Z.log` | 25 KB | Failed before workload | Host port 5432 was occupied. Changed the default published PostgreSQL port to 55432. |
| `artifacts/chaos/run-20261109T212816Z.log` | 5.4 KB | Failed, interrupted | 200 submitted; terminal count stuck at 20 after all eight workers were killed. Compose did not replace explicitly killed containers. Added explicit replacement startup; retained the lease-based recovery behavior. |
| `artifacts/chaos/run-20261109T212956Z.log` | 4.9 KB | PASS diagnostic | 200 rows; 180 succeeded; 20 cancelled; 180 effects; 183 attempts; 5 kills; 67 duplicate POSTs; all invariant/error counters zero; 8.457 s. |
| `artifacts/chaos/run-20261109T213034Z.log` | 11 KB | **PASS required run** | 10,000 rows; 9,000 succeeded; 1,000 cancelled; 9,000 effects; 9,049 attempts; 50 kills; 3,334 duplicate POSTs; all invariant/error counters zero; 2m10.754s. |

The final raw summary is:

```json
{"run_id":"chaos-20261109T213036.869282000Z","requested_jobs":10000,"job_rows":10000,"queued":0,"running":0,"succeeded":9000,"failed":0,"cancelled":1000,"effects":9000,"jobs_with_duplicate_effects":0,"inconsistent_final_states":0,"worker_attempts":9049,"worker_sigkills":50,"duplicate_posts":3334,"request_errors":0,"duplicate_id_mismatches":0,"duration":"2m10.754s","verdict":"PASS"}
```

## What this does not catch

This is one seeded-by-runtime workload, not a statistical proof. It does not kill PostgreSQL or the API, inject network partitions/latency, fill the disk, corrupt data, test backup/restore or failover, or run long enough to expose connection/memory leaks. It does not measure p95/p99 HTTP latency, so 100 accepted requests/s is a throughput observation rather than an SLO result. Cancellation selection is deterministic and the workload has one payload shape. A process freeze is covered by the targeted zombie test, but the chaos runner kills rather than pauses workers. Database-host clock jumps are not injected. Most importantly, the effect is inside PostgreSQL; the harness cannot prove exactly-once behavior for an external model registry, object store, or GPU side effect. Security, multi-region behavior, schema upgrades, and operator error are out of scope.

## Verdict: DON'T SHIP at 100 requests/second

The run is positive evidence that this implementation sustained the offered 100 unique POSTs/s while processing work, retries, cancellations, and 50 worker deaths without violating its database invariants. It is not enough evidence for production: there is no latency distribution or soak result; PostgreSQL is a single point of failure; and the claimed exactly-once effect stops at the database boundary.

The first correctness fix is to design the real side-effect protocol: give the external sink an idempotency key/fencing token, or write a transactional outbox record and have the sink deduplicate by job ID. Then I would add metrics/tracing and a longer load/soak test with latency SLOs, followed by PostgreSQL HA/backup/restore and failure injection. Until the external effect and database failure modes are tested, `SHIP` would overstate what this evidence proves.
