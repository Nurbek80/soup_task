# Raw chaos output

These files are append-only run captures produced by `scripts/chaos.sh`. Failed runs are intentionally retained.

- `run-20261109T212657Z.log`: failed startup, host port 5432 occupied.
- `run-20261109T212816Z.log`: failed diagnostic, killed Compose workers were not automatically replaced; interrupted after terminal progress stayed at 20/200 (`exit_code=141`).
- `run-20261109T212956Z.log`: passing 200-job diagnostic after explicit replacement was added.
- `run-20261109T213034Z.log`: passing required 10,000-job run.

See `../../CHAOS.md` for the summaries, limitations, and verdict. Logs include build/start output so the execution environment and failures are auditable.
