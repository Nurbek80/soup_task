# AI tool use

I used OpenAI Codex to extract and visually verify the one-page task PDF, propose the initial Go/PostgreSQL structure, write implementation and documentation drafts, run commands, and interpret test/chaos output.

I accepted the suggestions to use PostgreSQL `FOR UPDATE SKIP LOCKED`, random lease-token fencing, database time, a unique idempotency key plus request hash, and an atomic database effect/final-state transaction. I also accepted the split between targeted concurrency tests and a larger process-kill harness.

I did not treat generated code or a green compile as evidence. I verified it with `go test`, `go vet`, PostgreSQL-backed race tests, direct invariant queries, and preserved chaos logs. Two generated assumptions were wrong and were corrected from evidence: the first claim query had an ambiguous unqualified `RETURNING id`, which the database integration tests caught; and a Compose restart policy did not replace explicitly killed workers, which the first kill-enabled run exposed. I qualified the SQL fields, made the harness explicitly start replacements, reran the tests, then completed the full 10,000-job run. I also changed the generated host database port after the first raw run showed a collision. The production verdict and the limitation that exactly-once holds only for a co-transactional database effect were kept deliberately conservative.
