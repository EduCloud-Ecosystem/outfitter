# 09 — Reviewed local research broker (v1)

This executable slice supports the public/synthetic-data Waypoint research proof
of concept. It does not replace Ben's Jetstream2 milestones. It is a local CLI,
not an authentication service, cloud scheduler, monetary ledger, or arbitrary
code execution service. Inputs and recipe source must already be reviewed.

## Deliberate spec refinements and exceptions

Capabilities are explicit positive declarations with unknown capabilities refused;
this closes the unsafe extension default noted in the October 13 handoff. Docker
supports only `reviewed-local`, not T-standard/T-strong/T-batch. It declares no
self-expiry, CPU usage sampling, persistence, GPU, pause or multinode support.
Every request MUST explicitly set `allow_supervised_reaper: true`. The CLI starts
a detached watchdog before any Docker creation, and also provides an independent
`reap` command. This survives loss of the submitting broker process, but NOT loss
of the host/watchdog/Docker daemon. It is an explicit supervised experiment
exception under spec 01, not default provider conformance or cluster self-expiry.

The finite budget unit is reserved CPU-seconds: requested CPU ceiling × maximum
wall seconds. No currency, ACCESS credit, actual CPU usage, or invoice equivalence
is claimed. Until trustworthy usage sampling exists, successful/failing work
charges the full reservation as `estimated_upper_bound`. Unknown work/cleanup
keeps its whole reservation outstanding. No cancellation, restart, repeated
project-init or reconciliation increases the project allowance.

## Stable CLI handoff

Build `go build -o bin/outfitter ./cmd/outfitter`. All commands produce JSON:

```
outfitter --state-dir PRIVATE_PATH project-init --project-id ID --budget-cpu-seconds 300
outfitter --state-dir PRIVATE_PATH project-status --project-id ID
outfitter --state-dir PRIVATE_PATH run --request request.json
outfitter --state-dir PRIVATE_PATH status --request-id ID
outfitter --state-dir PRIVATE_PATH cancel --request-id ID
outfitter --state-dir PRIVATE_PATH reconcile --request-id ID
outfitter --state-dir PRIVATE_PATH reap
```

`run` is synchronous; duplicate identical requests return their persisted receipt,
never execute again. Collision of request ID with different normalized payload
is refused, including cross-project collision. `status` is ledger-only;
`reconcile` checks exact owned provider resources, preserving unknown execution
and conservative charges. `cancel` records cancellation before cleanup. Reaping
operates only on persisted IDs and verifies ownership labels before destruction.
It never removes an unrecorded resource or volume.

Request v1 (strict JSON, unknown fields refused):

```json
{"schema_version":1,"request_id":"run-1","project_id":"study-1",
 "image":"sha256:REPLACE_WITH_64_HEX_IMAGE_ID",
 "command":["python","/recipe/recipe.py","/input/data.csv","baseline"],
 "input_dir":"/absolute/inputs","recipe_dir":"/absolute/recipe",
 "cpus":1,"memory_mb":256,"pids_limit":64,"timeout_seconds":30,
 "max_output_bytes":65536,"isolation":"reviewed-local",
 "allow_supervised_reaper":true}
```

Image must be a local immutable sha256 image ID or repository digest; no implicit
pull. Admission caps CPU at 2, RAM at 1024 MiB, PIDs at 128, wall time at 300s,
combined stdout/stderr at 1 MiB, active jobs at 2 per project and 4 per store. A store admits at most 512 projects
and 512 lifetime request records. Encoded requests are capped at 32 KiB, images
at 512 bytes, input paths at 4096 bytes each and retained error text at 2048 bytes.
Compact authority records have a 96 KiB ceiling; the record/project admission
caps leave reserved terminal-state space below the 64 MiB reader bound. Reaching
capacity refuses new admission while existing cancellation/reaping can finish.
Input and recipe directories are copied into private immutable per-run snapshots
(maximum 16 MiB combined, no links/special files within snapshots). Container paths are `/input`
and `/recipe`; output is bounded stdout (one JSON result for Waypoint) and stderr.
There is no writable host output mount. `/tmp` is a private 16 MiB tmpfs.

Receipt includes `admitted`, `execution_started`, `schema_version`, request/project IDs, `status`, `exit_code`,
`error`, `stdout`, `stderr`, `cleanup_verified`, `unresolved_liability`, `image`,
`created_at`, `started_at`, `finished_at`, `deadline`, `cancel_requested`,
`compute:{reserved_cpu_seconds,charged_cpu_seconds,provenance}`, plus a statement
of the supervised-reaper exception. Status is reserved, creating, running,
succeeded, failed, cancelled, or unknown. Unknown is not silently retried.
Project usage includes budget, reserved, charged, remaining and unknown CPU-seconds.
Budget refusal returns a failed receipt with `admitted:false`, verified no creation,
zero compute and `not_admitted` provenance. Ledger errors/collisions return
`{"error":"..."}` with exit 2 because prior execution cannot be disproved.

## Durability and provider boundaries

Execution stdout/stderr are stored in separate private content-addressed diagnostic
files, referenced by SHA-256 and encoded length. The authority ledger does not
hydrate those files during accounting, cancellation, reconciliation or watchdog
reads. Missing/corrupt/unwritable diagnostics produce a visible `output_error` in
receipts, without preventing terminal authority updates. Receipts hydrate one
bounded output at a time. Accepted terminal receipts remain immutable under
concurrent cleanup.

Single local filesystem ledger uses process-shared advisory locking, validated
JSON, file fsync, atomic rename, and directory fsync. No shared/network filesystem;
one operator owns the private state directory. Project budgets are immutable. Recovered records must agree with their original
request IDs, project, image, payload hash and deterministic resource name. Only
zero-charge/full-reservation unresolved accounting or full-charge/verified-cleanup
settled accounting is accepted; partial charges and reattribution fail closed.
Admission commits a deterministic resource name and reservation before effects.
Missing/corrupt existing state fails closed. System parent paths are canonicalized;
the state directory itself cannot be a symlink. Lost Docker responses and uncertain
cleanup preserve liability. In particular, an absent container after an ambiguous
create response is provisional: later creation may still complete, so the request
remains unresolved and eligible for supervised reaping. This version has no
automatic settlement for an ambiguous create. Known-create cleanup failures can
settle only after verified teardown, charging the full ceiling. Private input snapshots remain for operator-controlled
recovery; no user records or payloads enter the accounting metadata.

Docker invokes argument arrays (no shell construction), disables networking and
restarts, sets non-root UID/GID 65532, drops all capabilities, enables
no-new-privileges, uses read-only rootfs and input mounts, sets CPU/RAM/PID/rlimit
ceilings (including 1024 open files for R compatibility), and disables Docker log persistence and image healthchecks. Images declaring
implicit writable volumes are refused. Bounded stdout/stderr capture kills
and cleans the container if exceeded. Cleanup requires matching ledger owner and
request labels and a fresh absent-resource observation. A failed provider query
is never absence. Container names cannot be reused for different requests.

## Acceptance boundaries

Unit/race tests: concurrent reservations, immutable budgets, idempotency and
collision, durable reopen, corrupt/missing state, positive capability refusal,
invalid limits/images/paths, ownership refusal, ambiguous creation/cleanup,
output/timeout/cancel and reconciliation. Actual Docker tests are opt-in and
report separately: pure estimate, reserved creation, running/status, declared
unsupported sampling, teardown, repeated teardown, nil/expired reservation,
isolation refusal, and broker SIGKILL with surviving watchdog. Host/watchdog-loss
self-expiry is explicitly unsupported and is not marked passed.

## Compatibility with the initial local pilot

Existing valid version-1 ledgers below 64 MiB remain readable. Their inline
stdout/stderr spill into hashed diagnostic files on the next authority update,
without resetting balances, request IDs, deadlines or retained results. A legacy
ledger already oversized or internally inconsistent is preserved and refused,
never silently reinitialized; operator-assisted recovery is required. The
per-record and record-count bounds do not replace a filesystem capacity/backup
policy for the diagnostic and input snapshots.
