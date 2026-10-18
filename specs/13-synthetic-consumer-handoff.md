# 13 — Durable synthetic consumer handoff

This is an Outfitter-owned local contract exercise for `cairn-synthetic-v1`, not a
production Cairn Runner, course admission, grading service, or native provider
qualification. Ben's assignments and the live criteria in issues #3–#5 stay open.
Docker remains reviewed-local with an explicitly acknowledged external watchdog;
host, watchdog and Docker-daemon loss are not covered. No network, mutable image,
native provider, retry/new-attempt API, or multi-step writable workspace is added.

## Pure preparation and durable identity

`consumer-preflight --request FILE` validates strict JSON without opening a ledger,
reading workload files, calling Docker or any provider. It returns the deterministic
request ID, binding SHA-256, finite CPU-second reservation, declared profile and
limitations. It proves only that the declared local contract is supported.

A consumer request supplies schema version 1, consumer, project_id, job_id,
attempt exactly 1, submission_commit (full lowercase Git object ID), policy_commit,
policy_manifest_sha256, sorted input_files and recipe_files (relative path, size,
SHA-256), plus a workload object using existing local request fields except that
request_id and project_id must be omitted/empty. The bridge derives those fields.
The key `(consumer, project_id, job_id, 1)` determines the request ID; the binding
includes that key, provenance, content manifests, image, command, limits, isolation
and supervised-reaper acknowledgement. Temporary input/recipe host paths are
excluded from the binding. File names and hashes are metadata; only synthetic,
reviewed content is allowed, not student identities, records or submissions.

Each manifest is explicit (empty array allowed), sorted and unique; at most 128
files combined, 256 bytes per relative path and 16 MiB combined content. Bare
`.`/`..`, absolute and parent-traversal paths fail preflight before admission. No links
or special files. Existing 32 KiB request and 96 KiB record limits also apply.
The supported consumer profile caps CPU at 1, RAM at 256 MiB, PIDs at 64, time at
30 seconds and output at 64 KiB. The one immutable image and command execute once,
with no writable input or recipe mount. Unlisted files, omitted files, changed
same-path bytes and mismatched sizes/hashes fail before provider creation. Actual
private snapshots are checked after copying, closing source changes between
preflight and snapshot; hashes are assertions of bytes, not proof of instructor
review or repository origin. Empty directories do not affect logical content.

## Single authority and one attempt

`consumer-run` uses the existing broker Run path, atomic budget reservation and
receipt store. The binding is committed with the reservation before any launch;
there is no second ledger. Version 4 is written only when admitting a consumer
request. Existing v1/v2/v3 records remain readable, with their previous hashes and
accounting unchanged; old binaries refuse version 4. Native account initialization
cannot downgrade version 4. Checkpoint/held restore/fenced activation retain the
binding and all unresolved liabilities in the existing authority snapshot.

An existing key with the same logical binding returns its receipt without reading
original paths or launching again, including concurrent, active and unknown work.
Changed binding conflicts, with no new reservation or provider effect. The returned
`reused` flag is determined by the atomic admission transaction. Budget refusal
has admitted:false and zero charge; it does not create a durable job. After any
admission, even a snapshot failure, this version does not offer another attempt.
Consumer admission permits only one active local placement in the store; ordinary
broker limits remain unchanged. Actual ambiguous provider responses retain their
reservation and never redispatch. No infrastructure retry loop is added.

`consumer-status --consumer cairn-synthetic-v1 --project-id ID --job-id ID` resolves
the persisted key without request files or original input paths. It is read-only
in active, held and fenced authority states, including missing diagnostics. Run is
refused in held/fenced state; explicit status is the recovery path. Receipt fields
are the existing lifecycle/compute fields plus consumer/job/attempt,
`binding_sha256`, `reused`, `reconciliation_required` and `grade_published:false`.
Active, unknown, unresolved or missing-diagnostic receipts require reconciliation;
status lookup never attempts it. Even a completed compute receipt is not a grade.
Only a separate explicit deterministic fixture check can establish expected output.

## Acceptance

Tests cover pure validation, content and policy/limit conflicts, duplicate and
concurrent replay, relocated/absent input paths, exhausted allowance before launch,
source replacement before copy, copied snapshot mismatch, missing diagnostics,
held/fenced status, recovery of unknown work, and mocked ambiguous provider outcomes
without redispatch. Bounded actual Docker fixtures check a correct and wrong Python
result, killed-consumer receipt lookup and independent cleanup. The harness runs
one container at a time with a finite total CPU-second budget and existing pinned
image, retains failed controls and declares local synthetic evidence only.

Live Jetstream2 testing remains blocked pending Greg's separate approval of a
completed manifest (operator/allocation, pins, rate evidence, independent cleanup
host/state access and teardown verification). This handoff cannot approve it.
