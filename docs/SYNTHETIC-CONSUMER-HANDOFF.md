# Synthetic consumer-job handoff

Outfitter now accepts a Cairn-shaped **synthetic fixture** with a durable one-attempt
job binding. This is not a production Cairn Runner or live coursework integration.
Outfitter returns compute evidence. A consumer must check the expected result
separately; every response explicitly says `grade_published:false`.

## Commands

Build `go build -o bin/outfitter ./cmd/outfitter`, then:

```sh
bin/outfitter consumer-preflight --request synthetic-job.json
bin/outfitter --state-dir /absolute/private/state project-init --project-id synthetic --budget-cpu-seconds 150
bin/outfitter --state-dir /absolute/private/state consumer-run --request synthetic-job.json
bin/outfitter --state-dir /absolute/private/state consumer-status --consumer cairn-synthetic-v1 --project-id synthetic --job-id fixture-1
```

Preflight only checks the declared contract. It does not inspect Docker, read the
workload, contact a provider, or open/create accounting state. JSON errors return
exit 2. A budget refusal returns an admitted:false compute receipt; held/fenced
admission returns exit 2, requiring an explicit status lookup for existing work.

Create the request from reviewed, synthetic local files. This illustrative schema
uses placeholders that must be replaced with actual content hashes and image:

```json
{
  "schema_version": 1,
  "consumer": "cairn-synthetic-v1",
  "project_id": "synthetic",
  "job_id": "fixture-1",
  "attempt": 1,
  "submission_commit": "FULL_LOWERCASE_GIT_OBJECT_ID",
  "policy_commit": "FULL_LOWERCASE_GIT_OBJECT_ID",
  "policy_manifest_sha256": "64_LOWERCASE_HEX",
  "input_files": [{"path": "value.txt", "size": 3, "sha256": "SHA256_OF_EXACT_42_NEWLINE_BYTES"}],
  "recipe_files": [{"path": "check.py", "size": 100, "sha256": "SHA256_OF_EXACT_RECIPE_BYTES"}],
  "workload": {
    "schema_version": 1,
    "image": "python@sha256:REPLACE_WITH_INSTALLED_IMAGE_DIGEST",
    "command": ["python", "/recipe/check.py", "/input/value.txt"],
    "input_dir": "/absolute/synthetic/input",
    "recipe_dir": "/absolute/synthetic/recipe",
    "cpus": 1,
    "memory_mb": 256,
    "pids_limit": 64,
    "timeout_seconds": 30,
    "max_output_bytes": 4096,
    "isolation": "reviewed-local",
    "allow_supervised_reaper": true
  }
}
```

`input_files` and `recipe_files` are explicit sorted arrays (including empty arrays),
with at most 128 files and 16 MiB combined. Paths are relative and unique, at most
256 bytes each. Symlinks, special files, extra files and missing files are refused.
The workload's request/project IDs are derived and must not be supplied. Networking
and writable input/workspace options are unsupported and rejected as unknown JSON.
There is one command and one immutable installed image, with no implicit pull.

The key is consumer/project/job/attempt=1. Changing host paths alone does not alter
the binding. Changing submission, policy, manifest, bytes, image, command or limits
does. Existing identical keys return the original receipt without touching the
input paths or dispatching, even while active or unknown. Conflicts fail. There is
no retry/new-attempt operation. A failed snapshot check is also a used attempt and
is conservatively charged by the existing broker; a timeout never refunds it.
The manifest is checked against the actual private copied snapshot before create,
so replacement at the original path cannot substitute different execution bytes.
Hashes do not prove that commits or policy were reviewed; the caller is trusted.

## Recovery and evidence

The response adds `binding_sha256`, `reused` and `reconciliation_required` to the
existing compute receipt. Status is ledger-only and works without original request
or input files in active, held and fenced states. Missing diagnostic bytes produce
`output_error` and require reconciliation without losing the binding or charges.
Unknown/active requests return their existing receipt and never automatically run
again. Use existing `cancel`, `reconcile`, and `reap` with its request_id for explicit
owned cleanup. Actual ambiguous create responses remain unresolved liabilities.

Version 4 retains consumer provenance in the same reservation authority and its
existing checkpoint/restore flow. Older binaries refuse the new version. Existing
local and native records retain their earlier hashes, allowances and accounting;
consumer admission cannot reset either. Input snapshots stay private and are not
copied into recovery checkpoints, which cannot replay them.

Run the bounded local harness with an already installed immutable image:

```sh
python3 scripts/verify_consumer.py --image python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36 --output .runtime/consumer-acceptance.json
```

It uses a cooperative `/tmp/educloud-docker-acceptance.lock`, one container at a
time, five 30 CPU-second reservations and a separate 1 CPU-second refusal account
(total configured allowance 151). Each job has 1 CPU, 256 MiB RAM, 64 PIDs and a
30-second deadline. It never pulls an image, restarts Docker, or broadly removes
resources. It retains synthetic files and failed controls under `.runtime/`.
The wrong Python answer intentionally fails the deterministic result check while
its compute receipt succeeds. This is evidence that successful compute is not a
grade. Mocked ambiguity, changed-policy conflicts, held/fenced status and recovery
are separately exercised by the Go suite. CI retains JSON evidence for its revision. The
[local acceptance record](evidence/2026-10-17-synthetic-consumer.json) retains
the correct/wrong controls and bounded Docker observations.

## Live decision remains separate

The existing [Jetstream2 proposal](manifests/jetstream2-live-test.proposed.json)
remains `PROPOSED_NOT_AUTHORIZED`. Greg must separately approve the completed
operator/allocation, provider pins, current rate/capacity evidence, independent
cleanup host/state access, finite workload/teardown and rescue plan. Its suggested
time window is not a scheduled run. This local bridge does not close those gaps or
qualify native execution, provider settlement, real course data, host-loss recovery,
or production Cairn retries. Ben's assignments and live acceptance in #3–#5 remain
unchanged. Belay, Portage and Cairn source/integration configuration are untouched.
