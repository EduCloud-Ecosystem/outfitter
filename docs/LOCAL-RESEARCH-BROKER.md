# Local research broker operator handoff

This is the first executable Docker slice for reviewed recipes and public or
synthetic inputs. It is a local operator CLI, not a remote service or authorization
boundary. Waypoint owns identities, project membership and scientific records.

## Build and run

Go 1.24 or later and a running Docker daemon are required. Tested locally with Go
1.26.4 and Docker 29.8.2. Build with `go build -o bin/outfitter ./cmd/outfitter`.
Use the CLI and request JSON in [spec 09](../specs/09-reviewed-local-research-broker.md).
The immutable image must already exist locally; no pull happens during admission.
Python and R commands are argument arrays. Both emit one JSON result on stdout;
Waypoint validates it independently and saves the resulting artifact. R requires
the bounded 1024-file descriptor limit used by the adapter.

Each project-init creates an immutable CPU-second allowance. Identical repeats
return current usage; changing its allowance is refused. Store this private
state on one local filesystem with mode 0700. It contains no identity tokens,
but does contain private input/recipe snapshots and bounded execution output;
protect it accordingly. Accounted CPU-seconds are ceiling estimates, not measured
CPU consumption, actual costs or grant service units.

The CLI starts a detached watchdog before creating any container. The watchdog
uses persisted exact resource names and ownership labels. Keep its executable and
ledger available until all outstanding work has been reconciled. For independent
supervision run `outfitter --state-dir PRIVATE_PATH reap` periodically under the
operator's service manager. The interval must fit the intended cleanup margin.
This is an explicit supervised exception: host, watchdog or Docker-daemon loss
can defeat the timeout. No default provider-self-expiry acceptance is claimed.

## Failure and recovery

- A duplicate identical request returns its original status and never reruns.
  Payload collision refuses execution, including a collision across projects.
- Use `status` for durable accounting status. `cancel` records intent before owned
  cleanup. `reconcile` independently observes cleanup and charges the full
  reservation; it never refunds uncertain execution.
- If the creation response was never recorded, a later absence is provisional:
  retain the liability and investigate the daemon/client outcome before recovery.
  The watchdog continues owned cleanup attempts rather than recreating work.
- A daemon/query failure is unknown, never absence. No command sweeps unrecorded
  containers, shared networks or volumes. Removal requires matching owner and
  request labels. The image cannot declare anonymous writable volumes.
- Do not delete or restore an older ledger to reset allowances. Restore current
  accounting and preserve unresolved liabilities before reopening admission.
- Snapshot retention is operator controlled in this proof of concept. Per-request
  input/output bounds do not provide a store-wide disk quota, backup policy or
  retention workflow. Those remain institutional deployment work.

## Verification

`go test -race ./...` and `go vet ./...` cover admission bounds, concurrent
reservations, durable reopen, immutable budgets, corrupt/missing state, symlinks,
unknown creation, cleanup failures, cancellation, timeout and output bounds.

Actual adapter conformance is opt-in:

```
OUTFITTER_DOCKER_IMAGE=sha256:PINNED_LOCAL_PYTHON_IMAGE go test -v ./internal/provider/docker
python3 scripts/verify_docker.py --image sha256:PINNED_LOCAL_PYTHON_IMAGE
```

The real-Docker script records independent expected output, non-root execution,
containment configuration, deduplication, output and wall-time bounds, explicit
cancellation, broker SIGKILL with watchdog cleanup, fresh absence observation,
budget refusal and separate-process reservation races. It creates and removes
only its recorded owned containers. It retains private ignored accounting state.

[October 17 actual results](DOCKER-ACCEPTANCE-2026-10-17.json) are synthetic local
acceptance evidence, not an institutional hosting, gVisor/Kata, Jetstream2,
financial accounting, real-student-data or scientific-validity claim. The
provider self-expiry test is explicitly skipped as unsupported.
