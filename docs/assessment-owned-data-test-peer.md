# Disabled assessment test peer

The production binary always refuses `owned-data prepare`, `ingest`, and `seal`
from the command position, before stdin or state access. There is no production
boundary factory, enabling option, v6 initializer, or migration. This document
describes disposable synthetic integration tests only; it grants no upload,
execution, allocation, host qualification, or trusted cleanup authority.

The Go test binaries contain the test-only constructors:

```sh
go test -c -o ledger.test ./internal/ledger
go test -c -o broker.test ./internal/broker
```

`ledger.test -test.run=^TestOwnedFixtureSeedHelper$` with
`OUTFITTER_OWNED_TEST_SEED` naming a **new** private directory constructs a fresh
v6 fixture through the real serializer. It records the initialized tombstone
before the journal and seeds paired project metadata with preexisting charged
and unresolved compute/native records. These synthetic amounts never authorize
compute and remain unchanged by the assessment lifecycle.

The one-shot peer command is `broker.test -test.run=^TestOwnedPeer$ -- OPERATION`.
Its test-only environment is:

| Variable | Meaning |
| --- | --- |
| `OUTFITTER_OWNED_TEST_HELPER` | Exact value `1`; selects the test entry point |
| `OUTFITTER_OWNED_TEST_STATE` | Fresh fixture state directory |
| `OUTFITTER_OWNED_TEST_ROOT` | Separate synthetic content directory |
| `OUTFITTER_OWNED_TEST_CLOCK` | Shared synthetic clock JSON file |
| `OUTFITTER_OWNED_TEST_WATCH_PID` | Optional private test observation path; never ownership proof |
| `OUTFITTER_OWNED_TEST_WATCH_EXIT` | Optional private test observation path; never ownership proof |

The clock JSON has `Wall` (UTC timestamp), `TickMS` (integer) and `Epoch`
(32 lowercase hex). The fixture starts at `2026-10-19T12:00:00Z`, tick `1000`,
epoch `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`. Its injected policy expires at 13:00
and caps the declared upload at 1024 bytes. Those are fixed synthetic test
values, not operating limits for a selected host.

Descriptor positions are fixed trusted launch parameters, never JSON fields:

| Operation | FD 3 | FD 4 | FD 5 |
| --- | --- | --- | --- |
| prepare | owner read | watcher read | watcher write |
| ingest / seal | owner read | watcher read | unused |
| internal watch child | owner read | watcher write | readiness write |

The owner retains the sole owner-write end and closes its watcher-write copy
immediately after prepare starts. Prepare verifies a private bounded pipe-pair
challenge, forwards the sole watcher-write end, waits for the exact readiness
reply and EOF, then exits. The watch child invokes the real `Broker.Watch`;
later one-shot commands call the real `Broker.OwnedData`. Descriptor identities
are bound separately because opposite pipe endpoints may have different inodes.
No readiness file, PID, imported reply or cached epoch can authorize a writer.

Anonymous lifelines require a positive platform check on the opened descriptor.
Linux requires kernel `pipefs` (`Fstatfs`, magic `0x50495045`) and one link;
Darwin requires zero links and a successful, exact-size self
`PROC_PIDFDPIPEINFO` result (184-byte `pipe_fdinfo`, kernel `DTYPE_PIPE`).
Named, renamed and unlinked filesystem FIFOs are refused. Unsupported platforms
and unavailable or incomplete descriptor queries fail closed. These checks
preserve endpoint access direction, bound identity and the pipe-pair challenge;
regular private files still require exactly one link.

Closed fixtures are in `internal/owneddata/testdata`. Non-ingest requests are
JSON plus EOF. Ingest is one JSON header line, LF, exactly `content_length`
bytes, then EOF. A caller closes broker stdin when its framed request ends.
The parser does not prefetch body bytes. Replies are canonical sorted JSON plus
one LF, with empty stderr: status exits 0, fixed `ASSESSMENT_*` error exits 2.
The private watcher ACK uses the same framing and ends with EOF.

Failure tests use additional test-only fault/crash/transition environment
variables defined in `owned_process_test.go`. They are absent from production
construction and must not be interpreted as configuration for a live service.
All synthetic cleanup stays `cleanup_unresolved`, with public trust and
resource-acceptance flags false. Close uncertainty, unknown ownership, missing
leases and unavailable journal publication retain holds and prior observations.

The peer, fixtures and lifecycle require independent coordinator review before
connected Waypoint tests. Live Jetstream2 and real uploads remain blocked.
