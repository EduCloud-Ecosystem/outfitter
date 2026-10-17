# Jetstream2 preflight and adapter integration handoff

The implemented command performs authentication and read-only resource checks:

```
go build -o bin/outfitter ./cmd/outfitter
bin/outfitter jetstream2-preflight --config PRIVATE_CONFIG.json
```

Use the nonsecret pinned configuration in [spec 10](../specs/10-jetstream2-preflight-and-worker-api.md).
Provide allocation-specific `OS_APPLICATION_CREDENTIAL_ID` and
`OS_APPLICATION_CREDENTIAL_SECRET` through the operator's environment. Do not put
credentials in configuration, screenshots, test fixtures or reports. The command
never prints identity tokens, credentials or upstream response bodies. It refuses
HTTP endpoints, redirects, project mismatches and missing/ambiguous regional
service endpoints. No broad credential discovery is performed.

The report separates passing resource checks from `provisioning_enabled:false`.
Image/quota/resource failures emit that report with a nonzero command exit.
It reports allocation balance, resource entitlement, host capacity, dated native
SU rates, durable native-unit accounting and independent expiry as unknown or
unaccepted. A quota check is not proof of an available worker. Image UUID plus
sha256/sha512 checksum must match; mutable image names are not accepted.

The `internal/provider/jetstream2` API includes reservation-gated creation of one
explicitly owned Neutron port followed by an image-backed Nova server, inspection,
and verified owned deletion. It requests no floating IP, volume, snapshot or new
network. Its `Journal` contract requires durable atomic native-unit reservation
and supervisor approval before any resource POST. The shipped executable does
not supply that integration and exposes no provisioning command. The in-memory
journal used by tests is mock evidence only.

A lost create response is unknown and is never retried automatically. Persisted
resource IDs and provider ownership must agree before deletion. Unknown server
creation requires exact-resource reconciliation before teardown can be accepted.
Verified deletion retains `reserved_unsettled` native-unit accounting and the
liability; no disappearance or guest shutdown becomes an assumed zero charge.

## Verified here

`go test -race ./...` and `go vet ./...` passed locally. TLS-backed simulated
OpenStack tests cover read-only preflight; region/project/authentication checks;
redirect refusal; image checksum and quota failures; missing reservation or
supervisor; create/status/delete and repeated teardown; lost-create response;
foreign/unrecorded resource refusal; delayed journal/port deadlines that cannot
start later creation; and retained native-unit liabilities.
No live authentication, resource entitlement, allocation balance, capacity,
creation, teardown or expiry test was performed.

## Next integration gates

1. Implement the durable native-SU journal and dated approved flavor rate without
   mixing it with the local CPU-second allowance. Persist unknown charges and
   resource IDs; prove cross-process admission and recovery.
2. Bind an independent cleanup supervisor with its own failure domain, durable
   credentials outside guests, bounded reaping and operator alerts. Demonstrate
   broker-loss behavior; record any supervised exception explicitly.
3. Run read-only preflight against the selected approved allocation and record
   project/region, image/flavor, network/security group, operator, finite test SU
   ceiling and maximum one concurrent worker. Confirm CPU-only suitability and
   operator access to the image. No allocation is assumed from historical notes.
4. Only after approval of that concrete bounded live configuration, execute the
   disposable worker lifecycle, failure/cleanup acceptance and separate
   Waypoint/Cairn handoff. The local research workspace does not depend on it.

The next increment is documented in [native accounting and cleanup](JETSTREAM2-NATIVE-ACCOUNTING.md). Its durable journal and cleanup-only CLI have simulated-provider tests; live creation, allocation use and independent-host qualification remain separate gates.
