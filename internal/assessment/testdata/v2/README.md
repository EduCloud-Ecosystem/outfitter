# V2 shared wire vectors

These are imported synthetic/unverified metadata, not workload measurements.
They do not authorize execution, attest cleanup, allocate storage, or accept a
resource. No payload bytes or row objects are present.

`build_vectors.py` is an independent Python authoring oracle constructed from
specification 22. It does not invoke Go or load Go-produced objects. Its default
mode checks all frozen bytes; `--write` creates only missing files and refuses
to replace differing existing evidence. Go tests load the immutable files and
compare complete canonical objects and the actual CLI response, rather than
regenerating expected results through the implementation under test.

The ordinary vector is a settled synthetic attempt at the maximum scalar
resource ceilings. The `maximum-` vector independently fills all 12 declared
slots, maximum 80-byte identifiers and 512-byte image, instructional fields,
full-width hardware/seed/metric integers, and nullable historical timestamp/
digest metadata. Its failed attempt retains a full unresolved reservation.
`execution_failed` is the longest accepted failure category; false execution
started plus null exit code is two bytes longer than true plus exit code 255.
The first source draft used OOM/true/255, undercounting five bytes in total.
That draft was corrected before vector generation or testing; it is not a
runtime failure. OOM remains a separate lifecycle control.
The extreme counter values exercise encoding bounds and are not plausible
measured utilization or results from an actual attempt.

`sizes.json` records exact encoded lengths. The maximum uses the longest valid
strings. `maximum-settled-` supplies the separate largest standalone measurement:
its charge/provenance are longer, but its cleanup has fewer liabilities, so the
full-hold form still has the larger receipt-set/result. The first actual neighbor
test exposed that distinction; its failed log and original size/digest indices
are retained in the author evidence. Tests compare each object against the
larger of both independently prescribed witnesses. Both forms use the longest
strings, all 12 slots, longest optional metadata and available non-GPU metrics.
IDs/images admit no JSON-escaping characters. Missing liabilities make deletion
reported_complete shorter overall than unknown, despite the longer token.
Unresolved accounting retains two extra liability strings compared with a
settled execution and outweighs its shorter charge/provenance. Smaller designs
lose whole reservation entries, exceeding their annotation size. Go controls
compare accepted neighboring lifecycle, design, metric and nullable forms with
these independently prescribed maximum bytes. All remain inside unchanged
64 KiB manifest, 128 KiB receipt-set, and 256 KiB result bounds.
`digests.json` binds every generated artifact except itself. JSON files
use sorted keys and no whitespace or trailing newline.

`negative-cases.json` contains bounded portable request mutations: object-key
paths with replacement values, or exact raw replacements for malformed JSON.
Every case must reject with no partial success. Go's additional typed controls
rehash modified cleanup receipts so semantic refusal cannot be accidentally
credited to a stale digest. They also test version isolation, full holds,
metric scope preservation, and old-control refusal.

V1 vectors in the parent directory and owned-data fixtures are frozen. No old
parser is broadened, and no legacy preview or accepted PR51 digest is replaced.
Waypoint's old v1 path must continue to reject this schema until its distinct
reviewed v2 implementation consumes these exact vectors after AS1 acceptance.

The `unknown-held-` and `unknown-settled-` vectors preserve unknown execution
and quality with no exit code. The latter describes full conservative CPU
accounting settlement after reported execution-resource cleanup. It does not
resolve data/recovery/termination/access liabilities or attest an actual journal
transition. Its reservation ceiling is the original bound, not a second hold.
These forms preserve accepted broker semantics; the earlier source-only design
that required every unknown outcome to hold CPU accounting was corrected in a
separate commit. All earlier test logs and generated indices remain historical.
