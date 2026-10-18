# 18 Ledger serialization before diagnostic effects

This is the narrow runtime follow-up to accepted qualification PR #15 (merge
`e5599066970750ff3aa8f539bf0391f9c7e32346`). It changes only ledger/output
preparation within the existing transaction. Waypoint's active broker pin remains
`5827c84e9952cc67b85e91ceff4282e0ead6923b`; adoption requires separate review.
Whole-root capacity remains UNKNOWN and quota enforcement false.

## Problem and contract

`Store.write` currently persists diagnostic sidecars before serializing compact
records and the full ledger. A serialization or metadata-capacity refusal can
therefore leave an orphan output despite publishing no authority change. A
separate filesystem error after sidecar publication can still do that after this
change; this slice makes no promise to remove all orphans or bound global retries.

Under the existing ledger lock and authority/path validation:

1. Prepare each diagnostic output with the actual Go JSON encoder, checking its
   existing 6 MiB + 512 encoded-byte limit, digest and length. Retain its source
   strings and small metadata only. Release each encoded buffer before preparing
   another; never keep an array of up to 512 encoded 6 MiB outputs.
2. Construct compact success and diagnostic-failure forms for each affected
   record. Both clear inline stdout/stderr. Success records exact digest/bytes and
   clears OutputError; failure clears digest/bytes and records the existing fixed
   diagnostic error. If output preparation itself fails, only the failure form is
   eligible. All compute, identity, native, consumer and recovery fields survive.
3. Validate both complete success/failure ledgers and each compact record's actual
   encoded bytes against the existing 96 KiB limit. For every record select the
   longer serialized form; serialize this maximum mixed ledger, including native
   accounts/runs and all other fields, against the existing 64 MiB limit. JSON
   object overhead and unchanged fields are identical across choices. Only local
   diagnostic fields vary and their validation is per-record; aggregate accounting
   is unchanged, so every mixed persistence outcome is covered. Do not enumerate
   2^N combinations or assume an all-success ledger always has the largest size.
4. Only after preparation succeeds, encode and persist one planned output at a
   time. Match digest/length before any sidecar effect. Choose that record's
   prevalidated success/failure form from the actual I/O result. Diagnostic failure
   still permits conservative accounting publication and cannot refund unknown
   compute, reset native allowances, change consumer identity or suppress failure.
5. Serialize and publish the resulting compact ledger with the existing temp,
   file-sync, rename and directory-sync sequence. Preserve defensive validation.
   No file is created by a preflight serialization/capacity refusal. Existing files
   and unknown entries remain untouched; the existing transaction's lock file and
   prior authority are not counted as new preflight effects.

No new persistent plan, capacity ledger, configurable limit, quota admission,
cleanup behavior, provider default, snapshot/watchdog/recovery protocol or schema
version is introduced. The current native experiment guards and main's native
and consumer bindings remain in force. This is logical serialization preparation,
not a physical or Go-heap capacity guarantee; existing raw inputs and compact
metadata also consume memory.

## Verification and limits

Use disposable real filesystem fixtures and fake provider work only. Independently
derive expected encoded output digests before submitting mutations. Verify:

- Actual Go serializer failure after logically valid metadata, and aggregate
  serialized-capacity refusal, leave old ledger bytes, sidecars and unknown files
  unchanged. Retain an observed pre-fix failure as evidence.
- Every success/failure combination for several outputs preserves the correct
  reference/error and full conservative charges or unresolved reservations.
  Cause individual output publication failures using owned fixture obstructions;
  do not require production fault hooks or external services.
- Go escaping amplification, exact record/ledger boundary comparisons and largest
  mixed form selection. Lowered private test limits, if needed, are labeled as
  such and do not imply ordinary CLI reachability of the 96 KiB production cap.
- Preparation retains no encoded diagnostic buffers; sequential publication
  verifies each planned digest/length. Do not claim a measured physical heap cap.
- Native/consumer fields, migration, held/fenced authority, existing diagnostic
  failure behavior and immutable experiment guards retain their existing checks.

Retain PR #15's original source-pinned observations, failed model controls and
Waypoint's fifth headroom failure byte-for-byte. Later output/ledger I/O faults
may still leave orphans, uncertain durability or an active destination after an
activation error. No new writer census/fence or complete storage envelope is proved.
Live Jetstream2 retains its engineering, operational and separate user-approval
gates. Independent review and exact-head CI are required before a separate merge.
