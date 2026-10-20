"""Independent Python authoring oracle for the v2 wire vectors; no Go imports.

Default: compare bytes with the frozen files. --write: create missing files,
refusing to overwrite a different prior vector. Never reads workload payloads,
starts processes, invokes engines, or allocates declared resource quantities.
"""

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROFILE = {"id": "cpu-python-r-bounded", "version": 1}
METHOD = {"method": "outfitter_normalized", "version": 1}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def manifest(maximum=False):
    # The maximum is constructed from the contract, not from Go output or a
    # modified default vector: twelve slots, 80-char identifiers, 512-char
    # image, instructional fields, full-width counters and optional receipt
    # metadata. All declared data sizes are scalars only.
    if maximum:
        assessment, project, environment = "a" * 80, "p" * 80, "e" * 80
        image = "r" * 440 + "@sha256:" + "f" * 64
        sizes, held, seeds = [999997, 999998, 999999], 1000000, [4294967293, 4294967294, 4294967295]
        reservations = [
            {"attempt_id": "a" * 77 + f"{n:03d}",
             "reservation_id": "r" * 77 + f"{n:03d}",
             "stage_id": "analysis", "sample_role": role,
             "sample_size": size, "repetition": rep}
            for n, (role, size, rep) in enumerate(
                [("fitting", size, rep) for size in sizes for rep in (1, 2, 3)] +
                [("held_out", held, rep) for rep in (1, 2, 3)], 1)
        ]
        instruction = {"enrollment": 100000, "simultaneous_sessions": 100000,
                       "assignment_count": 10000, "deadline_peak_sessions": 100000,
                       "per_student_storage_bytes": 1125899906842624}
    else:
        assessment, project, environment = "assessment-v2", "project-v2", "summary-python-v2"
        image = "python@sha256:" + "a" * 64
        sizes, held, seeds = [10000, 20000, 30000], 40000, [1, 2, 3]
        reservations = [{"attempt_id": "attempt-v2", "reservation_id": "reservation-v2",
                         "stage_id": "analysis", "sample_role": "fitting",
                         "sample_size": 10000, "repetition": 1}]
        instruction = None
    return {
        "schema": "educloud.benchmark-manifest/v2", "execution_profile": PROFILE,
        "assessment_id": assessment, "project_id": project,
        "pathway": "instructional" if maximum else "research",
        "data_classification": "synthetic",
        "workload": {"recipe_id": "summary-python", "recipe_sha256": "b" * 64,
                     "environment_id": environment, "image": image,
                     "stages": ["analysis"], "dependencies": []},
        "hardware": {"kind": "local_docker", "architecture": "amd64",
                     "cpu_count": 128 if maximum else 1,
                     "memory_bytes": 1125899906842624 if maximum else 1073741824,
                     "gpu_count": 0},
        "measurement": METHOD,
        "design": {"size_unit": "rows", "fitting_sizes": sizes,
                   "held_out_size": held, "repetitions": 3, "seeds": seeds,
                   "sampling": "synthetic_fixture", "exclusions": "declared" if maximum else "none",
                   "skew": "declared" if maximum else "unknown", "smaller_design": None},
        "quality": {"check": "reviewed_summary", "absolute_tolerance_nanounits": 1,
                    "relative_tolerance_parts_per_trillion": 1,
                    "maximum_failure_parts_per_million": 1000000 if maximum else 0},
        "ceilings": {"max_attempts": 12, "cpu_seconds": 21600, "cpu_count_per_attempt": 1,
                     "timeout_seconds_per_attempt": 1800, "memory_bytes": 1073741824,
                     "input_bytes": 5000000000, "scratch_bytes": 10000000000,
                     "input_rows": 1000000, "output_bytes": 65536},
        "expires_at": "9999-12-31T23:59:59Z" if maximum else "2026-10-21T00:00:00Z",
        "reservations": reservations, "instruction": instruction,
    }


def receipts(m, maximum=False):
    slot = m["reservations"][-1] if maximum else m["reservations"][0]
    binding = {**slot, "manifest_sha256": digest(m), "manifest_schema": m["schema"],
               "execution_profile": PROFILE, "assessment_id": m["assessment_id"],
               "project_id": m["project_id"], "environment_id": m["workload"]["environment_id"],
               "image": m["workload"]["image"], "hardware": m["hardware"],
               "measurement_method": METHOD}
    observation = {"source_kind": "unverified_observation" if maximum else "synthetic_fixture",
                   "authenticated": False, "independently_verified": False}
    cleanup = {
        "schema": "educloud.cleanup-receipt/v2", "receipt_id": "c" * 80 if maximum else "cleanup-v2",
        "binding": binding, "observation": observation,
        "observed_at": "2026-10-20T02:00:00Z" if maximum else None,
        "method": {"name": "unverified_scope_observation", "version": 1},
        "termination": "unknown", "access_revocation": "unknown",
        "observed_scopes": [
            {"kind": "execution_resource", "scope_id": slot["attempt_id"],
             "deletion": "unknown" if maximum else "reported_complete"},
            {"kind": "temporary_data", "scope_id": m["assessment_id"], "deletion": "unknown"},
            {"kind": "recovery_copies", "scope_id": m["assessment_id"], "deletion": "unknown"}],
        "verifier_id": None, "outcome": "unresolved", "quarantine": True, "reuse_denied": True,
        "remaining_liabilities": ["termination_unresolved", "access_revocation_unknown"] +
            (["execution_cleanup_unknown"] if maximum else []) +
            ["data_cleanup_unknown", "recovery_cleanup_unknown"] +
            (["accounting_unresolved"] if maximum else []),
        "prior_receipt_sha256": "d" * 64 if maximum else None,
    }
    units = {name: "nanoseconds" for name in (
        "cpu_consumed", "gpu_allocated_duration", "setup_duration", "loading_duration",
        "transfer_duration", "queue_duration", "execution_duration", "cleanup_duration",
        "sandbox_overhead", "instrumentation_overhead")}
    units.update({name: "bytes" for name in (
        "peak_memory", "scratch_peak", "output_peak", "io_read", "io_write", "gpu_peak_memory")})
    units.update({"gpu_utilization": "parts_per_million", "completed_units": "count"})
    metrics = {}
    for name, unit in units.items():
        available = maximum and not name.startswith("gpu_")
        metrics[name] = {
            "availability": "available" if available else "unsupported",
            "value": 9007199254740991 if available else None, "unit": unit,
            "scope": "sandbox" if name == "cpu_consumed" else
                     "execution_only" if name == "execution_duration" else "whole_attempt",
            "source": "unverified_collector" if available else "none"}
    measurement = {
        "schema": "educloud.measurement-receipt/v2", "binding": binding, "observation": observation,
        "execution": {"status": "failed" if maximum else "succeeded", "admitted": True,
                      "execution_started": not maximum, "cancel_requested": False,
                      "exit_code": None if maximum else 0,
                      "failure_category": "execution_failed" if maximum else "none",
                      "diagnostics_available": False, "reused": False},
        "measured_consumption": metrics,
        "reservation_ceiling": {"cpu_seconds": 1800, "source": "unverified_profile_reservation"},
        "conservative_accounting": {"charged_cpu_seconds": 0 if maximum else 1800,
                                    "provenance": "reserved" if maximum else "estimated_upper_bound",
                                    "unresolved_liability": maximum},
        "provider_reported_charge": {"availability": "unsupported", "value": None,
                                     "unit": "unknown", "source": "none"},
        "quality": {"status": "failed" if maximum else "unknown",
                    "source": "unverified_checker" if maximum else "none"},
        "cleanup_receipt_id": cleanup["receipt_id"], "cleanup_receipt_sha256": digest(cleanup),
    }
    return {"schema": "educloud.receipt-set/v2", "measurement": measurement, "cleanup": cleanup}


def vectors():
    files = {}
    sizes = {}
    for prefix, maximum in (("", False), ("maximum-", True), ("maximum-settled-", True),
                            ("unknown-held-", False), ("unknown-settled-", False)):
        m = manifest(maximum)
        s = receipts(m, maximum)
        if prefix == "maximum-settled-":
            # Standalone measurement and pair/result have different maxima.
            s["measurement"]["conservative_accounting"] = {
                "charged_cpu_seconds": 1800, "provenance": "estimated_upper_bound",
                "unresolved_liability": False}
            s["cleanup"]["observed_scopes"][0]["deletion"] = "reported_complete"
            s["cleanup"]["remaining_liabilities"] = [
                "termination_unresolved", "access_revocation_unknown",
                "data_cleanup_unknown", "recovery_cleanup_unknown"]
            s["measurement"]["cleanup_receipt_sha256"] = digest(s["cleanup"])
        if prefix.startswith("unknown-"):
            s["measurement"]["execution"].update(
                status="unknown", failure_category="unknown", exit_code=None)
            if prefix == "unknown-held-":
                s["measurement"]["conservative_accounting"] = {
                    "charged_cpu_seconds": 0, "provenance": "reserved", "unresolved_liability": True}
                s["cleanup"]["observed_scopes"][0]["deletion"] = "unknown"
                s["cleanup"]["remaining_liabilities"] = [
                    "termination_unresolved", "access_revocation_unknown", "execution_cleanup_unknown",
                    "data_cleanup_unknown", "recovery_cleanup_unknown", "accounting_unresolved"]
            s["measurement"]["cleanup_receipt_sha256"] = digest(s["cleanup"])
        result = {"schema": "outfitter.assessment-receipts-result/v2", "validation": "valid_unverified",
                  "manifest_sha256": digest(m), "measurement_sha256": digest(s["measurement"]),
                  "cleanup_sha256": digest(s["cleanup"]), "execution_authorized": False,
                  "cleanup_trusted": False, "actual_resource_accepted": False, "receipts": s}
        request = {"operation": "validate_v2", "manifest": m, "receipts": s}
        for name, value in (("benchmark-manifest", m), ("measurement-receipt", s["measurement"]),
                            ("cleanup-receipt", s["cleanup"]), ("receipt-set", s),
                            ("validate-request", request), ("validate-result", result)):
            key = prefix + name + ".canonical.json"
            files[key] = canonical(value)
            sizes[key] = len(files[key])

    # Small portable mutation recipes, not an unbounded collection of payloads.
    # Paths use object keys; raw replacements cover syntactically invalid JSON.
    cases = [
        {"name": name, "path": path, "value": value}
        for name, path, value in (
            ("input-plus-one", ["manifest", "ceilings", "input_bytes"], 5000000001),
            ("scratch-plus-one", ["manifest", "ceilings", "scratch_bytes"], 10000000001),
            ("memory-plus-one", ["manifest", "ceilings", "memory_bytes"], 1073741825),
            ("memory-wrong-five-gb", ["manifest", "ceilings", "memory_bytes"], 5000000000),
            ("partitions-overflow", ["manifest", "ceilings", "scratch_bytes"], 5000065535),
            ("aggregate-charge", ["manifest", "ceilings", "cpu_seconds"], 1800),
            ("profile-version", ["manifest", "execution_profile", "version"], 2),
            ("binding-profile", ["receipts", "measurement", "binding", "execution_profile", "version"], 2),
            ("charge-drop", ["receipts", "measurement", "conservative_accounting", "charged_cpu_seconds"], 1799),
            ("aggregate-per-attempt", ["receipts", "measurement", "reservation_ceiling", "cpu_seconds"], 21600),
            ("trusted-cleanup", ["receipts", "cleanup", "termination"], "verified"),
            ("authenticated", ["receipts", "measurement", "observation", "authenticated"], True),
            ("unknown-zero", ["receipts", "measurement", "measured_consumption", "cpu_consumed", "value"], 0),
            ("legacy-source", ["receipts", "measurement", "reservation_ceiling", "source"], "legacy_broker_receipt"),
            ("mixed-receipt", ["receipts", "cleanup", "schema"], "educloud.cleanup-receipt/v1"),
            ("old-operation", ["operation"], "validate"),
        )
    ]
    for token in ("true", "null", "1.0", "1e0", "NaN", "Infinity", "-1", "9007199254740992", "9223372036854775808"):
        cases.append({"name": "integer-" + token, "old": '"input_bytes":5000000000',
                      "new": '"input_bytes":' + token})
    cases += [
        {"name": "missing-ceiling", "old": '"input_bytes":5000000000,', "new": ""},
        {"name": "duplicate-ceiling", "old": '"input_bytes":5000000000',
         "new": '"input_bytes":1,"input_bytes":5000000000'},
        {"name": "unknown-profile", "old": '"id":"cpu-python-r-bounded"',
         "new": '"secret":"SECRET","id":"cpu-python-r-bounded"'},
    ]
    files["negative-cases.json"] = canonical(cases)
    files["sizes.json"] = canonical(sizes)
    files["digests.json"] = canonical({name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()})
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    for name, raw in vectors().items():
        path = ROOT / name
        if args.write and not path.exists():
            path.write_bytes(raw)
        if not path.exists() or path.read_bytes() != raw:
            raise SystemExit("frozen vector mismatch: " + name)
    print("v2 independent canonical vectors match")


if __name__ == "__main__":
    main()
