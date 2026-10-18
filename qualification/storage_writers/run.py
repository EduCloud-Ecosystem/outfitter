#!/usr/bin/env python3
"""Export a fixed broker commit and run declared disposable witness overlays.

No accepted-state argument. No Docker/cloud commands. Exclusive output directory
retains reports, original failure logs, source hashes and exact instrumentation.
Only the temporary export and fixtures owned by this run are removed.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

from finite_model import exercise

HERE = Path(__file__).resolve().parent
PIN = "5827c84e9952cc67b85e91ceff4282e0ead6923b"
MAIN = "e9c18f970bae38aef7254b90f1465dadd64673d5"
WAYPOINT = "e543ca4e8a388bb71fe96e11e12b6765829e4a29"
CONTRACT_SHA = "4e87eb642897e30cda868e361672c8a499328395c0b8506c9e7c6e87f0b5dbc2"
FAILURE_SHA = "763ec3e001ef50512fca925efe24b2f6878141f7b448b2ecb495a5b2b7db2957"
EXPECTED = {
    "repeated-spill-before-ledger-failure", "spill-before-lowered-compact-limit",
    "encoded-output-and-same-digest-overlap", "legacy-open-is-writer",
    "checkpoint-reference-only-under-exclusive-execution-lease",
    "checkpoint-corrupt-reference-retains-partial", "activation-error-can-leave-destination-active",
    "snapshot-entry-partial", "snapshot-growth-retains-probe-byte",
    "snapshot-cancellation-is-not-writer-completion",
    "watch-ready-outside-locks-and-cancel-blocked", "duplicate-watch-shared-ready",
    "requester-exit-does-not-stop-watch-writes", "settled-cutover-does-not-fence-watch-ready",
} | {f"publication-{family}-{phase}" for family in ("ledger", "output")
     for phase in ("file-sync", "rename", "directory-sync")}


def digest(b):
    return hashlib.sha256(b).hexdigest()


def command(args, cwd=None, env=None, timeout=60):
    return subprocess.run(args, cwd=cwd, env=env, check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout).stdout


def instrument(root):
    patches = []
    def replace(name, old, new, count=1):
        path = root / name
        content = path.read_text()
        actual = content.count(old)
        if actual != count:
            raise ValueError(f"patch anchor {name}: expected {count}, got {actual}")
        path.write_text(content.replace(old, new))
        patches.append({"file": name, "before": old, "after": new, "occurrences": count})
    store = "internal/ledger/store.go"
    broker = "internal/broker/broker.go"
    for name in (store, broker):
        old = '\t"github.com/EduCloud-Ecosystem/outfitter/internal/provider"'
        replace(name, old, old+'\n\t"github.com/EduCloud-Ecosystem/outfitter/internal/qualification"')
    replace(store, "f.Sync()", "qualification.Sync(f)", 3)
    replace(store, "dir.Sync()", "qualification.Sync(dir)")
    replace(store, "directory.Sync()", "qualification.Sync(directory)")
    replace(store, "os.Rename(", "qualification.Rename(", 2)
    replace(store, "len(encoded) > maxRecordBytes", "len(encoded) > qualification.RecordLimit(maxRecordBytes)")
    old = "\tif e = syscall.Flock(fd, syscall.LOCK_EX); e != nil {"
    replace(store, old, '\tif e = qualification.Gate("ledger-before-lock", s.Dir); e != nil { return e }\n'+old)
    old = "\t\t\tfd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)"
    replace(broker, old, '\t\t\tif e := qualification.Gate("snapshot-after-stat", path); e != nil { return e }\n'+old)
    old = '\tready := filepath.Join(b.Store.Dir, "watch-"+id+".ready")'
    replace(broker, old, '\tif e = qualification.Gate("watch-before-ready", b.Store.Dir); e != nil { return e }\n'+old)
    old = '\tdefer os.Remove(ready)'
    replace(broker, old, old+'\n\tif e = qualification.Gate("watch-after-ready", b.Store.Dir); e != nil { return e }')
    return patches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=HERE.parents[1], help="Git object source only")
    parser.add_argument("--output", type=Path, required=True, help="new exclusive evidence directory")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {"format": "outfitter-storage-writer-qualification-v1", "qualification_status": "NOT_RUN",
              "active_broker_pin": PIN, "main_comparison_pin": MAIN, "waypoint_contract_head": WAYPOINT,
              "quota_enforced": False, "whole_root_envelope": "UNKNOWN", "available_capacity": "UNKNOWN",
              "all_writers_complete": False, "runtime_changed": False,
              "preserved_fifth_failure": "FAIL/BLOCKER_REPRODUCED in upstream evidence; not repaired here",
              "not_run": ["physical ENOSPC/inode exhaustion", "host crash or power loss", "failed temp removal and retained scratch accumulation", "actual CLI watchdog launcher",
                          "Docker daemon/provider", "live Jetstream2", "complete independent writer discovery",
                          "enforced destination peak refusal", "production quota enforcement"],
              "witnesses": []}
    try:
        if sys.platform not in ("darwin", "linux"):
            raise RuntimeError("NOT_RUN: supported Unix flock/process semantics required")
        contract_bytes = (HERE / "upstream/waypoint-contract-v1.json").read_bytes()
        if digest(contract_bytes) != CONTRACT_SHA:
            raise ValueError("accepted contract bytes changed")
        contract = json.loads(contract_bytes)
        if contract["sources"]["outfitter"]["commit"] != PIN or len(contract["writers"]) != 17:
            raise ValueError("contract pin/row drift")
        if digest((HERE / "upstream/fifth-headroom-failure.json").read_bytes()) != FAILURE_SHA:
            raise ValueError("preserved failure drift")
        report["contract_sha256"] = digest(contract_bytes)
        report["preserved_failure_sha256"] = FAILURE_SHA
        report["qualification_source_hashes"] = {str(p.relative_to(HERE)): digest(p.read_bytes())
            for p in sorted(HERE.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}
        archive = command(["git", "-C", str(args.repository), "archive", "--format=tar", PIN])
        report["active_source_archive_sha256"] = digest(archive)
        with tempfile.TemporaryDirectory(prefix="outfitter-storage-witness-") as temporary:
            root = Path(temporary) / "source"
            root.mkdir()
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                # This fixed repository has only files/directories. Reject links,
                # absolute paths and parent traversals rather than trust archive names.
                for item in tar.getmembers():
                    if not (item.isfile() or item.isdir()) or Path(item.name).is_absolute() or ".." in Path(item.name).parts:
                        raise ValueError("unsafe source archive entry")
                tar.extractall(root)
            verified = {}
            for name, expected in contract["sources"]["outfitter"]["sha256"].items():
                actual = digest((root/name).read_bytes())
                if actual != expected:
                    raise ValueError(f"pinned source hash mismatch: {name}")
                verified[name] = actual
            report["verified_active_source_hashes"] = verified
            report["main_source_comparison"] = {}
            for name in verified:
                b = command(["git", "-C", str(args.repository), "show", f"{MAIN}:{name}"])
                report["main_source_comparison"][name] = {"sha256": digest(b), "same_as_active": digest(b) == verified[name]}
            for src in sorted((HERE/"overlays").rglob("*.go")):
                dst = root/src.relative_to(HERE/"overlays")
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    raise ValueError("overlay would replace existing source")
                shutil.copyfile(src, dst)
            patches = instrument(root)
            (args.output/"instrumentation.json").write_text(json.dumps(patches, indent=2)+"\n")
            go = shutil.which("go")
            if not go:
                raise RuntimeError("NOT_RUN: Go toolchain unavailable")
            for name in ("home", "tmp", "cache"):
                (Path(temporary)/name).mkdir()
            env = {"PATH": os.environ.get("PATH", ""), "HOME": str(Path(temporary)/"home"),
                   "TMPDIR": str(Path(temporary)/"tmp"), "GOCACHE": str(Path(temporary)/"cache"),
                   "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local", "CGO_ENABLED": "1"}
            command(["gofmt", "-w", "internal/ledger/store.go", "internal/broker/broker.go", "internal/qualification"], cwd=root, env=env)
            report["instrumented_source_hashes"] = {name: digest((root/name).read_bytes()) for name in ("internal/ledger/store.go", "internal/broker/broker.go", "internal/qualification/hooks.go")}
            report["toolchain"] = command([go, "version"], env=env).decode().strip()
            args_go = [go, "test", "-race", "-json", "-count=1", "-timeout=90s", "./internal/broker", "./internal/ledger", "-run", "^(TestStorageWriter|TestRecoveryCrashAfterFenceLeavesBothNonAdmittingAndRetryable$)"]
            report["command"] = ["go", *args_go[1:]]
            started = time.monotonic()
            with (args.output/"go-tests.jsonl").open("wb") as log, (args.output/"go-stderr.txt").open("wb") as err:
                run = subprocess.run(args_go, cwd=root, env=env, stdout=log, stderr=err, timeout=240)
            report["duration_seconds"] = round(time.monotonic()-started, 3)
            report["exit_code"] = run.returncode
            passed, skipped, failed = [], [], []
            for line in (args.output/"go-tests.jsonl").read_text().splitlines():
                event = json.loads(line)
                if event.get("Test") and event["Action"] in ("pass", "skip", "fail"):
                    {"pass": passed, "skip": skipped, "fail": failed}[event["Action"]].append(event["Test"])
                output = event.get("Output", "")
                if "WITNESS " in output:
                    report["witnesses"].append(json.loads(output.split("WITNESS ", 1)[1]))
            report["go_test_counts"] = {"pass": len(passed), "skip": len(skipped), "fail": len(failed)}
            ids = [w["id"] for w in report["witnesses"]]
            if run.returncode or failed or skipped or set(ids) != EXPECTED or len(ids) != len(EXPECTED):
                raise RuntimeError(f"witness execution incomplete; missing={sorted(EXPECTED-set(ids))}, skipped={skipped}, failed={failed}")
            report["candidate_model"] = exercise()
            report["qualification_status"] = "PASS_COUNTEREXAMPLES_REPRODUCED"
    except Exception as exc:
        report["qualification_status"] = "FAILED_OR_NOT_RUN"
        report["error"] = str(exc)
    finally:
        report["artifact_hashes"] = {p.name: digest(p.read_bytes()) for p in sorted(args.output.iterdir()) if p.is_file()}
        (args.output/"report.json").write_text(json.dumps(report, indent=2, sort_keys=True)+"\n")
    print(json.dumps({"status": report["qualification_status"], "witnesses": len(report["witnesses"]), "output": str(args.output), "error": report.get("error")}))
    return 0 if report["qualification_status"] == "PASS_COUNTEREXAMPLES_REPRODUCED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
