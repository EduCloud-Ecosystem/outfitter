#!/usr/bin/env python3
"""Fixed Linux CI partition; metadata is never test qualification.

The existing job owns the total deadline. -timeout=600s is Go's test-binary
alarm, not a renewed command/compilation allowance. No command is retried.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time


MODULE = "github.com/EduCloud-Ecosystem/outfitter"
BROKER = MODULE + "/internal/broker"
COMMANDS = ("M0", "M1", "M2", "R1", "R2", "R3")
ENV_KEYS = ("GOOS", "GOARCH", "CGO_ENABLED", "GOVERSION", "GOFLAGS", "GOWORK", "GOMOD")
MAX_STDOUT = 64 * 1024 * 1024
MAX_STDERR = 8 * 1024 * 1024
MAX_LINE = 1024 * 1024
MAX_METADATA = 4 * 1024 * 1024
# Fixed CI policy: move only these whole parents, including all subtests, to R2.
COMPLEMENT_EXECUTION_PARENTS = (
    "TestExecutionCleanupLateUncertaintyCannotPublishAbsence",
    "TestExecutionFinalResultFreshObservationRejectsLateUncertainty",
    "TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold",
    "TestExecutionFinalResultTerminalPublicationFaults",
    "TestExecutionDeferredCleanupStoppedACKGuards",
)
EXPECTED_BROKER = tuple("""
TestExecutionDeferredCleanupStoppedACKGuards
TestExecutionPositiveCommitFencesRecheckCurrentAuthority
TestExecutionMainCloseFailureSurvivesUncertaintyPublicationFault
TestExecutionNormalExhaustionPersistsSafetyDenial
TestExecutionFixedFreshGuardFailureDeniesBeforeClaim
TestExecutionWatchConcurrentCommitIsNotClockRollback
TestExecutionLostClaimACKCannotDispatchOrReopen
TestExecutionWholeAttemptOutputBudgetAndDiscard
TestExecutionConstructorCloseUncertaintyIsSticky
TestExecutionClaimIdentityAndPlanCannotBeRebound
TestExecutionDirectJournalCannotFinalizeMissingResourceIdentity
TestExecutionSerialSlotTwoUsesSameInputAndBudget
TestExecutionDisabledDirectEntrypointsHaveNoEffects
TestExecutionMetadataSlotRefusalHasNoScopeEffects
TestExecutionSlotCancellationDoesNotDenyRetainedScope
TestExecutionOrphanedClaimCancellationRemainsHeld
TestExecutionDeferredPhaseFailureClearsReturnedOutput
TestExecutionGlobalCloseRequiresDefinitivePhaseDrain
TestExecutionGlobalCloseNotInferredFromParentClose
TestExecutionLateTransportCloseCannotRetainCapture
TestExecutionCallerCancelSuppressesLaterProviderEffects
TestExecutionCallerCancelAfterClaimRetainsUncertainty
TestExecutionCallerCancelReaderCollectorBeforeDispatch
TestExecutionConnectedTwoPhaseOnceAndNonretention
TestExecutionFinalResultFreshObservationRejectsLateUncertainty
TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold
TestExecutionFinalResultTerminalPublicationFaults
TestExecutionFinalizeLateUncertaintyKeepsHeld
TestExecutionOwnershipPositiveLifecycleAndCleanup
TestExecutionOwnershipStoreRefusesForgedCompletionAndCopiesShareOnce
TestExecutionPrelaunchWatchFailureClosesOriginalOwnersOnce
TestExecutionPartialWatchConstructionClosesUnlaunchedHandles
TestExecutionLaunchedWatchIsNotClosedByAbortingParent
TestExecutionOwnedMarkerRequiresPrivateSingleLinkRegularFile
TestExecutionWatchLeasePartialAndReleaseFailures
TestExecutionCleanupLateUncertaintyCannotPublishAbsence
TestExecutionStartTimestampCannotBecomeSeparateLateEvent
TestExecutionPrivatePhaseHelper
TestExecutionSeparateSequentialPhaseProcesses
TestExecutionActualPhaseProcessCancellationKeepsAmbiguousStartHeld
TestExecutionSessionDiagnosticClosedBoundsAndPoison
TestExecutionSessionOriginalCleanupReturnDiffersFromWatchEOF
TestExecutionSessionRejectsTriggerQueuedBeforeACK
TestExecutionSessionWithheldTriggerHasNoCoreEffects
TestExecutionSessionOriginalOwnerLossUsesCheckedWatchDrain
TestExecutionSessionBlockedStageAndParentLoss
TestExecutionSessionBlockedReplyDrainsOriginalChild
TestExecutionSessionSafetyProgressDuringBlockedUpload
TestExecutionSessionRejectsQueuedNextFrameDuringDispatch
TestExecutionSessionRejectsUnknownReplyWithoutRawOutput
TestExecutionSessionCancelBeforeDeadlineReset
TestExecutionSessionExpiryUsesEarliestRealGrant
TestExecutionSessionPhaseProbeRejectsActualLeakedEndpoint
TestExecutionSessionPhaseClosedBranchControls
TestExecutionSessionActualOwnerLifecycleAndInertStage
TestExecutionPrivateSessionOwnerHelper
TestExecutionPrivateSessionPhaseHelper
TestExecutionStopFirstDenialACKAndGenerationGuards
TestExecutionStopAlreadyDeniedUsesCommittedState
TestExecutionWatcherStopsActiveDeniedOrExpiredAttempt
TestExecutionOutputOverflowCancelsBlockedStart
TestAtomicConcurrentReservation
TestDuplicateNeverExecutesTwice
TestUnknownCreateRetainsLiabilityAcrossReopen
TestCleanupFailureNeverClaimsSuccess
TestBoundedOutputAndTimeout
TestNoWatchdogNoProviderEffect
TestSymlinkInputRefused
TestCancelUnfinishedCreationRemainsUncertain
TestReconcileChargesFullCeilingNeverRefunds
TestCopiedSnapshotVerificationUsesLogicalPathsAndExactBytes
TestOwnedPipeLinuxSyscallWitness
TestOwnedPipeIdentityRejectsFIFO
TestOwnedPipeIdentityEndpoints
TestOwnedPipeIdentityPreservesRegularFileLinks
TestOwnedSeparateProcessesRefuseFIFOLifelines
TestOwnedPeer
TestOwnedSeparateProcessesCompleteSealCancel
TestOwnedSeparateProcessesShortOverlongAndClaimACKLoss
TestOwnedSeparateProcessesCompletedWriterCannotRestart
TestOwnedSeparateProcessesOwnerAndWatcherLoss
TestOwnedSeparateProcessesBlockedWriterCancelAndRace
TestOwnedSeparateProcessesRejectBeforeBody
TestOwnedSeparateProcessesUnknownProjectAndWrongPair
TestOwnedSeparateProcessesAlivePipeSubstitution
TestOwnedSeparateProcessesClockFences
TestOwnedSeparateProcessesForeignObjectsHeld
TestOwnedSeparateProcessesCommitTimeFences
TestOwnedSeparateProcessesCloseUncertainty
TestOwnedSeparateProcessesWatchReadyCommitFences
TestOwnedSeparateProcessesUnconfirmedWriterCrashHeld
TestOwnedSeparateProcessesSaturatedObservationsStillRemove
TestOwnedSeparateProcessesRemovalAndObservationFailures
TestOwnedAuthorityRefusesOrdinaryReconciliationBeforeProvider
TestRecoveryRetainsUnknownReservationsWithoutReplayAndRequiresReconciliation
TestRecoveryCompletedDiagnosticsAndBudgetSurviveCutover
TestRecoveryMissingOutputRefusesCompleteCheckpointWithoutBlockingAuthority
TestReviewPartialChargeCannotReleaseUnknownReservation
TestReviewReceiptProjectCannotRetargetCommittedReservation
TestReviewAcceptedBoundedOutputsCannotMakeLedgerUnreadable
TestMissingDiagnosticsNeverBlocksAuthorityOrCleanup
TestDiagnosticWriteFailureCannotBlockTerminalAccounting
TestLegacyInlineDiagnosticsSpillWithoutReset
TestFullAuthorityCapacityStillAllowsTerminalCleanup
TestReviewAmbiguousCreationCannotSettleBeforeLateProviderCompletion
""".split())


class Refused(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Refused(message)


def inherited_environment(env):
    require(not env.get("GOFLAGS") and not env.get("GORACE"), "inherited Go flags/race override")
    require(not any(k.startswith(("OUTFITTER_PRIVATE_", "OUTFITTER_OWNED_TEST_")) for k in env),
            "inherited private fixture environment")


def effective_environment(value, root):
    require(isinstance(value, dict) and set(value) == set(ENV_KEYS), "Go provenance shape")
    require(all(isinstance(v, str) for v in value.values()), "Go provenance types")
    require(value["GOFLAGS"] == "", "effective GOFLAGS override")
    require(value["GOOS"] == "linux", "Linux runner required")
    require(value["GOWORK"] in ("", "off"), "active Go workspace")
    require(Path(value["GOMOD"]).resolve() == (root / "go.mod").resolve(), "wrong candidate GOMOD")


def package_inventory(raw, root):
    decoder, packages, offset = json.JSONDecoder(), {}, 0
    text = raw.decode("utf-8")
    while text[offset:].strip():
        offset += len(text[offset:]) - len(text[offset:].lstrip())
        value, offset = decoder.raw_decode(text, offset)
        require(isinstance(value, dict) and not value.get("Error") and not value.get("Incomplete"),
                "incomplete package metadata")
        name = value.get("ImportPath", "")
        require(isinstance(name, str) and re.fullmatch(re.escape(MODULE) + r"(?:/[A-Za-z0-9_./-]+)?", name),
                "unexpected package import path")
        require(name not in packages and len(packages) < 4096, "duplicate/capped package inventory")
        require(isinstance(value.get("Dir"), str) and Path(value["Dir"]).resolve().is_relative_to(root),
                "package outside candidate")
        for key in ("TestGoFiles", "XTestGoFiles"):
            require(isinstance(value.get(key, []), list) and
                    all(isinstance(x, str) for x in value.get(key, [])), "package test-file metadata")
        packages[name] = value
    require(BROKER in packages and len(packages) > 1, "missing broker/other packages")
    return packages


def partition(names, expected=EXPECTED_BROKER):
    require(len(names) == len(set(names)) and len(names) <= 256, "duplicate/capped listed names")
    kinds, runnable = {}, []
    for name in names:
        require(isinstance(name, str) and len(name) <= 256 and
                re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), "unsupported listed identifier")
        kind = next((k for k in ("Test", "Example", "Fuzz", "Benchmark") if name.startswith(k)), None)
        require(kind is not None and name != "TestMain", "unknown listed kind")
        kinds[name] = kind
        if kind != "Benchmark":
            runnable.append(name)
    require(set(runnable) == set(expected), "actual list/source projection mismatch")
    groups = ([n for n in runnable if n.startswith("TestExecution") and n not in COMPLEMENT_EXECUTION_PARENTS],
              [n for n in runnable if not n.startswith("TestExecution") or n in COMPLEMENT_EXECUTION_PARENTS])
    require(all(groups), "empty broker partition")
    patterns = ["^(" + "|".join(g) + ")$" for g in groups]
    require(all(len(p) <= 65536 for p in patterns), "capped selection pattern")
    return {"listed": names, "kinds": kinds, "groups": groups, "patterns": patterns}


class Events:
    """One command's closed event state; FAIL is never replaced by later PASS."""

    def __init__(self, packages, parents=None, listing=False):
        self.packages = packages
        self.parents = None if parents is None else set(parents)
        self.listing = listing
        self.package_states, self.tests, self.errors = {}, {}, []
        self.buffer, self.list_buffer, self.names = b"", "", []

    def error(self, message):
        if len(self.errors) < 32:
            self.errors.append(message)

    def feed(self, raw):
        self.buffer += raw
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            require(len(line) <= MAX_LINE, "capped JSON line")
            self.event(json.loads(line))
        require(len(self.buffer) <= MAX_LINE, "capped partial JSON line")

    def event(self, event):
        require(isinstance(event, dict) and set(event) <=
                {"Time", "Action", "Package", "Test", "Elapsed", "Output", "FailedBuild"}, "Go event shape")
        package, action, test = event.get("Package"), event.get("Action"), event.get("Test")
        require(isinstance(package, str) and package in self.packages, "unlisted event package")
        require(isinstance(action, str) and action in ("start", "run", "pause", "cont", "pass", "fail", "skip", "output"),
                "unknown Go action")
        require(test is None or (isinstance(test, str) and 0 < len(test) <= 4096), "invalid test identity type")
        state = self.package_states.setdefault(package, {"start": 0, "terminal": None})
        if action == "start":
            require(not test and not state["start"], "duplicate package start")
            state["start"] = 1
            return
        require(state["start"] == 1 and state["terminal"] is None, "event outside open package")
        if action == "fail":
            self.error("Go FAIL: " + (test or package))
        if test is not None:
            require(not self.listing, "list mode executed a test")
            parent = test.split("/", 1)[0]
            require(re.fullmatch(r"(?:Test|Example|Fuzz)[A-Za-z0-9_]*", parent) and parent != "TestMain",
                    "unexpected runtime test kind")
            require(self.parents is None or parent in self.parents, "unlisted runtime parent")
            key = (package, test)
            if action == "run":
                require(key not in self.tests and len(self.tests) < 32768, "duplicate/capped test run")
                if "/" in test:
                    # A single t.Run name may itself contain '/'; it does not
                    # imply an intermediate test had its own RUN event.
                    require((package, parent) in self.tests and
                            self.tests[(package, parent)]["terminal"] is None, "subtest outside open top-level parent")
                self.tests[key] = {"terminal": None, "paused": False}
                return
            require(key in self.tests, "test event without run")
            item = self.tests[key]
            if action == "output":
                require(isinstance(event.get("Output"), str), "test output type")
                return
            require(item["terminal"] is None, "duplicate test terminal/event")
            if action in ("pause", "cont"):
                require(item["paused"] == (action == "cont"), "incoherent pause/cont")
                item["paused"] = action == "pause"
                return
            require(action in ("pass", "fail", "skip") and not item["paused"], "invalid test terminal")
            if "/" not in test:
                require(all(v["terminal"] is not None for (p, n), v in self.tests.items()
                            if p == package and n.startswith(test + "/")), "top-level parent before subtest terminal")
            item["terminal"] = action
            return
        if action == "output":
            require(isinstance(event.get("Output"), str), "package output type")
            if self.listing:
                self.list_buffer += event["Output"]
                while "\n" in self.list_buffer:
                    line, self.list_buffer = self.list_buffer.split("\n", 1)
                    require(len(line) <= MAX_LINE, "capped list line")
                    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", line):
                        require(len(self.names) < 512, "capped listed names")
                        self.names.append(line)
                    else:
                        require(re.fullmatch(r"ok\s+" + re.escape(BROKER) +
                                             r"\s+[0-9]+(?:\.[0-9]+)?s(?:\s+\[no tests to run\])?", line),
                                "unexpected list output")
                require(len(self.list_buffer) <= MAX_LINE, "capped partial list line")
            # Go's structured fail/status and missing terminals detect fatal
            # exits. An arbitrary log containing 'panic' is not a harness FAIL.
            return
        require(action in ("pass", "fail", "skip"), "invalid package action")
        if action == "skip":
            meta = self.packages[package]
            require(not self.listing and not meta.get("TestGoFiles") and not meta.get("XTestGoFiles"),
                    "unexpected package skip")
        require(all(v["terminal"] is not None for (p, _), v in self.tests.items() if p == package),
                "unfinished test at package terminal")
        state["terminal"] = action

    def finish(self):
        require(not self.buffer and not self.list_buffer, "truncated JSON/list line")
        require(set(self.package_states) == set(self.packages), "missing package")
        require(all(x["start"] == 1 and x["terminal"] in ("pass", "skip")
                    for x in self.package_states.values()), "missing/failed package terminal")
        require(all(x["terminal"] is not None for x in self.tests.values()), "unfinished test")
        if self.parents is not None:
            require({n for _, n in self.tests if "/" not in n} == self.parents, "missing selected parent")
        require(not self.errors, "; ".join(self.errors))

    def summary(self):
        parents = {p: {n: x["terminal"] or "UNFINISHED" for (q, n), x in self.tests.items()
                       if p == q and "/" not in n} for p in self.packages}
        if self.parents is not None:
            parents[BROKER].update({n: "NOT_RUN" for n in self.parents if n not in parents[BROKER]})
        packages = {p: {**s, "classification": "NO_TEST_FILES" if s["terminal"] == "skip" else None}
                    for p, s in self.package_states.items()}
        return {"packages": packages, "parents": parents,
                "subtests": sum("/" in n for _, n in self.tests), "failures": self.errors}


class Original:
    """Only WNOWAIT may observe before the last permitted group signal."""

    def __init__(self, process):
        self.process, self.observed = process, None
        self.lost, self.reaped, self.waited = False, False, False
        self.signals = []

    def peek(self):
        require(not self.lost and not self.reaped, "original identity unavailable")
        try:
            require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, "incompatible SIGCHLD handling")
            value = os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if value is not None:
                require(value.si_pid == self.process.pid and
                        value.si_code in (os.CLD_EXITED, os.CLD_KILLED, os.CLD_DUMPED), "unexpected original wait observation")
                code = value.si_status if value.si_code == os.CLD_EXITED else -value.si_status
                require(self.observed is None or self.observed == code, "original exit changed")
                self.observed = code
        except InterruptedError:
            return self.observed
        except BaseException as error:
            self.lost = True
            raise Refused("original wait identity lost") from error
        return self.observed

    def send(self, sig):
        try:
            self.peek()  # May latch identity loss. Never signal afterward.
            require(not self.reaped and not self.lost, "signal after identity loss/reap")
            require((sig == signal.SIGTERM and not self.signals) or
                    (sig == signal.SIGKILL and self.signals == [signal.SIGTERM]), "unexpected/repeated group signal")
            self.signals.append(sig)  # Validation/consumption faults are unknown too.
            try:
                os.killpg(self.process.pid, sig)
            except ProcessLookupError:
                pass
        except BaseException as error:
            self.lost = True
            raise Refused("original group signaling unknown: " + type(error).__name__) from error

    def reap(self):
        require(not self.reaped and not self.lost and self.observed is not None, "unwitnessed final wait")
        self.reaped = True  # No signal is possible from this point onward.
        try:
            code = self.process.wait()
            require(code == self.observed, "final wait disagrees with WNOWAIT")
        except BaseException:
            self.lost = True
            raise
        self.waited = True
        return code

    def diagnostic(self):
        require(self.waited, "group diagnostic before completed final reap")
        try:
            os.killpg(self.process.pid, 0)  # Existence check ONLY; never cleanup.
        except ProcessLookupError:
            return "ABSENT"
        except OSError:
            return "UNKNOWN"
        return "RESIDUE"


class Evidence:
    def __init__(self, root):
        (root / ".runtime").mkdir(exist_ok=True)
        self.directory = root / ".runtime/ci-race"
        self.directory.mkdir()  # Existing evidence is never overwritten.
        self.aggregate = (root / ".runtime/go-tests.jsonl").open("xb")
        self.ledger = (self.directory / "commands.jsonl").open("xb")
        self.outcomes = {}
        self.write("plan.json", {"commands": list(COMMANDS), "broker_source_projection": EXPECTED_BROKER,
                                 "status": "NOT_RUN", "total_job_minutes": 15, "margin": "UNKNOWN"})
        for name in COMMANDS:
            self.record({"command": name, "state": "PLANNED"})

    def write(self, name, value):
        with (self.directory / name).open("xb") as output:
            output.write((json.dumps(value, sort_keys=True) + "\n").encode())

    def record(self, value):
        self.ledger.write((json.dumps({"at": time.time(), **value}, sort_keys=True) + "\n").encode())
        self.ledger.flush()


def run(evidence, name, argv, gate=None, runtime=False):
    """One selector loop copies both streams and alone observes/signals/reaps."""
    # Even a capture-open/finalization fault must not relabel this attempt
    # NOT_RUN. STARTED is set in memory before any STARTED record can fail.
    evidence.outcomes[name] = {"command": name, "state": "ATTEMPTED", "status": "UNFINISHED", "argv": argv}
    files, selector, finalization_errors = [], None, []
    hashes, stored, seen = [hashlib.sha256(), hashlib.sha256()], [0, 0], [0, 0]
    eof = [False, False]
    caps = [MAX_STDOUT if gate else MAX_METADATA, MAX_STDERR]
    offset = None
    original, reason = Original(None), None
    exit_at = term_at = kill_at = None
    signal_transition = None
    spawn_attempted = capture_attempted = False
    identity_unknown, exit_code, residue = False, None, "UNKNOWN"
    try:
        for suffix in (".stdout", ".stderr"):
            files.append((evidence.directory / (name + suffix)).open("xb"))
        selector = selectors.DefaultSelector()
        offset = evidence.aggregate.tell() if runtime else None
        evidence.record({"command": name, "state": "STARTING", "argv": argv})
        while True:
            try:
                if not spawn_attempted:
                    spawn_attempted = True
                    evidence.outcomes[name].update(state="SPAWN_ATTEMPTED", identity_unknown=True)
                    original.process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE, start_new_session=True)
                    evidence.outcomes[name].update(state="STARTED", pid=original.process.pid, pgid=original.process.pid)
                if not capture_attempted:
                    # A setup exception must not retry registration or bypass
                    # the original-owner pre-reap cleanup loop.
                    capture_attempted = True
                    process = original.process
                    evidence.record({"command": name, "state": "STARTED", "pid": process.pid, "pgid": process.pid})
                    for index, stream in enumerate((process.stdout, process.stderr)):
                        selector.register(stream, selectors.EVENT_READ, index)
                code = original.peek()
                now = time.monotonic()
                if code is not None and exit_at is None:
                    exit_at = now
                    evidence.record({"command": name, "state": "EXIT_OBSERVED_UNREAPED", "exit": code})
                if code is not None and all(eof) and not selector.get_map() and reason is None:
                    break
                if exit_at is not None and now - exit_at >= 2 and reason is None:
                    reason = "missing stream EOF after original exit"
                if reason is not None:
                    if term_at is None:
                        signal_transition = signal.SIGTERM
                        term_at = now
                        original.send(signal.SIGTERM)
                        evidence.record({"command": name, "state": "TERM_UNREAPED"})
                        signal_transition = None
                    elif kill_at is None and now - term_at >= 2:
                        signal_transition = signal.SIGKILL
                        kill_at = now
                        original.send(signal.SIGKILL)
                        evidence.record({"command": name, "state": "KILL_UNREAPED"})
                        signal_transition = None
                    elif kill_at is not None and now - kill_at >= 2:
                        break
                for key, _ in selector.select(0.05):
                    index = key.data
                    raw = os.read(key.fd, 65536)
                    if not raw:
                        eof[index] = True
                        selector.unregister(key.fileobj)
                        continue
                    seen[index] += len(raw)
                    keep = raw[:max(0, caps[index] - stored[index])]
                    files[index].write(keep)
                    files[index].flush()
                    hashes[index].update(keep)
                    stored[index] += len(keep)
                    if index == 0 and runtime:
                        evidence.aggregate.write(keep)
                        evidence.aggregate.flush()
                    target = sys.stdout.buffer if index == 0 else sys.stderr.buffer
                    target.write(keep)
                    target.flush()
                    if len(keep) != len(raw):
                        reason = reason or "CAPPED stream; retained prefix is incomplete"
                    if index == 0 and gate is not None and reason is None:
                        gate.feed(keep)
            except BaseException as error:
                reason = reason or (type(error).__name__ + ": " + str(error))
                if signal_transition is not None:
                    # Includes an interruption after send succeeded but before
                    # its caller finished recording the consumed transition.
                    original.lost = True
                if original.process is None or original.lost:
                    identity_unknown = True
                    break
                # The same loop performs bounded pre-reap cleanup; no reaper
                # thread, Popen.poll or TERM->wait->blind KILL exists.
                if kill_at is not None and time.monotonic() - kill_at >= 2:
                    break
        if original.observed is not None and not original.lost:
            exit_code = original.reap()
            residue = original.diagnostic()
        else:
            identity_unknown = True
        if reason is None:
            require(exit_code == 0 and all(eof) and not selector.get_map() and residue == "ABSENT",
                    "exit/EOF/group diagnostic incomplete")
            if gate is not None:
                gate.finish()
    except BaseException as error:
        reason = reason or (type(error).__name__ + ": " + str(error))
    finally:
        if spawn_attempted and (not original.waited or original.lost):
            identity_unknown = True
        closers = []
        if selector is not None:
            closers.append(("selector.close", selector.close))
        if original.process is not None:
            closers.extend((("child.stdout.close", original.process.stdout.close),
                            ("child.stderr.close", original.process.stderr.close)))
        closers.extend(("capture." + str(index) + ".close", output.close) for index, output in enumerate(files))
        for label, close in closers:
            try:
                close()
            except BaseException as error:
                finalization_errors.append({"operation": label, "error": type(error).__name__})
                reason = reason or ("finalization failed: " + label + ": " + type(error).__name__)
    coverage = None
    if gate is not None:
        try:
            coverage = gate.summary()
        except BaseException as error:
            finalization_errors.append({"operation": "coverage.summary", "error": type(error).__name__})
            reason = reason or ("coverage finalization failed: " + type(error).__name__)
    outcome = {"command": name, "kind": "METADATA" if name.startswith("M") else "RACE",
               "state": "TERMINAL", "status": "FAIL" if reason or identity_unknown else "PASS",
               "argv": argv, "exit": exit_code, "identity_unknown": identity_unknown, "group_diagnostic": residue,
               "original_wait_completed": original.waited, "stream_eof": eof,
               "unfinished_signal_transition": signal_transition,
               "spawn_attempted": spawn_attempted, "started": original.process is not None,
               "finalization_errors": finalization_errors,
               "reason": reason, "stored_bytes": stored, "seen_bytes": seen,
               "sha256": [h.hexdigest() for h in hashes], "aggregate_offset": offset,
               "aggregate_bytes": stored[0] if runtime else 0,
               "coverage": coverage}
    evidence.outcomes[name] = outcome
    try:
        evidence.record(outcome)
    except BaseException as error:
        # Keep a truthful in-memory result for summary even when the append
        # fails. Do not retry or overwrite the earlier evidence prefix.
        outcome.update(status="FAIL", state="FINALIZATION_FAILED")
        outcome["finalization_errors"].append({"operation": "terminal.record", "error": type(error).__name__})
        outcome["reason"] = outcome["reason"] or ("terminal record failed: " + type(error).__name__)
    require(outcome["status"] == "PASS", "command " + name + " failed/incomplete")
    return (evidence.directory / (name + ".stdout")).read_bytes() if gate is None else None


def main():
    root = Path(__file__).resolve().parent.parent
    os.chdir(root)
    evidence = None
    failure = None
    completed, selection, packages = False, None, None
    try:
        evidence = Evidence(root)
        inherited_environment(os.environ)
        require(sys.platform == "linux" and hasattr(os, "waitid") and hasattr(os, "WNOWAIT"), "Linux WNOWAIT required")
        require(signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL, "pre-existing SIGCHLD handler")
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)  # Establish waitable children; never a SIG_IGN auto-reaper.
        def interrupted(_signum, _frame):
            raise KeyboardInterrupt("runner interrupted")
        signal.signal(signal.SIGTERM, interrupted)
        provenance = json.loads(run(evidence, "M0", ["go", "env", "-json", *ENV_KEYS]))
        effective_environment(provenance, root)
        evidence.write("environment.json", {**provenance, "GORACE": os.environ.get("GORACE"),
                                             "GOMAXPROCS": os.environ.get("GOMAXPROCS")})
        packages = package_inventory(run(evidence, "M1", ["go", "list", "-json", "./..."]), root)
        evidence.write("packages.json", packages)
        listed = Events({BROKER: packages[BROKER]}, listing=True)
        flags = ["go", "test", "-race", "-json", "-count=1", "-timeout=600s"]
        run(evidence, "M2", [*flags, "-list=.", "./internal/broker"], listed)
        selection = partition(listed.names)
        evidence.write("partition.json", selection)
        for name, parents, pattern in zip(("R1", "R2"), selection["groups"], selection["patterns"]):
            run(evidence, name, [*flags, "-run=" + pattern, "./internal/broker"],
                Events({BROKER: packages[BROKER]}, parents), runtime=True)
        others = {n: p for n, p in packages.items() if n != BROKER}
        run(evidence, "R3", [*flags, *others], Events(others), runtime=True)
        require(set(evidence.outcomes) == set(COMMANDS) and
                all(o["status"] == "PASS" for o in evidence.outcomes.values()), "incomplete command gates")
        completed = True
    except BaseException as error:
        failure = type(error).__name__ + ": " + str(error)
        print(failure, file=sys.stderr)
    finally:
        if evidence is not None:
            finalization_errors = []
            for label, handle in (("aggregate.close", evidence.aggregate), ("ledger.close", evidence.ledger)):
                try:
                    handle.close()
                except BaseException as error:
                    finalization_errors.append({"operation": label, "error": type(error).__name__})
                    failure = failure or ("evidence finalization failed: " + label + ": " + type(error).__name__)
                    completed = False
            outcomes = dict(evidence.outcomes)
            for name in COMMANDS:
                if name not in outcomes:
                    outcomes[name] = {"status": "NOT_RUN"}
                    if selection is not None and name in ("R1", "R2"):
                        outcomes[name]["parents"] = {BROKER: {n: "NOT_RUN" for n in selection["groups"][name == "R2"]}}
                    if packages is not None and name == "R3":
                        outcomes[name]["packages"] = {p: "NOT_RUN" for p in packages if p != BROKER}
            evidence.write("summary.json", {"status": "PASS" if completed and not failure else "FAIL",
                                             "failure": failure, "commands": outcomes,
                                             "finalization_errors": finalization_errors,
                                             "remaining_coverage": "COMPLETE" if completed else "INCOMPLETE",
                                             "downstream_acceptance": "STILL_REQUIRED", "margin": "UNKNOWN"})
    return 0 if completed and not failure else 1


if __name__ == "__main__":
    sys.exit(main())
