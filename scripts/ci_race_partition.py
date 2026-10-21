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
import stat
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
TestExecutionSessionSeedBoundaryControls
TestExecutionPrivateSessionSeedHelper
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



# One fixed, file-only artifact verifier. It never runs a command, installs a
# signal handler, or borrows the original-process controller's authority.
SEED_DIRECTORY = ".runtime/j1-seed-helper"
SEED_RAW_DIRECTORY = ".runtime/j1-seed-exercised"
SEED_POSITIVE = "TestExecutionSessionActualOwnerLifecycleAndInertStage"
SEED_BOUNDARY = "TestExecutionSessionSeedBoundaryControls"
SEED_HELPER = "TestExecutionPrivateSessionSeedHelper"
SEED_BINARY_MAX = 64 * 1024 * 1024
SEED_ASSET_MAX = 262144
SEED_ASSETS_MAX = 3 * 1024 * 1024
SEED_TOTAL_MAX = 75497472
SEED_R1_OUTPUT = SEED_RAW_DIRECTORY + "/broker.test"
SEED_ASSETS = {
    "go.mod": {
        "bytes": 58,
        "sha256": "bcf81d648b7f769adf99a0864b64701f350d46d3784c8b1c24fc55fbeccb341f"
    },
    "internal/broker/assessment_session_driver_test.go": {
        "bytes": 30855,
        "sha256": "a151e968c35a108d2966489d6d0607d3010aa208d9798f0435960d3c9bb3e51e"
    },
    "internal/broker/assessment_session_helper_test.go": {
        "bytes": 26542,
        "sha256": "45d92fccecc1d9760c0c68e605cf52f6ab86d95553b4ce16f6b4cb7403249c27"
    },
    "internal/broker/assessment_session_seed_linux_test.go": {
        "bytes": 44860,
        "sha256": "02649a0c6b44bd0392a69986f7651a8f12ac3160639f9761814e6e41a45b9ceb"
    },
    "internal/broker/assessment_session_seed_other_test.go": {
        "bytes": 4827,
        "sha256": "263d8048be0ead881e3d4c970b5316c599810c47815dbd4cbb8e4ae515c0aaf3"
    },
    "internal/broker/assessment_session_seed_test.go": {
        "bytes": 11500,
        "sha256": "ebc59838d30637b834f6034e6de9af85cf585ee0628de0c1e225b77bd9b6e7ea"
    },
    "internal/broker/testdata/execution-session-a2-input.txt": {
        "bytes": 21,
        "sha256": "6a3c8e8e07b90e41e33adc3d537c772317092f2ab87ec714e96c0277cd208360"
    },
    "internal/broker/testdata/execution-session-a2-manifest.json": {
        "bytes": 1752,
        "sha256": "7d47caac0dabc9d7b9e7fad74823ec6616f42254b1da65dc59eb18c40edf1d5b"
    },
    "internal/broker/testdata/execution-session-a2-seed-vectors.json": {
        "bytes": 4340,
        "sha256": "3105fe70b716621d1123dcaa72c9b2d5551ca1910c9de1735480f0744fb904ed"
    },
    "specs/24-assessment-session-fixture.md": {
        "bytes": 16869,
        "sha256": "8e2e7bb8ef8ef8fa6e260a641c1aa7b337f55d392642dc92d869415b1bb4b07a"
    }
}
SEED_LEAVES = (
    "literal-and-clock-boundaries", "fresh-factor-shape", "existing-and-partial-refuse",
    "nofollow-and-private-shape", "expired-positive-window", "closed-output-held",
    "blocked-output-deadline", "partial-write-held", "checked-close-and-late-wait-held",
    "single-shot-no-reseed",
)
SEED_PRODUCER = re.compile(
    r"    assessment_session_driver_test\.go:651: external seed executable: "
    r"sha256=([0-9a-f]{64}) bytes=([1-9][0-9]{0,8})\n")
SEED_METADATA = {"source-id.before.txt": 256, "source-id.after.txt": 256,
                 "tracked.before.nul": 131072, "tracked.after.nul": 131072,
                 "status.before.txt": 65536, "status.after.txt": 65536}
SEED_BEFORE_FILES = {"metadata/" + name for name in SEED_METADATA if name.endswith(".before.txt") or name.endswith(".before.nul")}
SEED_INITIAL_FILES = {"broker.test", *("metadata/" + name for name in SEED_METADATA)}


def seed_prelaunch(root):
    """Refuse a prior binary, including a dangling link, before the sole R1 run."""
    seed_shape(root, SEED_BEFORE_FILES, SEED_RAW_DIRECTORY)
    try:
        (root / SEED_R1_OUTPUT).lstat()
    except FileNotFoundError:
        return
    raise Refused("preexisting exercised binary")


def seed_stream(root, relative, cap=None, consume=None):
    """Bounded buffers; reject links/special files before opening or reading."""
    require(isinstance(relative, str) and len(relative) <= 4096 and
            re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", relative) and
            all(p not in (".", "..") for p in relative.split("/")), "artifact path")
    path = root
    for component in relative.split("/")[:-1]:
        path = path / component
        require(stat.S_ISDIR(path.lstat().st_mode), "artifact parent is not original directory")
    path = root / relative
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and (cap is None or before.st_size <= cap),
            "artifact file shape/cap")
    digest, size, prefix = hashlib.sha256(), 0, b""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        require(os.path.samestat(before, opened) and stat.S_ISREG(opened.st_mode), "artifact open identity")
        while block := handle.read(65536):
            size += len(block)
            require(size <= before.st_size and (cap is None or size <= cap), "artifact grew/cap")
            digest.update(block)
            prefix = (prefix + block[:64])[:64]
            if consume is not None:
                consume(block)
        after = os.fstat(handle.fileno())
        require(size == before.st_size == after.st_size and os.path.samestat(before, after) and
                before.st_mtime_ns == after.st_mtime_ns, "artifact changed during read")
    final = path.lstat()
    require(os.path.samestat(before, final) and before.st_size == final.st_size and
            before.st_mtime_ns == final.st_mtime_ns, "artifact changed after close")
    return {"bytes": size, "sha256": digest.hexdigest()}, prefix


def seed_bytes(root, relative, cap):
    blocks = []
    seed_stream(root, relative, cap, blocks.append)
    return b"".join(blocks)


def seed_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate artifact JSON key")
        value[key] = item
    return value


def seed_json(raw):
    def invalid(_value):
        raise Refused("nonfinite artifact JSON")
    return json.loads(raw, object_pairs_hook=seed_object, parse_constant=invalid)


def seed_shape(root, files, relative=SEED_DIRECTORY):
    directory = root / relative
    require(stat.S_ISDIR((root / ".runtime").lstat().st_mode) and
            stat.S_ISDIR(directory.lstat().st_mode), "artifact directory shape")
    directories = {str(Path(name).parent) for name in files}
    directories = {str(parent) for name in directories for parent in (Path(name), *Path(name).parents)
                   if str(parent) != "."}
    observed = set()
    for path in directory.rglob("*"):
        name, mode = str(path.relative_to(directory)), path.lstat().st_mode
        require((stat.S_ISDIR(mode) and name in directories) or
                (stat.S_ISREG(mode) and name in files), "unknown/link/special artifact entry")
        if stat.S_ISREG(mode):
            observed.add(name)
        require(len(observed) <= 4096, "artifact entry cap")
    require(observed == set(files), "missing artifact file")


def seed_events(root, relative, gate, records):
    pending = b""
    def consume(block):
        nonlocal pending
        pending += block
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            require(len(line) <= MAX_LINE, "artifact event line cap")
            value = seed_json(line)
            gate.event(value)
            output = value.get("Output")
            if isinstance(output, str) and "external seed executable:" in output:
                match = SEED_PRODUCER.fullmatch(output)
                require(value.get("Package") == BROKER and value.get("Test") == SEED_POSITIVE and
                        value.get("Action") == "output" and match is not None,
                        "wrong/malformed executable producer")
                records.append({"sha256": match[1], "bytes": int(match[2])})
                require(len(records) == 1 and records[0]["bytes"] <= SEED_BINARY_MAX,
                        "duplicate/oversize executable producer")
        require(len(pending) <= MAX_LINE, "artifact partial event cap")
    result, _ = seed_stream(root, relative, MAX_STDOUT, consume)
    require(not pending, "unfinished artifact events")
    gate.finish()
    return result


def seed_write(directory, name, raw, cap):
    require(len(raw) <= cap, "artifact output cap")
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        require(handle.write(raw) == len(raw), "short artifact write")
    require(seed_stream(directory, name, cap)[0] ==
            {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}, "artifact write/close/hash")


def seed_encode(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def seed_validate_exercised(root):
    """Validate every input before any binary or asset can enter the archive."""
    seed_shape(root, SEED_INITIAL_FILES, SEED_RAW_DIRECTORY)
    metadata = {name: seed_bytes(root, SEED_RAW_DIRECTORY + "/metadata/" + name, cap)
                for name, cap in SEED_METADATA.items()}
    require(sum(map(len, metadata.values())) <= 393728, "artifact metadata aggregate cap")
    require(metadata["status.before.txt"] == metadata["status.after.txt"] == b"", "dirty/untracked source")
    require(metadata["source-id.before.txt"] == metadata["source-id.after.txt"] and
            re.fullmatch(rb"[0-9a-f]{40}\n[0-9a-f]{40}\n", metadata["source-id.before.txt"]),
            "checkout identity drift/shape")
    head, tree = metadata["source-id.before.txt"].decode().splitlines()
    require(metadata["tracked.before.nul"] == metadata["tracked.after.nul"] and
            metadata["tracked.before.nul"].endswith(b"\0"), "tracked inventory drift/shape")
    names = metadata["tracked.before.nul"][:-1].decode("utf-8").split("\0")
    require(0 < len(names) <= 4096 and names == sorted(set(names)) and
            set(SEED_ASSETS) <= set(names) and "go.sum" not in names,
            "tracked inventory/asset/absence shape")
    require(not (root / "go.sum").exists() and not (root / "go.sum").is_symlink(), "unexpected go.sum")
    inventory = [{"path": name, **seed_stream(root, name)[0]} for name in names]
    inventory_raw = seed_encode(inventory)
    require(len(inventory_raw) <= 262144, "source inventory output cap")
    indexed = {item["path"]: {k: item[k] for k in ("bytes", "sha256")} for item in inventory}
    require(all(indexed[name] == pin for name, pin in SEED_ASSETS.items()), "pinned asset/source drift")

    summary = seed_json(seed_bytes(root, ".runtime/ci-race/summary.json", MAX_METADATA))
    require(isinstance(summary, dict) and set(summary) ==
            {"status", "failure", "commands", "finalization_errors", "remaining_coverage",
             "downstream_acceptance", "margin"} and summary["status"] == "PASS" and
            summary["failure"] is None and summary["finalization_errors"] == [] and
            summary["remaining_coverage"] == "COMPLETE" and
            summary["downstream_acceptance"] == "STILL_REQUIRED" and summary["margin"] == "UNKNOWN",
            "incomplete/failed six-command summary")
    outcomes = summary["commands"]
    require(isinstance(outcomes, dict) and set(outcomes) == set(COMMANDS), "missing/extra command outcome")
    outcome_keys = {"command", "kind", "state", "status", "argv", "exit", "identity_unknown",
                    "group_diagnostic", "original_wait_completed", "stream_eof", "unfinished_signal_transition",
                    "spawn_attempted", "started", "finalization_errors", "reason", "stored_bytes", "seen_bytes",
                    "sha256", "aggregate_offset", "aggregate_bytes", "coverage"}
    for name, outcome in outcomes.items():
        require(isinstance(outcome, dict) and set(outcome) == outcome_keys and
                outcome["command"] == name and outcome["kind"] == ("METADATA" if name.startswith("M") else "RACE") and
                outcome["state"] == "TERMINAL" and outcome["status"] == "PASS" and
                type(outcome["exit"]) is int and outcome["exit"] == 0 and
                outcome["identity_unknown"] is False and outcome["group_diagnostic"] == "ABSENT" and
                outcome["original_wait_completed"] is True and outcome["stream_eof"] == [True, True] and
                outcome["unfinished_signal_transition"] is None and outcome["spawn_attempted"] is True and
                outcome["started"] is True and outcome["finalization_errors"] == [] and outcome["reason"] is None,
                "failed/unknown command outcome")
        require(all(isinstance(outcome[key], list) and len(outcome[key]) == 2 and
                    all(type(value) is int and value >= 0 for value in outcome[key])
                    for key in ("stored_bytes", "seen_bytes")) and
                isinstance(outcome["sha256"], list) and len(outcome["sha256"]) == 2 and
                all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                    for value in outcome["sha256"]), "artifact capture metadata types")
    provenance = seed_json(seed_bytes(root, ".runtime/ci-race/M0.stdout", MAX_METADATA))
    effective_environment(provenance, root)
    environment = seed_json(seed_bytes(root, ".runtime/ci-race/environment.json", MAX_METADATA))
    require(isinstance(environment, dict) and set(environment) == {*ENV_KEYS, "GORACE", "GOMAXPROCS"} and
            {k: environment[k] for k in ENV_KEYS} == provenance and environment["GORACE"] in (None, ""),
            "artifact Go environment drift")
    require(all(len(v) <= 4096 for v in provenance.values()), "Go environment field cap")
    packages = package_inventory(seed_bytes(root, ".runtime/ci-race/M1.stdout", MAX_METADATA), root)
    require(seed_json(seed_bytes(root, ".runtime/ci-race/packages.json", MAX_METADATA)) == packages,
            "artifact package inventory drift")
    records, observed = [], {}
    listed = Events({BROKER: packages[BROKER]}, listing=True)
    observed["M2"] = seed_events(root, ".runtime/ci-race/M2.stdout", listed, records)
    require(listed.names == list(EXPECTED_BROKER), "artifact actual list/source order mismatch")
    selection = partition(listed.names)
    require(seed_json(seed_bytes(root, ".runtime/ci-race/partition.json", MAX_METADATA)) ==
            json.loads(json.dumps(selection)), "artifact partition drift")
    flags = ["go", "test", "-race", "-json", "-count=1", "-timeout=600s"]
    expected_argv = {"M0": ["go", "env", "-json", *ENV_KEYS], "M1": ["go", "list", "-json", "./..."],
                     "M2": [*flags, "-list=.", "./internal/broker"]}
    gates = {"M2": listed}
    for name, parents, pattern in zip(("R1", "R2"), selection["groups"], selection["patterns"]):
        gates[name] = Events({BROKER: packages[BROKER]}, parents)
        expected_argv[name] = [*flags, *(["-o", SEED_R1_OUTPUT] if name == "R1" else []),
                               "-run=" + pattern, "./internal/broker"]
    others = {name: value for name, value in packages.items() if name != BROKER}
    gates["R3"] = Events(others)
    expected_argv["R3"] = [*flags, *others]
    for name in ("R1", "R2", "R3"):
        observed[name] = seed_events(root, ".runtime/ci-race/" + name + ".stdout", gates[name], records)
    require(len(records) == 1, "missing executable producer")
    for test in (SEED_POSITIVE, SEED_BOUNDARY, SEED_HELPER,
                 *(SEED_BOUNDARY + "/" + leaf for leaf in SEED_LEAVES)):
        require(gates["R1"].tests.get((BROKER, test), {}).get("terminal") == "pass", "unqualified seed parent/leaf")
    for name in COMMANDS:
        outcome = outcomes[name]
        require(outcome["argv"] == expected_argv[name], "artifact command argv drift")
        if name not in observed:
            observed[name] = seed_stream(root, ".runtime/ci-race/" + name + ".stdout", MAX_STDOUT)[0]
        stderr = seed_stream(root, ".runtime/ci-race/" + name + ".stderr", MAX_STDERR)[0]
        require(outcome["stored_bytes"] == outcome["seen_bytes"] ==
                [observed[name]["bytes"], stderr["bytes"]] and
                outcome["sha256"] == [observed[name]["sha256"], stderr["sha256"]], "artifact capture drift")
        require(outcome["coverage"] == (gates[name].summary() if name in gates else None),
                "artifact coverage summary drift")
    binary, prefix = seed_stream(root, SEED_R1_OUTPUT, SEED_BINARY_MAX)
    machines = {"amd64": 62, "arm64": 183}
    require(binary["bytes"] >= 64 and prefix[:7] == b"\x7fELF\x02\x01\x01" and
            provenance["GOARCH"] in machines and
            int.from_bytes(prefix[18:20], "little") == machines[provenance["GOARCH"]], "export target ELF identity")
    require(binary == records[0], "retained binary differs from exercised seed executable")
    asset_size = sum(pin["bytes"] for pin in SEED_ASSETS.values())
    require(asset_size <= SEED_ASSETS_MAX, "asset aggregate cap")
    receipt = {"schema": "outfitter.j1-linux-helper-association/v2", "status": "MATCHED_BINARY",
               "checkout_head": head, "checkout_tree": tree,
               "checkout_relation": "ACTUAL_CHECKOUT; ROOT_PARENT_TREE_API_REVIEW_REQUIRED",
               "source_inventory_sha256": hashlib.sha256(inventory_raw).hexdigest(),
               "source_files": len(inventory), "go_sum": "ABSENT",
               "environment": {k: provenance[k] for k in ("GOOS", "GOARCH", "CGO_ENABLED", "GOVERSION", "GOFLAGS", "GOWORK")},
               "r1_argv": expected_argv["R1"], "r1_exit": outcomes["R1"]["exit"],
               "r1_meaning": "COMPILE_AND_TEST", "producer": {"package": BROKER, "test": SEED_POSITIVE, **records[0]},
               "exercised_binary": binary,
               "assets": SEED_ASSETS, "compiler_attestation": False,
               "downstream_acceptance": "STILL_REQUIRED", "full_ci_acceptance": "ROOT_REVIEW_REQUIRED",
               "margin": "UNKNOWN"}
    receipt_raw = seed_encode(receipt)
    require(len(receipt_raw) <= 16384, "association receipt cap")
    sources = {"broker.test": (SEED_R1_OUTPUT, binary, SEED_BINARY_MAX)}
    for name, cap in SEED_METADATA.items():
        relative = SEED_RAW_DIRECTORY + "/metadata/" + name
        raw = metadata[name]
        sources["metadata/" + name] = (relative, {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}, cap)
    for name, pin in SEED_ASSETS.items():
        require(pin["bytes"] <= SEED_ASSET_MAX, "asset file cap")
        sources["assets/" + name] = (name, pin, SEED_ASSET_MAX)
    require(sum(pin["bytes"] for _, pin, _ in sources.values()) + len(inventory_raw) + len(receipt_raw) <=
            SEED_TOTAL_MAX, "artifact total cap")
    return receipt, sources, inventory_raw


def seed_publish(root, temporary, directory, name, pin, cap, source=None, raw=None):
    """Only checked, closed and rehashed temporary files enter the archive."""
    target = temporary / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if raw is not None:
        seed_write(temporary, name, raw, cap)
    else:
        with target.open("xb") as output:
            def consume(block):
                require(output.write(block) == len(block), "short artifact copy")
            copied, _ = seed_stream(root, source, cap, consume)
            require(copied == pin, "artifact input changed during copy")
        require(seed_stream(temporary, name, cap)[0] == pin, "artifact copy/close/hash")
    destination = directory / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    require(not destination.exists() and not destination.is_symlink(), "preexisting artifact destination")
    os.rename(target, destination)
    require(seed_stream(directory, name, cap)[0] == pin, "published artifact hash/close")


SEED_FAILURE_CODES = {
    "unknown/link/special artifact entry": "ARTIFACT_SHAPE",
    "missing artifact file": "ARTIFACT_MISSING",
    "artifact file shape/cap": "ARTIFACT_SHAPE_OR_CAP",
    "artifact grew/cap": "ARTIFACT_UNSTABLE",
    "artifact changed during read": "ARTIFACT_UNSTABLE",
    "artifact changed after close": "ARTIFACT_UNSTABLE",
    "artifact open identity": "ARTIFACT_UNSTABLE",
    "dirty/untracked source": "SOURCE_DRIFT",
    "checkout identity drift/shape": "SOURCE_DRIFT",
    "tracked inventory drift/shape": "SOURCE_DRIFT",
    "tracked inventory/asset/absence shape": "SOURCE_DRIFT",
    "unexpected go.sum": "SOURCE_DRIFT",
    "pinned asset/source drift": "ASSET_DRIFT",
    "incomplete/failed six-command summary": "INCOMPLETE_RUN",
    "missing/extra command outcome": "INCOMPLETE_RUN",
    "failed/unknown command outcome": "INCOMPLETE_RUN",
    "artifact command argv drift": "ARGV_DRIFT",
    "missing executable producer": "PRODUCER_MISSING",
    "duplicate/oversize executable producer": "PRODUCER_INVALID",
    "wrong/malformed executable producer": "PRODUCER_INVALID",
    "unqualified seed parent/leaf": "SEED_FAILED",
    "export target ELF identity": "BINARY_IDENTITY",
    "retained binary differs from exercised seed executable": "BINARY_MISMATCH",
    "short artifact copy": "COPY_FAILED",
    "artifact input changed during copy": "COPY_FAILED",
    "artifact copy/close/hash": "COPY_FAILED",
    "short artifact write": "PUBLICATION_FAILED",
    "artifact write/close/hash": "PUBLICATION_FAILED",
    "published artifact hash/close": "PUBLICATION_FAILED",
    "retained input hash drift": "PUBLICATION_FAILED",
    "retained inventory hash drift": "PUBLICATION_FAILED",
}


def seed_failure_code(error):
    return SEED_FAILURE_CODES.get(str(error), "UNKNOWN") if isinstance(error, Refused) else "UNKNOWN"


def seed_failure(root, directory, phase, error, temporary, attempted):
    evidence = []
    candidates = {"metadata/" + name: cap for name, cap in SEED_METADATA.items()}
    for name, cap in candidates.items():
        item = {"path": name, "status": "IO_UNKNOWN", "bytes": None, "sha256": None}
        try:
            relative = SEED_RAW_DIRECTORY + "/" + name
            path = root
            for part in relative.split("/")[:-1]:
                path = path / part
                require(stat.S_ISDIR(path.lstat().st_mode), "failure evidence parent shape")
            info = (root / relative).lstat()
            require(stat.S_ISREG(info.st_mode), "failure evidence shape")
            item["bytes"] = info.st_size
            if info.st_size > cap:
                item["status"] = "OMITTED_CAP"  # Do not open, read or hash it.
            elif (directory / name).exists():
                # No second copy/adoption of an already attempted destination.
                item.update(status="ALREADY_STAGED", **seed_stream(directory, name, cap)[0])
            elif temporary is not None and name not in attempted:
                attempted.add(name)
                pin = seed_stream(root, relative, cap)[0]
                seed_publish(root, temporary, directory, name, pin, cap, source=relative)
                item.update(status="RETAINED", **pin)
        except FileNotFoundError:
            item["status"] = "MISSING"
        except BaseException:
            item["status"] = "IO_UNKNOWN"
        evidence.append(item)
    binary = {"path": "broker.test", "status": "UNKNOWN", "bytes": None, "sha256": None}
    try:
        pin, _ = seed_stream(root, SEED_R1_OUTPUT, SEED_BINARY_MAX)
        binary.update(status="UNQUALIFIED_HASH", **pin)
    except BaseException:
        pass
    evidence.append(binary)
    failure = {"schema": "outfitter.j1-linux-helper-failure/v2", "status": "FAILED",
               "phase": phase, "reason_code": seed_failure_code(error), "evidence": evidence,
               "partial_stage": "UNQUALIFIED", "full_ci_acceptance": "FAILED_OR_INCOMPLETE",
               "compiler_attestation": False}
    # This bounded FAILED record can survive even when the raw root is missing.
    raw = seed_encode(failure)
    seed_write(directory, "failure.pending", raw, 16384)
    os.rename(directory / "failure.pending", directory / "failure.json")


def verify_seed_exercised(root):
    """One fresh staging attempt; association alone never authorizes execution."""
    require(stat.S_ISDIR((root / ".runtime").lstat().st_mode), "artifact parent shape")
    directory = root / SEED_DIRECTORY
    directory.mkdir()  # No adoption, cleanup or retry of an existing stage.
    temporary, phase, attempted = None, "INPUT_QUALIFICATION", set()
    try:
        receipt, sources, inventory_raw = seed_validate_exercised(root)
        temporary = root / SEED_RAW_DIRECTORY / "retention"
        temporary.mkdir()  # Raw temporary files are never uploaded.
        phase = "COPY_OR_PUBLICATION"
        for name, (source, pin, cap) in sources.items():
            attempted.add(name)
            seed_publish(root, temporary, directory, name, pin, cap, source=source)
        inventory_pin = {"bytes": len(inventory_raw), "sha256": hashlib.sha256(inventory_raw).hexdigest()}
        seed_publish(root, temporary, directory, "source-inventory.json", inventory_pin, 262144, raw=inventory_raw)
        files = {*sources, "source-inventory.json"}
        seed_shape(root, files)
        # Re-read every archived file after all copies and checked closes.
        for name, (_, pin, cap) in sources.items():
            require(seed_stream(directory, name, cap)[0] == pin, "retained input hash drift")
        require(seed_stream(directory, "source-inventory.json", 262144)[0] == inventory_pin,
                "retained inventory hash drift")
        receipt_raw = seed_encode(receipt)
        receipt_pin = {"bytes": len(receipt_raw), "sha256": hashlib.sha256(receipt_raw).hexdigest()}
        # The success receipt is the LAST publication, from a checked raw temp.
        seed_publish(root, temporary, directory, "association.json", receipt_pin, 16384, raw=receipt_raw)
        return receipt
    except BaseException as error:
        # A return-boundary publication fault is still FAILED. Remove only this
        # fresh attempt's success name; never reuse/retry the original stage.
        try:
            (directory / "association.json").unlink(missing_ok=True)
        except BaseException:
            pass  # File alone is never authority; mode/job/root acceptance is mandatory.
        if temporary is None:
            try:
                raw_directory = root / SEED_RAW_DIRECTORY
                require(stat.S_ISDIR(raw_directory.lstat().st_mode), "raw directory shape")
                temporary = raw_directory / "retention"
                temporary.mkdir()
            except BaseException:
                temporary = None
        seed_failure(root, directory, phase, error, temporary, attempted)
        raise Refused("J1 artifact qualification failed: " + phase) from error


def seed_exercised_main():
    try:
        verify_seed_exercised(Path(__file__).resolve().parent.parent)
        return 0
    except BaseException as error:
        print("J1 artifact FAILED: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        return 1


def entry(arguments):
    if arguments == ["--verify-j1-seed-exercised"]:
        return seed_exercised_main()
    if arguments:
        print("unsupported CI arguments", file=sys.stderr)
        return 1
    return main()


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
            if name == "R1":
                seed_prelaunch(root)
            run(evidence, name, [*flags, *(["-o", SEED_R1_OUTPUT] if name == "R1" else []),
                                 "-run=" + pattern, "./internal/broker"],
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
    sys.exit(entry(sys.argv[1:]))
