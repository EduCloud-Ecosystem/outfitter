"""Pure synthetic parser/state controls: never starts Go or another process."""

from contextlib import ExitStack
import errno
import itertools
import json
from pathlib import Path
import signal
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

import ci_race_partition as ci


META = {"TestGoFiles": ["fixture_test.go"]}


def event(action, test=None, package=ci.BROKER, **extra):
    value = {"Action": action, "Package": package, **extra}
    if test is not None:
        value["Test"] = test
    return value


def complete(gate, names):
    gate.event(event("start"))
    for name in names:
        gate.event(event("run", name))
        gate.event(event("pass", name))
    gate.event(event("pass"))
    gate.finish()


class ParserControls(unittest.TestCase):
    def test_real_projection_is_complete_and_disjoint(self):
        selection = ci.partition(list(ci.EXPECTED_BROKER))
        self.assertEqual(len(ci.EXPECTED_BROKER), 105)
        self.assertEqual(len(set(ci.EXPECTED_BROKER)), 105)
        self.assertEqual([len(g) for g in selection["groups"]], [56, 49])
        self.assertFalse(set(selection["groups"][0]) & set(selection["groups"][1]))
        self.assertEqual(set().union(*map(set, selection["groups"])), set(ci.EXPECTED_BROKER))
        parent = "TestExecutionFinalizeLateUncertaintyKeepsHeld"
        self.assertEqual(ci.EXPECTED_BROKER.count(parent), 1)
        self.assertEqual(selection["groups"][0].count(parent), 1)
        self.assertNotIn(parent, selection["groups"][1])
        position = ci.EXPECTED_BROKER.index(parent)
        self.assertEqual(ci.EXPECTED_BROKER[position - 4:position + 2],
                         ("TestExecutionConnectedTwoPhaseOnceAndNonretention",
                          "TestExecutionFinalResultFreshObservationRejectsLateUncertainty",
                          "TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold",
                          "TestExecutionFinalResultTerminalPublicationFaults", parent,
                          "TestExecutionOwnershipPositiveLifecycleAndCleanup"))
        self.assertEqual(ci.EXPECTED_BROKER[:2],
                         ("TestExecutionDeferredCleanupStoppedACKGuards",
                          "TestExecutionPositiveCommitFencesRecheckCurrentAuthority"))
        position = ci.EXPECTED_BROKER.index("TestExecutionStopFirstDenialACKAndGenerationGuards")
        self.assertEqual(ci.EXPECTED_BROKER[position - 1:position + 3],
                         ("TestExecutionPrivateSessionPhaseHelper",
                          "TestExecutionStopFirstDenialACKAndGenerationGuards",
                          "TestExecutionStopAlreadyDeniedUsesCommittedState",
                          "TestExecutionWatcherStopsActiveDeniedOrExpiredAttempt"))

    def test_only_named_parents_move_and_owned_group_stays_whole(self):
        moved = (
            "TestExecutionCleanupLateUncertaintyCannotPublishAbsence",
            "TestExecutionFinalResultFreshObservationRejectsLateUncertainty",
            "TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold",
            "TestExecutionFinalResultTerminalPublicationFaults",
            "TestExecutionDeferredCleanupStoppedACKGuards",
        )
        new_execution = ("TestExecutionStopFirstDenialACKAndGenerationGuards",
                         "TestExecutionStopAlreadyDeniedUsesCommittedState")
        added = (*moved[1:], *new_execution)
        self.assertEqual(ci.COMPLEMENT_EXECUTION_PARENTS, moved)
        selection = ci.partition(list(ci.EXPECTED_BROKER))
        execution, complement = selection["groups"]
        all_execution = [n for n in ci.EXPECTED_BROKER if n.startswith("TestExecution")]
        self.assertEqual([n for n in all_execution if n not in execution],
                         [n for n in ci.EXPECTED_BROKER if n in moved])
        self.assertEqual(execution, [n for n in all_execution if n not in moved])
        self.assertEqual(complement, [n for n in ci.EXPECTED_BROKER
                                     if not n.startswith("TestExecution") or n in moved])
        self.assertEqual(complement, [n for n in ci.EXPECTED_BROKER if n in set(complement)])
        for name in added:
            group = execution if name in new_execution else complement
            other = complement if name in new_execution else execution
            self.assertEqual(ci.EXPECTED_BROKER.count(name), 1)
            self.assertEqual(group.count(name), 1)
            self.assertNotIn(name, other)
        self.assertEqual(complement.count(moved[0]), 1)
        prior = [n for n in ci.EXPECTED_BROKER if n not in added]
        self.assertEqual(len(prior), 99)
        prior_execution = [n for n in prior if n.startswith("TestExecution") and n != moved[0]]
        prior_complement = [n for n in prior if not n.startswith("TestExecution") or n == moved[0]]
        self.assertEqual([len(prior_execution), len(prior_complement)], [54, 45])
        self.assertEqual([n for n in execution if n not in added], prior_execution)
        self.assertEqual([n for n in complement if n not in added], prior_complement)
        # No slash selector may split the ACK five leaves, FinalResult fault
        # siblings or Stop ACK/generation six leaves into separate invocations.
        for pattern in selection["patterns"]:
            self.assertNotIn("/", pattern)
        for name, group in zip(("R1", "R2"), selection["groups"]):
            pattern = selection["patterns"][name == "R2"]
            self.assertEqual(pattern, "^(" + "|".join(group) + ")$")
        future = ["TestExecutionFinalResultFutureControl", "ExampleOne"]
        self.assertEqual(ci.partition(future, expected=future)["groups"], ([future[0]], [future[1]]))
        self.assertTrue(all(n in complement for n in ci.EXPECTED_BROKER if n.startswith("TestOwned")))
        self.assertEqual(ci.COMMANDS, ("M0", "M1", "M2", "R1", "R2", "R3"))

    def test_examples_and_seed_corpus_selected_benchmarks_not_enabled(self):
        names = ["TestExecutionOne", "ExampleOne", "FuzzOne", "BenchmarkOne"]
        selection = ci.partition(names, expected=names[:3])
        self.assertEqual(selection["groups"], ([names[0]], names[1:3]))
        self.assertEqual(selection["patterns"][1], "^(ExampleOne|FuzzOne)$")
        complete(ci.Events({ci.BROKER: META}, names[:3]), names[:3])

    def test_selection_refusals(self):
        for names in (["TestExecutionOne"], ["TestExecutionOne", "TestExecutionOne"],
                      ["TestExecutionOne", "TestOther/child"], ["TestExecutionOne", "TestMain"],
                      ["TestExecutionOne", "unexpected"], ["TestExecutionOne", "TestDifferent"]):
            with self.subTest(names=names), self.assertRaises(ci.Refused):
                ci.partition(names, expected=("TestExecutionOne", "TestOther"))

    def test_split_json_and_list_output(self):
        gate = ci.Events({ci.BROKER: META}, listing=True)
        values = [event("start"), event("output", Output="TestExecutionOne\nExa"),
                  event("output", Output="mpleOne\nFuzzOne\nBenchmarkOne\n"),
                  event("output", Output="ok\t" + ci.BROKER + "\t0.001s\n"), event("pass")]
        raw = b"".join((json.dumps(v) + "\n").encode() for v in values)
        for start in range(0, len(raw), 7):
            gate.feed(raw[start:start + 7])
        gate.finish()
        self.assertEqual(gate.names, ["TestExecutionOne", "ExampleOne", "FuzzOne", "BenchmarkOne"])

    def test_list_noise_test_events_and_malformed_json_refused(self):
        for value in (event("output", Output="unexpected noise\n"), event("run", "TestOne"),
                      event("output", Output="ok\twrong-package\t0.1s\n")):
            gate = ci.Events({ci.BROKER: META}, listing=True)
            gate.event(event("start"))
            with self.subTest(value=value), self.assertRaises(ci.Refused):
                gate.event(value)
        with self.assertRaises(ValueError):
            ci.Events({ci.BROKER: META}).feed(b"{broken}\n")
        gate = ci.Events({ci.BROKER: META})
        gate.feed(b'{"Action":')
        with self.assertRaises(ci.Refused):
            gate.finish()
        with patch.object(ci, "MAX_LINE", 4), self.assertRaises(ci.Refused):
            ci.Events({ci.BROKER: META}).feed(b"12345")

    def test_missing_duplicate_unlisted_and_wrong_package(self):
        gate = ci.Events({ci.BROKER: META}, ["TestOne"])
        gate.event(event("start"))
        gate.event(event("pass"))
        with self.assertRaises(ci.Refused):
            gate.finish()
        for values in ([event("run", "TestOne"), event("run", "TestOne")],
                       [event("run", "TestOne"), event("pass", "TestOne"), event("pass", "TestOne")],
                       [event("run", "TestUnlisted")], [event("run", "TestOne", package="wrong")],
                       [event("pass", "TestOne")], [event("run", "TestOne"), event("pass")]):
            gate = ci.Events({ci.BROKER: META}, ["TestOne"])
            gate.event(event("start"))
            with self.subTest(values=values), self.assertRaises(ci.Refused):
                for value in values:
                    gate.event(value)

    def test_subtest_slashes_pause_and_skip_preserved(self):
        gate = ci.Events({ci.BROKER: META}, ["TestOne"])
        for value in (event("start"), event("run", "TestOne"), event("run", "TestOne/a/b"),
                      event("pause", "TestOne/a/b"), event("cont", "TestOne/a/b"),
                      event("skip", "TestOne/a/b"), event("pass", "TestOne"), event("pass")):
            gate.event(value)
        gate.finish()
        self.assertEqual(gate.tests[(ci.BROKER, "TestOne/a/b")]["terminal"], "skip")
        broken = ci.Events({ci.BROKER: META})
        for value in (event("start"), event("run", "TestOne")):
            broken.event(value)
        with self.assertRaises(ci.Refused):
            broken.event(event("cont", "TestOne"))

    def test_literal_slash_sibling_is_not_an_invented_ancestor(self):
        gate = ci.Events({ci.BROKER: META}, ["TestOne"])
        for value in (event("start"), event("run", "TestOne"), event("run", "TestOne/a"),
                      event("pause", "TestOne/a"), event("run", "TestOne/a/b"), event("pause", "TestOne/a/b"),
                      event("cont", "TestOne/a"), event("pass", "TestOne/a"),
                      event("cont", "TestOne/a/b"), event("pass", "TestOne/a/b"),
                      event("pass", "TestOne"), event("pass")):
            gate.event(value)
        gate.finish()

    def test_retained_actual_literal_slash_records(self):
        # Four exact selected records from archived39408950606 go-tests.jsonl,
        # SHA0bba6d94, lines4,6,13,43. Only package boundaries are synthetic;
        # other original child branches are not replayed by this control.
        records = (
            '{"Time":"2026-10-20T10:35:26.344313428Z","Action":"run","Package":"github.com/EduCloud-Ecosystem/outfitter/cmd/outfitter","Test":"TestExecutionCLIContentRefusalPrecedesInputAndState"}',
            '{"Time":"2026-10-20T10:35:26.34474216Z","Action":"run","Package":"github.com/EduCloud-Ecosystem/outfitter/cmd/outfitter","Test":"TestExecutionCLIContentRefusalPrecedesInputAndState/prepare/fixed"}',
            '{"Time":"2026-10-20T10:35:26.348852409Z","Action":"pass","Package":"github.com/EduCloud-Ecosystem/outfitter/cmd/outfitter","Test":"TestExecutionCLIContentRefusalPrecedesInputAndState/prepare/fixed","Elapsed":0}',
            '{"Time":"2026-10-20T10:35:26.353803146Z","Action":"pass","Package":"github.com/EduCloud-Ecosystem/outfitter/cmd/outfitter","Test":"TestExecutionCLIContentRefusalPrecedesInputAndState","Elapsed":0.01}',
        )
        package = ci.MODULE + "/cmd/outfitter"
        gate = ci.Events({package: META})
        gate.event(event("start", package=package))
        for raw in records:
            gate.feed((raw + "\n").encode())
        gate.event(event("pass", package=package))
        gate.finish()
        self.assertNotIn((package, "TestExecutionCLIContentRefusalPrecedesInputAndState/prepare"), gate.tests)

    def test_same_name_two_packages_keeps_both_summaries(self):
        other = ci.MODULE + "/internal/other"
        gate = ci.Events({ci.BROKER: META, other: META})
        for package in (ci.BROKER, other):
            for value in (event("start", package=package), event("run", "TestSame", package=package),
                          event("pass", "TestSame", package=package), event("pass", package=package)):
                gate.event(value)
        gate.finish()
        self.assertEqual(gate.summary()["parents"], {ci.BROKER: {"TestSame": "pass"}, other: {"TestSame": "pass"}})

    def test_malformed_typed_events_refuse_before_failure_formatting(self):
        for bad in (event("fail", True), {"Action": "output", "Package": []},
                    event("run", ["TestOne"]), event("run", "")):
            gate = ci.Events({ci.BROKER: META})
            gate.event(event("start"))
            with self.subTest(bad=bad), self.assertRaises(ci.Refused):
                gate.event(bad)

    def test_failure_stays_failure_and_panic_word_is_ordinary_log(self):
        gate = ci.Events({ci.BROKER: META}, ["TestOne"])
        for value in (event("start"), event("run", "TestOne"),
                      event("output", "TestOne", Output="    fixture.go:1: expected panic: input rejected\n"),
                      event("fail", "TestOne"), event("pass")):
            gate.event(value)
        with self.assertRaises(ci.Refused):
            gate.finish()
        self.assertEqual(gate.summary()["parents"][ci.BROKER]["TestOne"], "fail")
        ordinary = ci.Events({ci.BROKER: META}, ["TestOne"])
        for value in (event("start"), event("run", "TestOne"),
                      event("output", "TestOne", Output="    fixture.go:1: expected panic: input rejected\n"),
                      event("pass", "TestOne"), event("pass")):
            ordinary.event(value)
        ordinary.finish()
        complete(ci.Events({ci.BROKER: META}, ["ExampleOne"]), ["ExampleOne"])

    def test_no_test_files_is_separate_from_test_pass(self):
        gate = ci.Events({ci.BROKER: {}})
        gate.event(event("start"))
        gate.event(event("skip"))
        gate.finish()
        self.assertEqual(gate.summary()["packages"][ci.BROKER]["terminal"], "skip")
        self.assertEqual(gate.summary()["packages"][ci.BROKER]["classification"], "NO_TEST_FILES")
        gate = ci.Events({ci.BROKER: META})
        gate.event(event("start"))
        with self.assertRaises(ci.Refused):
            gate.event(event("skip"))

    def test_effective_flags_and_candidate_binding(self):
        root = Path("/fixed-candidate")
        value = dict.fromkeys(ci.ENV_KEYS, "")
        value.update(GOOS="linux", GOMOD=str(root / "go.mod"))
        ci.inherited_environment({})
        ci.effective_environment(value, root)
        for key, replacement in (("GOFLAGS", "-skip=TestImportant"), ("GOWORK", "/workspace/go.work"),
                                 ("GOMOD", "/elsewhere/go.mod")):
            with self.subTest(key=key), self.assertRaises(ci.Refused):
                ci.effective_environment({**value, key: replacement}, root)
        for env in ({"GOFLAGS": "-run=Tiny"}, {"GORACE": "exitcode=0"},
                    {"OUTFITTER_PRIVATE_SESSION_PHASE": "1"}):
            with self.assertRaises(ci.Refused):
                ci.inherited_environment(env)

    def test_package_inventory_duplicates_and_scope(self):
        root = Path("/fixed-candidate")
        values = [{"ImportPath": p, "Dir": str(root / "internal" / str(i))}
                  for i, p in enumerate((ci.BROKER, ci.MODULE + "/internal/other"))]
        raw = b"\n".join(json.dumps(v).encode() for v in values)
        self.assertEqual(list(ci.package_inventory(raw, root)), [v["ImportPath"] for v in values])
        with self.assertRaises(ci.Refused):
            ci.package_inventory(raw + b"\n" + json.dumps(values[0]).encode(), root)
        with self.assertRaises(ci.Refused):
            ci.package_inventory(json.dumps({**values[0], "Dir": "/elsewhere"}).encode(), root)

    def test_artifacts_are_exclusive_and_offsets_preserve_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = ci.Evidence(root)
            first, second = b'{"phase":"first-failed"}\n', b'{"phase":"later"}\n'
            evidence.aggregate.write(first)
            offset = evidence.aggregate.tell()
            evidence.aggregate.write(second)
            evidence.aggregate.flush()
            self.assertEqual(offset, len(first))
            self.assertEqual((root / ".runtime/go-tests.jsonl").read_bytes(), first + second)
            with self.assertRaises(FileExistsError):
                ci.Evidence(root)
            with self.assertRaises(FileExistsError):
                evidence.write("plan.json", {})
            evidence.aggregate.close()
            evidence.ledger.close()


class OriginalIdentityControls(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for key, value in (("P_PID", 1), ("WEXITED", 4), ("WNOHANG", 1), ("WNOWAIT", 16),
                           ("CLD_EXITED", 1), ("CLD_KILLED", 2), ("CLD_DUMPED", 3)):
            self.stack.enter_context(patch.object(ci.os, key, value, create=True))
        self.peek = self.stack.enter_context(patch.object(ci.os, "waitid", return_value=None, create=True))
        self.kill = self.stack.enter_context(patch.object(ci.os, "killpg"))
        self.stack.enter_context(patch.object(ci.signal, "getsignal", return_value=signal.SIG_DFL))
        self.process = SimpleNamespace(pid=24680, wait=Mock(return_value=0))
        self.original = ci.Original(self.process)

    def test_signals_are_before_single_final_reap(self):
        order = []
        self.kill.side_effect = lambda _pid, sig: order.append(sig)
        self.process.wait.side_effect = lambda: order.append("reap") or 0
        self.original.send(signal.SIGTERM)
        self.original.send(signal.SIGKILL)
        self.process.wait.assert_not_called()
        self.peek.return_value = SimpleNamespace(si_pid=24680, si_code=ci.os.CLD_EXITED, si_status=0)
        self.original.peek()
        self.original.reap()
        with self.assertRaises(ci.Refused):
            self.original.send(signal.SIGKILL)
        self.assertEqual(order, [signal.SIGTERM, signal.SIGKILL, "reap"])
        self.process.wait.assert_called_once_with()

    def test_echild_and_incompatible_handler_forbid_signals(self):
        self.peek.side_effect = ChildProcessError(errno.ECHILD, "already reaped")
        with self.assertRaises(ci.Refused):
            self.original.send(signal.SIGTERM)
        with self.assertRaises(ci.Refused):
            self.original.send(signal.SIGKILL)
        self.kill.assert_not_called()
        self.process.wait.assert_not_called()
        other = ci.Original(self.process)
        with patch.object(ci.signal, "getsignal", return_value=signal.SIG_IGN), self.assertRaises(ci.Refused):
            other.send(signal.SIGTERM)
        self.kill.assert_not_called()

    def test_post_reap_residue_is_diagnostic_only(self):
        self.peek.return_value = SimpleNamespace(si_pid=24680, si_code=ci.os.CLD_EXITED, si_status=0)
        self.original.peek()
        self.original.reap()
        self.assertEqual(self.original.diagnostic(), "RESIDUE")
        self.kill.assert_called_once_with(24680, 0)
        with self.assertRaises(ci.Refused):
            self.original.send(signal.SIGTERM)
        self.kill.assert_called_once_with(24680, 0)

    def test_interrupted_or_unexpected_signal_consumption_latches_unknown(self):
        for sig in (signal.SIGTERM, signal.SIGKILL):
            for error in (KeyboardInterrupt("synthetic signal interruption"), RuntimeError("synthetic signal fault")):
                with self.subTest(sig=sig, error=type(error).__name__):
                    original = ci.Original(self.process)
                    self.kill.reset_mock(side_effect=True)
                    if sig == signal.SIGKILL:
                        original.send(signal.SIGTERM)
                    self.kill.side_effect = error
                    with self.assertRaises(ci.Refused):
                        original.send(sig)
                    calls = self.kill.call_count
                    with self.assertRaises(ci.Refused):
                        original.send(signal.SIGKILL)
                    self.assertTrue(original.lost)
                    self.assertEqual(self.kill.call_count, calls)
                    self.process.wait.assert_not_called()

    def test_signal_validation_and_append_faults_latch_unknown(self):
        class InterruptedAppend(list):
            def append(self, _value):
                raise KeyboardInterrupt("synthetic pre-effect consumption interruption")

        self.original.signals = InterruptedAppend()
        with self.assertRaises(ci.Refused):
            self.original.send(signal.SIGTERM)
        self.assertTrue(self.original.lost)
        self.kill.assert_not_called()
        invalid = ci.Original(self.process)
        with self.assertRaises(ci.Refused):
            invalid.send(signal.SIGKILL)
        self.assertTrue(invalid.lost)
        self.kill.assert_not_called()
        self.process.wait.assert_not_called()


class CaptureFailureControls(unittest.TestCase):
    def main_patches(self, stack, root):
        stack.enter_context(patch.object(ci, "__file__", str(root / "scripts/ci_race_partition.py")))
        stack.enter_context(patch.object(ci.os, "chdir"))
        stack.enter_context(patch.object(ci.sys, "platform", "linux"))
        stack.enter_context(patch.object(ci.os, "waitid", create=True))
        stack.enter_context(patch.object(ci.os, "WNOWAIT", 16, create=True))
        stack.enter_context(patch.object(ci, "inherited_environment"))
        stack.enter_context(patch.object(ci.signal, "getsignal", return_value=signal.SIG_DFL))
        stack.enter_context(patch.object(ci.signal, "signal"))
        stack.enter_context(patch.object(ci.subprocess, "Popen", side_effect=AssertionError("unexpected child creation")))

    def test_synthetic_successes_append_exact_raw_streams_with_phase_offsets(self):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            evidence = ci.Evidence(Path(temporary))
            stack.callback(evidence.aggregate.close)
            stack.callback(evidence.ledger.close)
            streams, active, observed = [], {}, []

            class Selector:
                def __init__(self):
                    self.keys = {}

                def register(self, stream, _events, index):
                    self.keys[index] = SimpleNamespace(fd=index, data=index, fileobj=stream)

                def unregister(self, stream):
                    self.keys.pop(0 if stream is active["process"].stdout else 1)

                def get_map(self):
                    return self.keys

                def select(self, _timeout):
                    return [(key, 1) for key in list(self.keys.values())]

                def close(self):
                    pass

            def spawn(*_args, **_kwargs):
                process = SimpleNamespace(pid=24680 + len(observed), stdout=Mock(), stderr=Mock(), wait=Mock(return_value=0))
                active.update(process=process, chunks={0: [streams[len(observed)], b""], 1: [b""]})
                observed.append(process)
                return process

            def peek(*_args):
                if any(active["chunks"].values()):
                    return None
                return SimpleNamespace(si_pid=active["process"].pid, si_code=ci.os.CLD_EXITED, si_status=0)

            stack.enter_context(patch.object(ci.selectors, "DefaultSelector", Selector))
            stack.enter_context(patch.object(ci.subprocess, "Popen", side_effect=spawn))
            stack.enter_context(patch.object(ci.os, "read", side_effect=lambda fd, _n: active["chunks"][fd].pop(0)))
            stack.enter_context(patch.object(ci.os, "waitid", side_effect=peek, create=True))
            for key, value in (("P_PID", 1), ("WEXITED", 4), ("WNOHANG", 1), ("WNOWAIT", 16),
                               ("CLD_EXITED", 1), ("CLD_KILLED", 2), ("CLD_DUMPED", 3)):
                stack.enter_context(patch.object(ci.os, key, value, create=True))
            diagnostic = stack.enter_context(patch.object(ci.os, "killpg", side_effect=ProcessLookupError()))
            stack.enter_context(patch.object(ci.signal, "getsignal", return_value=signal.SIG_DFL))
            stack.enter_context(patch.object(ci.sys, "stdout", SimpleNamespace(buffer=Mock())))
            stack.enter_context(patch.object(ci.sys, "stderr", SimpleNamespace(buffer=Mock())))
            for command, parent in (("R1", "TestExecutionOne"), ("R2", "TestOther")):
                raw = b"".join((json.dumps(v, separators=(",", ":")) + "\n").encode() for v in
                               (event("start"), event("run", parent), event("pass", parent), event("pass")))
                streams.append(raw)
                ci.run(evidence, command, ["go", "synthetic-not-executed"],
                       ci.Events({ci.BROKER: META}, [parent]), runtime=True)
            self.assertEqual((Path(temporary) / ".runtime/go-tests.jsonl").read_bytes(), b"".join(streams))
            self.assertEqual(evidence.outcomes["R1"]["aggregate_offset"], 0)
            self.assertEqual(evidence.outcomes["R2"]["aggregate_offset"], len(streams[0]))
            self.assertTrue(all(x["status"] == "PASS" for x in evidence.outcomes.values()))
            self.assertTrue(all(call.args[1] == 0 for call in diagnostic.call_args_list))

    def test_setup_unexpected_parser_and_caps_use_original_cleanup(self):
        for fault in ("started-record", "register", "register-fast-exit", "signal-interrupt",
                      "return-term", "return-kill", "before-term", "unexpected-parser", "cap",
                      "close-selector", "close-child-stdout", "close-child-stderr", "close-capture-stdout",
                      "close-capture-stderr", "close-terminal-record"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                evidence = ci.Evidence(Path(temporary))
                stack.callback(evidence.aggregate.close)
                stack.callback(evidence.ledger.close)
                if fault in ("started-record", "close-terminal-record"):
                    record = evidence.record
                    def record_fault(value):
                        if value.get("state") == "STARTED":
                            if fault == "started-record":
                                raise RuntimeError("synthetic capture-admission record failure")
                        if value.get("state") == "TERMINAL" and fault == "close-terminal-record":
                            raise OSError("synthetic terminal append failure")
                        return record(value)
                    evidence.record = record_fault
                state = {"code": 0 if fault == "register-fast-exit" else None}
                process = SimpleNamespace(pid=24680, stdout=Mock(), stderr=Mock())
                process.wait = Mock(side_effect=lambda: state["code"])
                keys = {}
                selector = Mock()
                selector.get_map.side_effect = lambda: keys
                close_case = fault.startswith("close-")
                close_calls = []

                def checked_close(label, underlying=None):
                    def close():
                        close_calls.append(label)
                        if underlying is not None:
                            underlying.close()
                        if fault == label:
                            raise OSError("synthetic close failure")
                    return close

                selector.close.side_effect = checked_close("close-selector")
                process.stdout.close.side_effect = checked_close("close-child-stdout")
                process.stderr.close.side_effect = checked_close("close-child-stderr")
                original_open = Path.open

                class Output:
                    def __init__(self, handle, label):
                        self.write, self.flush = handle.write, handle.flush
                        self.close = checked_close(label, handle)

                def capture_open(path, *args, **kwargs):
                    handle = original_open(path, *args, **kwargs)
                    if path.name in ("R1.stdout", "R1.stderr"):
                        return Output(handle, "close-capture-" + path.suffix[1:])
                    return handle

                stack.enter_context(patch.object(Path, "open", capture_open))
                # Mocked time, no busy waiting or additional real test budget.
                clock = itertools.count(0, 0.5)
                stack.enter_context(patch.object(ci.time, "monotonic", side_effect=lambda: next(clock)))

                def register(stream, _events, index):
                    if fault in ("register", "register-fast-exit", "signal-interrupt", "return-term", "return-kill", "before-term"):
                        raise OSError("synthetic register failure")
                    keys[index] = SimpleNamespace(fd=index, data=index, fileobj=stream)

                selector.register.side_effect = register
                selector.select.side_effect = lambda _timeout: [(key, 1) for key in list(keys.values())]
                selector.unregister.side_effect = lambda stream: keys.pop(0 if stream is process.stdout else 1)
                stack.enter_context(patch.object(ci.selectors, "DefaultSelector", return_value=selector))
                popen = stack.enter_context(patch.object(ci.subprocess, "Popen", return_value=process))
                stack.enter_context(patch.object(ci.signal, "getsignal", return_value=signal.SIG_DFL))
                for key, value in (("P_PID", 1), ("WEXITED", 4), ("WNOHANG", 1), ("WNOWAIT", 16),
                                   ("CLD_EXITED", 1), ("CLD_KILLED", 2), ("CLD_DUMPED", 3)):
                    stack.enter_context(patch.object(ci.os, key, value, create=True))

                valid = b"".join((json.dumps(v) + "\n").encode() for v in
                                 (event("start"), event("run", "TestOne"), event("pass", "TestOne"), event("pass")))
                chunks = {0: [valid, b""], 1: [b""]}

                def peek(*_args):
                    if close_case and not any(chunks.values()):
                        state["code"] = 0
                    return None if state["code"] is None else SimpleNamespace(
                        si_pid=24680, si_code=ci.os.CLD_EXITED if state["code"] == 0 else ci.os.CLD_KILLED,
                        si_status=abs(state["code"]))

                order = []

                def kill(_pid, sig):
                    order.append(sig)
                    if sig == 0:
                        raise ProcessLookupError()
                    if fault == "signal-interrupt":
                        raise KeyboardInterrupt("synthetic interrupted signal")
                    if state["code"] is None:
                        state["code"] = -sig

                stack.enter_context(patch.object(ci.os, "waitid", side_effect=peek, create=True))
                stack.enter_context(patch.object(ci.os, "killpg", side_effect=kill))
                if fault in ("return-term", "return-kill", "before-term"):
                    send = ci.Original.send

                    def boundary(original, sig):
                        if fault == "before-term" and sig == signal.SIGTERM:
                            raise KeyboardInterrupt("synthetic pre-effect transition interruption")
                        result = send(original, sig)  # Native primitives remain mocked.
                        if (fault == "return-term" and sig == signal.SIGTERM) or (fault == "return-kill" and sig == signal.SIGKILL):
                            raise KeyboardInterrupt("synthetic interruption at successful send return")
                        return result

                    stack.enter_context(patch.object(ci.Original, "send", boundary))
                stack.enter_context(patch.object(ci.os, "read", side_effect=lambda fd, _n:
                                                chunks[fd].pop(0) if close_case else
                                                (b"" if state["code"] is not None else b'{"Action":"start"}\n')))
                stack.enter_context(patch.object(ci.sys, "stdout", SimpleNamespace(buffer=Mock())))
                stack.enter_context(patch.object(ci.sys, "stderr", SimpleNamespace(buffer=Mock())))
                gate = ci.Events({ci.BROKER: META})
                if fault == "unexpected-parser":
                    gate.feed = Mock(side_effect=TypeError("synthetic unexpected parser failure"))
                if fault == "cap":
                    stack.enter_context(patch.object(ci, "MAX_STDOUT", 4))
                with self.assertRaises(ci.Refused):
                    ci.run(evidence, "R1", ["go", "not-executed"], gate, runtime=True)
                self.assertEqual(evidence.outcomes["R1"]["status"], "FAIL")
                unknown_signal = fault in ("signal-interrupt", "return-term", "return-kill", "before-term")
                expected_order = ([0] if close_case else [] if fault == "before-term" else
                                  [signal.SIGTERM, signal.SIGKILL] if fault == "return-kill" else
                                  [signal.SIGTERM] if unknown_signal else [signal.SIGTERM, signal.SIGKILL, 0])
                self.assertEqual(order, expected_order)
                if unknown_signal:
                    process.wait.assert_not_called()
                    self.assertTrue(evidence.outcomes["R1"]["identity_unknown"])
                    self.assertEqual(evidence.outcomes["R1"]["group_diagnostic"], "UNKNOWN")
                    self.assertFalse(evidence.outcomes["R1"]["original_wait_completed"])
                    self.assertIsNotNone(evidence.outcomes["R1"]["unfinished_signal_transition"])
                else:
                    process.wait.assert_called_once_with()
                self.assertTrue(evidence.outcomes["R1"]["started"])
                self.assertEqual(set(close_calls), {"close-selector", "close-child-stdout", "close-child-stderr",
                                                   "close-capture-stdout", "close-capture-stderr"})
                if close_case:
                    self.assertTrue(evidence.outcomes["R1"]["finalization_errors"])
                if fault == "register-fast-exit":
                    self.assertEqual(evidence.outcomes["R1"]["exit"], 0)
                    self.assertEqual(evidence.outcomes["R1"]["stream_eof"], [False, False])
                self.assertEqual(popen.call_args.kwargs["stdin"], ci.subprocess.DEVNULL)
                self.assertEqual(popen.call_count, 1)
                self.assertNotIn("R2", evidence.outcomes)
                if fault == "cap":
                    self.assertEqual(evidence.outcomes["R1"]["aggregate_bytes"], 4)
                    self.assertEqual((Path(temporary) / ".runtime/go-tests.jsonl").read_bytes(), b'{"Ac')

    def test_unexpected_main_exception_cannot_publish_pass_or_advance(self):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            self.main_patches(stack, root)
            runner = stack.enter_context(patch.object(ci, "run", side_effect=TypeError("synthetic unexpected fault")))
            self.assertEqual(ci.main(), 1)
            self.assertEqual(runner.call_count, 1)
            self.assertEqual(runner.call_args.args[1], "M0")
            summary = json.loads((root / ".runtime/ci-race/summary.json").read_bytes())
            self.assertEqual(summary["status"], "FAIL")
            self.assertIn("TypeError", summary["failure"])
            self.assertTrue(all(x["status"] == "NOT_RUN" for x in summary["commands"].values()))

    def test_main_close_faults_preserve_started_unfinished_command(self):
        for failed in ("aggregate", "ledger"):
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                root = Path(temporary)
                self.main_patches(stack, root)
                constructor, closes = ci.Evidence, []

                class Handle:
                    def __init__(self, handle, label):
                        self.handle, self.label = handle, label

                    def close(self):
                        closes.append(self.label)
                        self.handle.close()
                        if self.label == failed:
                            raise OSError("synthetic evidence close failure")

                def evidence_factory(path):
                    evidence = constructor(path)
                    evidence.aggregate = Handle(evidence.aggregate, "aggregate")
                    evidence.ledger = Handle(evidence.ledger, "ledger")
                    return evidence

                def interrupted(evidence, name, _argv):
                    evidence.outcomes[name] = {"status": "UNFINISHED", "state": "STARTED", "identity_unknown": True}
                    raise ci.Refused("synthetic unfinished current command")

                stack.enter_context(patch.object(ci, "Evidence", side_effect=evidence_factory))
                runner = stack.enter_context(patch.object(ci, "run", side_effect=interrupted))
                self.assertEqual(ci.main(), 1)
                self.assertEqual(runner.call_count, 1)
                self.assertEqual(closes, ["aggregate", "ledger"])
                summary = json.loads((root / ".runtime/ci-race/summary.json").read_bytes())
                self.assertEqual(summary["status"], "FAIL")
                self.assertEqual(summary["commands"]["M0"]["status"], "UNFINISHED")
                self.assertEqual(summary["commands"]["M0"]["state"], "STARTED")
                self.assertEqual(summary["commands"]["M1"]["status"], "NOT_RUN")
                self.assertTrue(summary["finalization_errors"])

    def test_failed_first_broker_command_leaves_complement_and_other_packages_not_run(self):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            self.main_patches(stack, root)
            provenance = dict.fromkeys(ci.ENV_KEYS, "")
            provenance.update(GOOS="linux", GOMOD=str(root / "go.mod"))
            packages = [{"ImportPath": p, "Dir": str(root / "internal" / str(i)), **META}
                        for i, p in enumerate((ci.BROKER, ci.MODULE + "/internal/other"))]
            calls = []

            def simulated(evidence, name, _argv, gate=None, runtime=False):
                calls.append(name)
                evidence.outcomes[name] = {"status": "FAIL" if name == "R1" else "PASS"}
                if name == "M0":
                    return json.dumps(provenance).encode()
                if name == "M1":
                    return b"\n".join(json.dumps(p).encode() for p in packages)
                if name == "M2":
                    gate.names = list(ci.EXPECTED_BROKER)
                    return None
                raise ci.Refused("synthetic failed original capture")

            stack.enter_context(patch.object(ci, "run", side_effect=simulated))
            self.assertEqual(ci.main(), 1)
            self.assertEqual(calls, ["M0", "M1", "M2", "R1"])
            summary = json.loads((root / ".runtime/ci-race/summary.json").read_bytes())
            self.assertEqual(summary["status"], "FAIL")
            self.assertEqual(summary["commands"]["R1"]["status"], "FAIL")
            self.assertEqual(summary["commands"]["R2"]["status"], "NOT_RUN")
            self.assertEqual(len(summary["commands"]["R2"]["parents"][ci.BROKER]), 49)
            self.assertEqual(summary["commands"]["R3"]["status"], "NOT_RUN")


if __name__ == "__main__":
    unittest.main()
