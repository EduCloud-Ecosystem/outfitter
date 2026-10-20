"""Pure synthetic parser/state controls: never starts Go or another process."""

from contextlib import ExitStack
import errno
import hashlib
import itertools
import io
import json
from pathlib import Path
import signal
import shutil
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
        self.assertEqual(len(ci.EXPECTED_BROKER), 107)
        self.assertEqual(len(set(ci.EXPECTED_BROKER)), 107)
        self.assertEqual([len(g) for g in selection["groups"]], [58, 49])
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
        self.assertEqual(ci.EXPECTED_BROKER[position - 3:position + 3],
                         ("TestExecutionPrivateSessionPhaseHelper",
                          "TestExecutionSessionSeedBoundaryControls",
                          "TestExecutionPrivateSessionSeedHelper",
                          "TestExecutionStopFirstDenialACKAndGenerationGuards",
                          "TestExecutionStopAlreadyDeniedUsesCommittedState",
                          "TestExecutionWatcherStopsActiveDeniedOrExpiredAttempt"))

    def test_seed_additions_preserve_all_prior_105_assignments(self):
        additions = (ci.SEED_BOUNDARY, ci.SEED_HELPER)
        prior = [name for name in ci.EXPECTED_BROKER if name not in additions]
        self.assertEqual(len(prior), 105)
        self.assertEqual(hashlib.sha256(("\n".join(prior) + "\n").encode()).hexdigest(),
                         "176757991a5d474336d9ceec2f101977b8e2ab783569449ff91e313b8a75f1ba")
        self.assertNotIn("TestExecutionSessionSeedUnsupportedTarget", ci.EXPECTED_BROKER)
        groups = ci.partition(list(ci.EXPECTED_BROKER))["groups"]
        for name in additions:
            self.assertEqual(ci.EXPECTED_BROKER.count(name), 1)
            self.assertEqual(groups[0].count(name), 1)
            self.assertNotIn(name, groups[1])
        self.assertEqual([n for n in groups[0] if n not in additions],
                         [n for n in prior if n.startswith("TestExecution") and
                          n not in ci.COMPLEMENT_EXECUTION_PARENTS])
        self.assertEqual(groups[1], [n for n in prior if not n.startswith("TestExecution") or
                                    n in ci.COMPLEMENT_EXECUTION_PARENTS])

    def test_only_named_parents_move_and_owned_group_stays_whole(self):
        moved = (
            "TestExecutionCleanupLateUncertaintyCannotPublishAbsence",
            "TestExecutionFinalResultFreshObservationRejectsLateUncertainty",
            "TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold",
            "TestExecutionFinalResultTerminalPublicationFaults",
            "TestExecutionDeferredCleanupStoppedACKGuards",
        )
        new_execution = ("TestExecutionStopFirstDenialACKAndGenerationGuards",
                         "TestExecutionStopAlreadyDeniedUsesCommittedState",
                         "TestExecutionSessionSeedBoundaryControls",
                         "TestExecutionPrivateSessionSeedHelper")
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



class SeedArtifactControls(unittest.TestCase):
    """Synthetic files/events only; all process/signal/wait entrypoints denied."""

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for module, names in ((ci.subprocess, ("Popen", "run", "check_output", "call", "check_call")),
                              (ci.os, ("kill", "killpg", "waitid", "waitpid")),
                              (ci.signal, ("signal",))):
            for name in names:
                self.stack.enter_context(patch.object(module, name,
                    side_effect=AssertionError("artifact verifier attempted process/signal/wait")))
        self.temporary = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.root = Path(self.temporary).resolve()
        self.directory = self.root / ci.SEED_RAW_DIRECTORY
        self.stage = self.root / ci.SEED_DIRECTORY
        (self.directory / "metadata").mkdir(parents=True)
        source_root = Path(ci.__file__).resolve().parent.parent
        for name in ci.SEED_ASSETS:
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((source_root / name).read_bytes())
        self.names = sorted(ci.SEED_ASSETS)
        for suffix in ("before", "after"):
            self.meta("source-id." + suffix + ".txt", ("1" * 40 + "\n" + "2" * 40 + "\n").encode())
            self.meta("tracked." + suffix + ".nul", ("\0".join(self.names) + "\0").encode())
            self.meta("status." + suffix + ".txt", b"")
        self.binary = bytearray(128)
        self.binary[:7] = b"\x7fELF\x02\x01\x01"
        self.binary[18:20] = (62).to_bytes(2, "little")
        (self.directory / "broker.test").write_bytes(self.binary)
        for name in ("export.stdout.txt", "export.stderr.txt"):
            (self.directory / name).write_bytes(b"")
        (self.directory / "export.exit.txt").write_bytes(b"0\n")
        self.pair = {"bytes": len(self.binary), "sha256": hashlib.sha256(self.binary).hexdigest()}
        self.producer = ("    assessment_session_driver_test.go:651: external seed executable: sha256=" +
                         self.pair["sha256"] + " bytes=" + str(self.pair["bytes"]) + "\n")
        self.race = self.root / ".runtime/ci-race"
        self.race.mkdir()
        self.provenance = {"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "1", "GOVERSION": "go1.26.1",
                           "GOFLAGS": "", "GOWORK": "off", "GOMOD": str(self.root / "go.mod")}
        self.packages = {ci.BROKER: {"ImportPath": ci.BROKER, "Dir": str(self.root / "internal/broker"),
                                    **META},
                         ci.MODULE + "/internal/fixture": {"ImportPath": ci.MODULE + "/internal/fixture",
                            "Dir": str(self.root / "internal/fixture"), **META}}
        self.selection = ci.partition(list(ci.EXPECTED_BROKER))
        flags = ["go", "test", "-race", "-json", "-count=1", "-timeout=600s"]
        self.argv = {"M0": ["go", "env", "-json", *ci.ENV_KEYS], "M1": ["go", "list", "-json", "./..."],
                     "M2": [*flags, "-list=.", "./internal/broker"],
                     "R1": [*flags, "-run=" + self.selection["patterns"][0], "./internal/broker"],
                     "R2": [*flags, "-run=" + self.selection["patterns"][1], "./internal/broker"],
                     "R3": [*flags, ci.MODULE + "/internal/fixture"]}
        self.outcomes, self.events = {}, {}
        self.capture("M0", json.dumps(self.provenance).encode())
        self.capture("M1", b"\n".join(json.dumps(p).encode() for p in self.packages.values()))
        listed = [event("start"), event("output", Output="\n".join(ci.EXPECTED_BROKER) + "\n"),
                  event("output", Output="ok\t" + ci.BROKER + "\t0.001s\n"), event("pass")]
        self.capture_events("M2", listed, ci.Events({ci.BROKER: self.packages[ci.BROKER]}, listing=True))
        for name, parents in zip(("R1", "R2"), self.selection["groups"]):
            values = [event("start")]
            for parent in parents:
                values.append(event("run", parent))
                if parent == ci.SEED_POSITIVE:
                    values.append(event("output", parent, Output=self.producer))
                if parent == ci.SEED_BOUNDARY:
                    for leaf in ci.SEED_LEAVES:
                        values.extend((event("run", parent + "/" + leaf), event("pass", parent + "/" + leaf)))
                values.append(event("pass", parent))
            values.append(event("pass"))
            self.capture_events(name, values, ci.Events({ci.BROKER: self.packages[ci.BROKER]}, parents))
        package = ci.MODULE + "/internal/fixture"
        self.capture_events("R3", [event("start", package=package), event("run", "TestOther", package),
                                    event("pass", "TestOther", package), event("pass", package=package)],
                            ci.Events({package: self.packages[package]}))
        self.summary = {"status": "PASS", "failure": None, "commands": self.outcomes,
                        "finalization_errors": [], "remaining_coverage": "COMPLETE",
                        "downstream_acceptance": "STILL_REQUIRED", "margin": "UNKNOWN"}
        self.save("environment.json", {**self.provenance, "GORACE": None, "GOMAXPROCS": None})
        self.save("packages.json", self.packages)
        self.save("partition.json", self.selection)
        self.save_summary()

    def verify(self):
        # Reset only this test's synthetic staging between independent controls.
        # Production never removes/adopts an existing raw or retained root.
        for path in (self.stage, self.directory / "retention"):
            if path.exists():
                shutil.rmtree(path)
        return ci.verify_seed_export(self.root)

    def meta(self, name, raw):
        (self.directory / "metadata" / name).write_bytes(raw)

    def save(self, name, value):
        (self.race / name).write_text(json.dumps(value) + "\n")

    def save_summary(self):
        self.save("summary.json", self.summary)

    def capture(self, name, raw, coverage=None):
        (self.race / (name + ".stdout")).write_bytes(raw)
        (self.race / (name + ".stderr")).write_bytes(b"")
        self.outcomes[name] = {"command": name, "kind": "METADATA" if name.startswith("M") else "RACE",
            "state": "TERMINAL", "status": "PASS", "argv": self.argv[name], "exit": 0,
            "identity_unknown": False, "group_diagnostic": "ABSENT", "original_wait_completed": True,
            "stream_eof": [True, True], "unfinished_signal_transition": None, "spawn_attempted": True,
            "started": True, "finalization_errors": [], "reason": None,
            "stored_bytes": [len(raw), 0], "seen_bytes": [len(raw), 0],
            "sha256": [hashlib.sha256(raw).hexdigest(), hashlib.sha256(b"").hexdigest()],
            "aggregate_offset": 0 if name.startswith("R") else None,
            "aggregate_bytes": len(raw) if name.startswith("R") else 0, "coverage": coverage}

    def capture_events(self, name, values, gate):
        self.events[name] = values
        for value in values:
            gate.event(value)
        gate.finish()
        self.capture(name, b"".join((json.dumps(value) + "\n").encode() for value in values), gate.summary())

    def rewrite_events(self, name, values):
        # Keep the previous valid coverage summary, while binding hashes to the
        # changed stream: semantic refusals must not be mere checksum failures.
        old_coverage = self.outcomes[name]["coverage"]
        self.capture(name, b"".join((json.dumps(value) + "\n").encode() for value in values), old_coverage)
        self.save_summary()

    def test_exact_exercised_binary_and_ten_assets_are_retained(self):
        receipt = self.verify()
        self.assertEqual(receipt["status"], "MATCHED_BINARY")
        self.assertEqual(receipt["producer"]["sha256"], self.pair["sha256"])
        self.assertEqual(receipt["export"], self.pair)
        self.assertEqual(receipt["checkout_head"], "1" * 40)
        self.assertNotEqual(receipt["checkout_head"], "6cccd6533213ad0cc91e0a23e3067b72161e11ac")
        self.assertEqual(receipt["checkout_tree"], "2" * 40)
        self.assertEqual(receipt["full_ci_acceptance"], "ROOT_REVIEW_REQUIRED")
        self.assertEqual(receipt["downstream_acceptance"], "STILL_REQUIRED")
        self.assertFalse(receipt["compiler_attestation"])
        self.assertEqual(receipt["go_sum"], "ABSENT")
        self.assertEqual(len(receipt["assets"]), 10)
        for name in ("internal/broker/testdata/execution-session-a2-manifest.json",
                     "internal/broker/testdata/execution-session-a2-input.txt"):
            self.assertEqual((self.stage / "assets" / name).read_bytes(), (self.root / name).read_bytes())
        self.assertEqual(receipt["build_argv"], ci.SEED_EXPORT_ARGV)
        self.assertEqual(ci.COMMANDS, ("M0", "M1", "M2", "R1", "R2", "R3"))
        previous = (self.stage / "association.json").read_bytes()
        with self.assertRaises(FileExistsError):
            ci.verify_seed_export(self.root)  # Never overwrite a prior receipt.
        self.assertEqual((self.stage / "association.json").read_bytes(), previous)

    def test_producer_context_and_closed_driver_prefix(self):
        original = self.events["R1"]
        index = next(i for i, v in enumerate(original) if v.get("Output") == self.producer)
        for label, change in (
                ("wrong-test", {"Test": "TestExecutionSessionPhaseClosedBranchControls"}),
                ("wrong-package", {"Package": ci.MODULE + "/internal/fixture"}),
                ("wrong-source", {"Output": self.producer.replace("driver_test.go:651", "seed_test.go:287")}),
                ("wrong-line", {"Output": self.producer.replace(":651:", ":652:")}),
                ("extra-text", {"Output": self.producer.rstrip("\n") + " extra\n"}),
                ("uppercase", {"Output": self.producer.replace(self.pair["sha256"], "A" * 64)}),
                ("bad-size", {"Output": self.producer.replace("bytes=128", "bytes=true")})):
            values = [dict(value) for value in original]
            values[index].update(change)
            self.rewrite_events("R1", values)
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__), "unlisted event package" if label == "wrong-package" else "wrong/malformed executable producer")
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
            self.assertFalse((self.stage / "association.json").exists())

    def test_missing_duplicate_and_unfinished_positive_refuse(self):
        original = self.events["R1"]
        producer = next(v for v in original if v.get("Output") == self.producer)
        for label, values in (
                ("missing", [v for v in original if v is not producer]),
                ("duplicate", [v for v in original for _ in range(2 if v is producer else 1)]),
                ("failed-positive", [{**v, "Action": "fail"} if v.get("Test") == ci.SEED_POSITIVE and
                                      v["Action"] == "pass" else v for v in original]),
                ("unfinished", [v for v in original if not (v.get("Test") == ci.SEED_POSITIVE and v["Action"] == "pass")]),
                ("failed-boundary-leaf", [{**v, "Action": "fail"} if v.get("Test") == ci.SEED_BOUNDARY + "/" + ci.SEED_LEAVES[0]
                                           and v["Action"] == "pass" else v for v in original])):
            self.rewrite_events("R1", values)
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__), {
                "missing": "missing executable producer", "duplicate": "duplicate/oversize executable producer",
                "failed-positive": "Go FAIL: " + ci.SEED_POSITIVE,
                "unfinished": "unfinished test at package terminal",
                "failed-boundary-leaf": "Go FAIL: " + ci.SEED_BOUNDARY + "/" + ci.SEED_LEAVES[0],
            }[label])
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
            self.assertFalse((self.stage / "association.json").exists())

    def test_all_six_completed_outcomes_required(self):
        original = json.loads(json.dumps(self.summary))
        for label in ("FAIL", "NOT_RUN", "missing", "unknown-owner", "argv", "typed-event"):
            self.summary = json.loads(json.dumps(original))
            if label in ("FAIL", "NOT_RUN"):
                self.summary["commands"]["R3"]["status"] = label
            elif label == "missing":
                del self.summary["commands"]["R2"]
            elif label == "unknown-owner":
                self.summary["commands"]["R1"]["identity_unknown"] = True
            elif label == "argv":
                self.summary["commands"]["R2"]["argv"].append("-skip=TestOwned")
            else:
                values = [dict(v) for v in self.events["R1"]]
                values[1] = {**values[1], "Action": "fail", "Test": True}
                self.rewrite_events("R1", values)
            self.save_summary()
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__), {
                "FAIL": "failed/unknown command outcome", "NOT_RUN": "failed/unknown command outcome",
                "missing": "missing/extra command outcome", "unknown-owner": "failed/unknown command outcome",
                "argv": "artifact command argv drift", "typed-event": "invalid test identity type",
            }[label])
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
            self.assertFalse((self.stage / "association.json").exists())

    def test_same_source_does_not_excuse_binary_hash_size_or_elf_mismatch(self):
        path = self.directory / "broker.test"
        for label, raw in (("hash", bytes(self.binary[:-1]) + b"x"),
                           ("size", bytes(self.binary) + b"x"),
                           ("elf", b"bad!" + bytes(self.binary[4:])),
                           ("architecture", bytes(self.binary[:18]) + (183).to_bytes(2, "little") + bytes(self.binary[20:]))):
            path.write_bytes(raw)
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__), "export differs from exercised seed executable" if label in ("hash", "size") else "export target ELF identity")
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
            self.assertFalse((self.stage / "association.json").exists())

    def test_checkout_metadata_drift_and_unexpected_go_sum_refuse(self):
        before = (self.directory / "metadata/source-id.before.txt").read_bytes()
        tracked = (self.directory / "metadata/tracked.before.nul").read_bytes()
        for label in ("head", "tree", "list", "dirty", "go.sum", "unknown-file"):
            self.meta("source-id.after.txt", before)
            self.meta("tracked.after.nul", tracked)
            self.meta("status.after.txt", b"")
            (self.root / "go.sum").unlink(missing_ok=True)
            (self.directory / "extra.txt").unlink(missing_ok=True)
            if label in ("head", "tree"):
                self.meta("source-id.after.txt", before.replace(b"1" if label == "head" else b"2", b"3"))
            elif label == "list":
                self.meta("tracked.after.nul", tracked + b"extra\0")
            elif label == "dirty":
                self.meta("status.after.txt", b" M go.mod\n")
            elif label == "go.sum":
                (self.root / "go.sum").write_bytes(b"")
            else:
                (self.directory / "extra.txt").write_bytes(b"unexpected")
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__), {
                "head": "checkout identity drift/shape", "tree": "checkout identity drift/shape",
                "list": "tracked inventory drift/shape", "dirty": "dirty/untracked source",
                "go.sum": "unexpected go.sum", "unknown-file": "unknown/link/special artifact entry",
            }[label])
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")

    def test_source_asset_drift_or_missing_embedded_bytes_refuse(self):
        name = "internal/broker/testdata/execution-session-a2-input.txt"
        path = self.root / name
        original = path.read_bytes()
        path.write_bytes(original + b"changed")
        with self.assertRaises(ci.Refused) as changed:
            self.verify()
        self.assertIsInstance(changed.exception.__cause__, ci.Refused)
        self.assertEqual(str(changed.exception.__cause__), "pinned asset/source drift")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
        self.assertFalse((self.stage / "assets").exists())
        path.unlink()
        with self.assertRaises(ci.Refused) as missing:
            self.verify()
        self.assertIsInstance(missing.exception.__cause__, FileNotFoundError)
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["reason_type"], "FileNotFoundError")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
        self.assertFalse((self.stage / "association.json").exists())

    def test_symlink_special_and_oversize_refuse_before_reading(self):
        path = self.directory / "broker.test"
        path.unlink()
        path.symlink_to(self.root / "go.mod")
        with self.assertRaises(ci.Refused) as link:
            self.verify()
        self.assertEqual(str(link.exception.__cause__), "unknown/link/special artifact entry")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
        path.unlink()
        path.write_bytes(self.binary)
        original_lstat = Path.lstat
        def special(candidate):
            return SimpleNamespace(st_mode=ci.stat.S_IFIFO) if candidate == path else original_lstat(candidate)
        with patch.object(Path, "lstat", special), self.assertRaises(ci.Refused) as special_file:
            self.verify()
        self.assertEqual(str(special_file.exception.__cause__), "unknown/link/special artifact entry")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
        with patch.object(ci, "SEED_BINARY_MAX", 127), self.assertRaises(ci.Refused) as producer_cap:
            self.verify()
        # This whole-verifier case reaches producer admission before file stat.
        self.assertEqual(str(producer_cap.exception.__cause__), "duplicate/oversize executable producer")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
        # Independently reach the file-cap guard with the same inert128-byte ELF.
        with patch.object(ci.os, "open", side_effect=AssertionError("cap refusal must precede open")) as opened:
            with self.assertRaisesRegex(ci.Refused, "^artifact file shape/cap$"):
                ci.seed_stream(self.root, ci.SEED_RAW_DIRECTORY + "/broker.test", 127)
            opened.assert_not_called()
        self.assertFalse((self.stage / "association.json").exists())

    def test_archive_component_and_total_limits_refuse_without_receipt(self):
        for label, name, value in (("asset", "SEED_ASSETS_MAX", 1), ("total", "SEED_TOTAL_MAX", 1)):
            with self.subTest(label=label), patch.object(ci, name, value), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
            self.assertEqual(str(rejected.exception.__cause__),
                             "asset aggregate cap" if label == "asset" else "artifact total cap")
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")
            self.assertFalse((self.stage / "association.json").exists())
        with (self.directory / "export.stderr.txt").open("wb") as handle:
            handle.truncate(1048577)  # Sparse fixture; no large output allocation.
        with self.assertRaises(ci.Refused) as log_cap:
            self.verify()
        self.assertEqual(str(log_cap.exception.__cause__), "artifact file shape/cap")
        self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "INPUT_QUALIFICATION")

    def test_unknown_mode_and_unexpected_exception_never_start_go(self):
        with patch.object(ci, "main", side_effect=AssertionError("unknown mode reached controller")) as controller, \
             patch.object(ci, "seed_export_main", side_effect=AssertionError("unknown mode reached verifier")) as verifier, \
             patch.object(ci.sys, "stderr", io.StringIO()):
            self.assertEqual(ci.entry(["--verify-j1-seed-export", "unexpected"]), 1)
            controller.assert_not_called()
            verifier.assert_not_called()
        with patch.object(ci, "verify_seed_export", side_effect=RuntimeError("synthetic metadata fault")) as verify, \
             patch.object(ci.sys, "stderr", io.StringIO()) as stderr:
            self.assertEqual(ci.seed_export_main(), 1)
            verify.assert_called_once()
            self.assertIn("RuntimeError: synthetic metadata fault", stderr.getvalue())


    def test_missing_invalid_and_nonzero_compiler_exit_never_qualify(self):
        path = self.directory / "export.exit.txt"
        for label, raw, expected in (("missing", None, None), ("invalid", b"00\n", None),
                                     ("nonzero", b"2\n", 2), ("signal-status", b"137\n", 137)):
            if raw is None:
                path.unlink(missing_ok=True)
            else:
                path.write_bytes(raw)
            with self.subTest(label=label), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            cause = rejected.exception.__cause__
            if label == "missing":
                self.assertIsInstance(cause, FileNotFoundError)
            else:
                self.assertIsInstance(cause, ci.Refused)
                self.assertEqual(str(cause), "unknown/invalid compiler exit" if label == "invalid" else
                                 "compiler did not succeed")
            failed = json.loads((self.stage / "failure.json").read_bytes())
            self.assertEqual(failed["status"], "FAILED")
            self.assertEqual(failed["phase"], "INPUT_QUALIFICATION")
            self.assertEqual(failed["compiler_exit"], expected)
            self.assertEqual(failed["compiler_exit_state"], "UNKNOWN" if expected is None else "KNOWN")
            self.assertFalse((self.stage / "broker.test").exists())
            self.assertFalse((self.stage / "assets").exists())
            self.assertFalse((self.stage / "association.json").exists())

    def test_failed_compiler_stages_only_bounded_known_diagnostics(self):
        (self.directory / "export.exit.txt").write_bytes(b"1\n")
        (self.directory / "export.stdout.txt").write_bytes(b"bounded compiler diagnostic\n")
        with (self.directory / "export.stderr.txt").open("wb") as handle:
            handle.truncate(1048577)
        (self.directory / "metadata/status.after.txt").unlink()
        opened, original_open = [], ci.os.open
        def observed_open(path, *args, **kwargs):
            if Path(path) == self.directory / "export.stderr.txt":
                opened.append(path)
                raise AssertionError("oversize log must never be opened/hashed")
            return original_open(path, *args, **kwargs)
        with patch.object(ci.os, "open", observed_open), self.assertRaises(ci.Refused) as rejected:
            self.verify()
        self.assertIsInstance(rejected.exception.__cause__, ci.Refused)
        self.assertEqual(str(rejected.exception.__cause__), "compiler did not succeed")
        self.assertEqual(opened, [])
        failed = json.loads((self.stage / "failure.json").read_bytes())
        self.assertEqual(failed["phase"], "INPUT_QUALIFICATION")
        self.assertEqual(failed["compiler_exit"], 1)
        items = {item["path"]: item for item in failed["evidence"]}
        self.assertEqual(items["export.stderr.txt"], {"path": "export.stderr.txt", "status": "OMITTED_CAP",
                                                    "bytes": 1048577, "sha256": None})
        self.assertEqual(items["metadata/status.after.txt"]["status"], "MISSING")
        self.assertEqual(items["export.stdout.txt"]["status"], "RETAINED")
        self.assertEqual((self.stage / "export.stdout.txt").read_bytes(), b"bounded compiler diagnostic\n")
        self.assertEqual((self.stage / "export.exit.txt").read_bytes(), b"1\n")
        self.assertFalse((self.stage / "export.stderr.txt").exists())
        self.assertFalse((self.stage / "broker.test").exists())
        workflow = (Path(ci.__file__).resolve().parent.parent / ".github/workflows/broker.yml").read_text()
        uploads = workflow.split("- name: Save sanitized local acceptance evidence", 1)[1]
        self.assertIn(".runtime/j1-seed-helper/", uploads)
        self.assertNotIn(".runtime/j1-seed-export", uploads)
        self.assertEqual(workflow.count("go test -race -json -count=1 -timeout=600s -c"), 1)

    def test_short_copy_checked_close_and_destination_hash_faults_have_no_success_receipt(self):
        original_open, original_stream = Path.open, ci.seed_stream
        for fault in ("short-copy", "close-copy", "destination-hash"):
            reached = []
            def output_open(path, *args, **kwargs):
                handle = original_open(path, *args, **kwargs)
                if path == self.directory / "retention/broker.test" and args and args[0] == "xb":
                    class Output:
                        def __enter__(inner):
                            return inner
                        def write(inner, block):
                            if fault == "short-copy":
                                reached.append(fault)
                                return handle.write(block[:-1])
                            return handle.write(block)
                        def __exit__(inner, *_):
                            handle.close()
                            if fault == "close-copy":
                                reached.append(fault)
                                raise OSError("synthetic checked copy close")
                    return Output()
                return handle
            def changed_hash(root, name, *args, **kwargs):
                result = original_stream(root, name, *args, **kwargs)
                if fault == "destination-hash" and root == self.stage and name == "broker.test":
                    reached.append(fault)
                    return {**result[0], "sha256": "0" * 64}, result[1]
                return result
            with self.subTest(fault=fault), patch.object(Path, "open", output_open), \
                 patch.object(ci, "seed_stream", changed_hash), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertEqual(reached, [fault])
            expected_type, expected_reason = {
                "short-copy": (ci.Refused, "short artifact copy"),
                "close-copy": (OSError, "synthetic checked copy close"),
                "destination-hash": (ci.Refused, "published artifact hash/close"),
            }[fault]
            self.assertIsInstance(rejected.exception.__cause__, expected_type)
            self.assertEqual(str(rejected.exception.__cause__), expected_reason)
            self.assertFalse((self.stage / "association.json").exists())
            failed = json.loads((self.stage / "failure.json").read_bytes())
            self.assertEqual(failed["status"], "FAILED")
            self.assertEqual(failed["phase"], "COPY_OR_PUBLICATION")

    def test_success_receipt_is_last_and_publication_errors_cannot_qualify(self):
        original_rename, original_open = ci.os.rename, Path.open
        for fault in ("before-rename", "after-rename", "receipt-short", "receipt-close"):
            reached = []
            def rename(source, destination, *args, **kwargs):
                if Path(destination) == self.stage / "association.json":
                    # Every other archive file must already be complete before
                    # the official success filename can be published.
                    self.assertTrue((self.stage / "source-inventory.json").exists())
                    for name in ci.SEED_ASSETS:
                        self.assertEqual((self.stage / "assets" / name).read_bytes(), (self.root / name).read_bytes())
                    if fault == "before-rename":
                        reached.append(fault)
                        raise OSError("synthetic pre-publication fault")
                    result = original_rename(source, destination, *args, **kwargs)
                    if fault == "after-rename":
                        reached.append(fault)
                        raise KeyboardInterrupt("synthetic publication return-boundary fault")
                    return result
                return original_rename(source, destination, *args, **kwargs)
            def output_open(path, *args, **kwargs):
                handle = original_open(path, *args, **kwargs)
                if path == self.directory / "retention/association.json" and args and args[0] == "xb":
                    class Output:
                        def __enter__(inner):
                            return inner
                        def write(inner, raw):
                            if fault == "receipt-short":
                                reached.append(fault)
                            return handle.write(raw[:-1] if fault == "receipt-short" else raw)
                        def __exit__(inner, *_):
                            handle.close()
                            if fault == "receipt-close":
                                reached.append(fault)
                                raise OSError("synthetic success receipt close")
                    return Output()
                return handle
            with self.subTest(fault=fault), patch.object(ci.os, "rename", rename), \
                 patch.object(Path, "open", output_open), self.assertRaises(ci.Refused) as rejected:
                self.verify()
            self.assertEqual(reached, [fault])
            expected_type, expected_reason = {
                "before-rename": (OSError, "synthetic pre-publication fault"),
                "after-rename": (KeyboardInterrupt, "synthetic publication return-boundary fault"),
                "receipt-short": (ci.Refused, "short artifact write"),
                "receipt-close": (OSError, "synthetic success receipt close"),
            }[fault]
            self.assertIsInstance(rejected.exception.__cause__, expected_type)
            self.assertEqual(str(rejected.exception.__cause__), expected_reason)
            self.assertFalse((self.stage / "association.json").exists())
            self.assertEqual(json.loads((self.stage / "failure.json").read_bytes())["phase"], "COPY_OR_PUBLICATION")


if __name__ == "__main__":
    unittest.main()
