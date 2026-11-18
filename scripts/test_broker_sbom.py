# SPDX-License-Identifier: Apache-2.0
"""Controls for binary provenance, native dependency closure, and tested-byte receipts."""

from contextlib import chdir
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import broker_sbom as sbom

REVISION = 'a' * 40
ELF = """  Class: ELF64
  Data: 2's complement, little endian
  Machine: Advanced Micro Devices X86-64
"""


class BrokerSbomTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.enterContext(chdir(root))
        self.enterContext(patch.object(sbom, 'ROOT', root))
        sbom.BINARY.parent.mkdir()
        sbom.BINARY.write_bytes(b'exercised broker')
        sbom.OUTPUT.mkdir(parents=True)
        self.value = {'revision': REVISION, 'binary': sbom.hashes(sbom.BINARY),
                      'build_info': {'GoVersion': 'go1.26.11'},
                      'native': {'linkage': 'static', 'binary_elf': {}, 'libraries': []}}
        sbom.write_json(sbom.OUTPUT / 'provenance.json', self.value)

    def receipt(self, suite):
        report = sbom.OUTPUT / f'{suite}-report.json'
        sbom.write_json(report, {'image': 'pinned', 'passed': 1, 'checks': [{'passed': True}]})
        sbom.write_json(sbom.OUTPUT / f'{suite}-receipt.json',
                        {'suite': suite, 'image': 'pinned', 'revision': REVISION,
                         'binary': self.value['binary'], 'report': sbom.hashes(report)})

    def test_build_info_requires_source_platform_toolchain_and_module_agreement(self):
        settings = {'GOOS': 'linux', 'GOARCH': 'amd64', 'CGO_ENABLED': '1', 'vcs': 'git',
                    'vcs.revision': REVISION, 'vcs.modified': 'false'}
        info = {'Path': sbom.MODULE + '/cmd/outfitter', 'Main': {'Path': sbom.MODULE},
                'GoVersion': 'go1.26.11',
                'Settings': [{'Key': key, 'Value': value} for key, value in settings.items()]}
        info['Settings'].append({'Key': 'CGO_CFLAGS'})
        env = {**settings, 'GOVERSION': 'go1.26.11', 'GOFLAGS': '', 'GOWORK': '',
               'GOMOD': str(sbom.ROOT / 'go.mod')}
        graph = [{'Path': sbom.MODULE, 'Main': True}]
        sbom.verify_build(info, graph, env, REVISION)
        for field, value in (('GOARCH', 'arm64'), ('vcs.revision', 'b' * 40),
                             ('vcs.modified', 'true'), ('CGO_ENABLED', '0')):
            with self.subTest(field=field):
                changed = copy.deepcopy(info)
                next(s for s in changed['Settings'] if s['Key'] == field)['Value'] = value
                with self.assertRaises(ValueError):
                    sbom.verify_build(changed, graph, env, REVISION)
        for changed in ({**info, 'GoVersion': 'go1.26.10'},
                        {**info, 'Path': sbom.MODULE + '/another'},
                        {**info, 'Deps': [{'Path': 'foreign', 'Version': 'v1', 'Sum': 'h1:other'}]}):
            with self.assertRaises(ValueError):
                sbom.verify_build(changed, graph, env, REVISION)

    def test_native_resolution_refuses_missing_or_foreign_linkage(self):
        dynamic = ELF + (' [Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]\n'
                         ' (NEEDED) Shared library: [libc.so.6]\n')
        self.assertEqual(sbom.elf_metadata(dynamic)['needed'], ['libc.so.6'])
        resolved = ('linux-vdso.so.1 (0x123)\nlibc.so.6 => /lib/libc.so.6 (0x456)\n'
                    '/lib64/ld-linux-x86-64.so.2 (0x789)\n')
        self.assertEqual(len(sbom.resolved_libraries(resolved, '/lib64/ld-linux-x86-64.so.2')), 2)
        for changed in (dynamic.replace('X86-64', 'AArch64'), dynamic + '(RPATH) path: [/tmp]'):
            with self.assertRaises(ValueError):
                sbom.elf_metadata(changed)
        for changed in (resolved.replace('/lib/libc.so.6 (0x456)', 'not found'),
                        resolved.replace('/lib64/ld-linux-x86-64.so.2 (0x789)', '')):
            with self.assertRaises(ValueError):
                sbom.resolved_libraries(changed, '/lib64/ld-linux-x86-64.so.2')

    def test_native_closure_and_package_ownership_are_required(self):
        library = sbom.ROOT / 'libc.so.6'
        loader = sbom.ROOT / 'ld-linux.so.2'
        library.write_bytes(b'native libc')
        loader.write_bytes(b'native loader')
        dynamic = ELF + f' [Requesting program interpreter: {loader}]\n (NEEDED) Shared library: [libc.so.6]\n'
        ldd = f'libc.so.6 => {library} (0x123)\n{loader} (0x456)\n'
        package = {'name': 'libc6:amd64', 'version': '2.43-1', 'architecture': 'amd64'}
        libc_elf = ELF + f' [Requesting program interpreter: {loader}]\n (NEEDED) Shared library: [ld-linux.so.2]\n'
        with (patch.dict(os.environ, {}, clear=True),
              patch.object(sbom, 'run', side_effect=[dynamic, ldd, ELF, libc_elf]),
              patch.object(sbom, 'native_package', return_value=package)):
            result = sbom.inspect_native(sbom.BINARY)
        self.assertEqual(result['linkage'], 'dynamic')
        self.assertEqual(len(result['libraries']), 2)
        libc = next(lib for lib in result['libraries'] if lib['soname'] == 'libc.so.6')
        self.assertEqual(libc['hashes'], sbom.hashes(library))
        with (patch.dict(os.environ, {}, clear=True),
              patch.object(sbom, 'run', side_effect=[dynamic, ldd, ELF + '(NEEDED) Shared library: [absent.so]\n', ELF]),
              patch.object(sbom, 'native_package', return_value=package)):
            with self.assertRaisesRegex(ValueError, 'closure'):
                sbom.inspect_native(sbom.BINARY)
        with patch.object(sbom.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', '')):
            with self.assertRaisesRegex(ValueError, 'package owner'):
                sbom.native_package(str(library))

    def test_static_linkage_is_observed_and_does_not_infer_native_packages(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(sbom, 'run', return_value=ELF) as run:
            self.assertEqual(sbom.inspect_native(sbom.BINARY)['linkage'], 'static')
            self.assertEqual(run.call_count, 1)
        sbom.verify_linkage({'CGO_ENABLED': '0'}, {'linkage': 'static'})
        sbom.verify_linkage({'CGO_ENABLED': '1'}, {'linkage': 'dynamic'})
        with self.assertRaisesRegex(ValueError, 'CGO libraries'):
            sbom.verify_linkage({'CGO_ENABLED': '1'}, {'linkage': 'static'})

    def test_changed_binary_or_source_cannot_get_acceptance_receipt(self):
        with patch.object(sbom, 'run', return_value=REVISION + '\n'):
            self.assertEqual(sbom.snapshot(), self.value)
            sbom.BINARY.write_bytes(b'rebuilt broker')
            with self.assertRaisesRegex(ValueError, 'binary changed'):
                sbom.snapshot()
        with patch.object(sbom, 'run', return_value='b' * 40):
            with self.assertRaisesRegex(ValueError, 'source changed'):
                sbom.snapshot()

    def test_exercise_binds_successful_suite_and_detects_mid_suite_binary_change(self):
        def suite(_command, **_kwargs):
            sbom.write_json(sbom.OUTPUT / 'docker-report.json',
                            {'image': 'pinned', 'passed': 1, 'checks': [{'passed': True}]})
        with (patch.object(sbom, 'run', return_value=REVISION),
              patch.object(sbom.subprocess, 'run', side_effect=suite)):
            sbom.exercise('docker', 'pinned')
        receipt = json.loads((sbom.OUTPUT / 'docker-receipt.json').read_text())
        self.assertEqual(receipt['binary'], self.value['binary'])
        (sbom.OUTPUT / 'docker-receipt.json').unlink()
        (sbom.OUTPUT / 'docker-report.json').unlink()

        def replaced(command, **kwargs):
            suite(command, **kwargs)
            sbom.BINARY.write_bytes(b'foreign binary')
        with (patch.object(sbom, 'run', return_value=REVISION),
              patch.object(sbom.subprocess, 'run', side_effect=replaced)):
            with self.assertRaisesRegex(ValueError, 'binary changed'):
                sbom.exercise('docker', 'pinned')
        self.assertFalse((sbom.OUTPUT / 'docker-receipt.json').exists())

    def test_missing_failed_and_changed_reports_refuse_final_inventory(self):
        with self.assertRaises(FileNotFoundError):
            sbom.verify_receipts(self.value)
        for suite in sbom.SUITES:
            self.receipt(suite)
        sbom.verify_receipts(self.value)
        (sbom.OUTPUT / 'recovery-report.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'report changed'):
            sbom.verify_receipts(self.value)
        for report in ({'checks': [], 'passed': 0},
                       {'checks': [{'passed': False}], 'passed': 1},
                       {'checks': [{'passed': True}], 'passed': 0}):
            with self.assertRaisesRegex(ValueError, 'incomplete or failed'):
                sbom.verify_report(report)

    def test_finish_retains_tested_binary_and_digest_bound_spdx(self):
        for suite in sbom.SUITES:
            self.receipt(suite)
        with (patch.object(sbom, 'run', return_value=REVISION),
              patch.object(sbom, 'inspect_native', return_value=self.value['native'])):
            sbom.finish()
        self.assertEqual((sbom.OUTPUT / 'outfitter').read_bytes(), sbom.BINARY.read_bytes())
        doc = json.loads((sbom.OUTPUT / 'outfitter-linux-amd64.spdx.json').read_text())
        broker = doc['packages'][0]
        self.assertEqual(broker['checksums'][1]['checksumValue'], self.value['binary']['sha256'])
        self.assertTrue(broker['filesAnalyzed'])
        verification_code = sbom.hashlib.sha1(self.value['binary']['sha1'].encode()).hexdigest()
        self.assertEqual(broker['packageVerificationCode']['packageVerificationCodeValue'], verification_code)
        self.assertTrue(broker['sourceInfo'].endswith(REVISION))
        self.assertEqual(doc['packages'][1]['versionInfo'], 'go1.26.11')
        for line in (sbom.OUTPUT / 'SHA256SUMS').read_text().splitlines():
            digest, name = line.split()
            self.assertEqual(sbom.hashes(sbom.OUTPUT / name)['sha256'], digest)

    def test_native_files_are_dependencies_and_go_modules_are_contained(self):
        value = copy.deepcopy(self.value)
        value['build_info']['Deps'] = [{'Path': 'example.org/module', 'Version': 'v1.2.3', 'Sum': 'h1:example'}]
        value['native']['libraries'] = [{'package': {'name': 'libc6:amd64', 'version': '2.43-1'},
                                        'resolved': '/usr/lib/libc.so.6', 'hashes': self.value['binary']}]
        doc = sbom.document(value, '2026-11-17T20:00:00Z')
        relationships = {(r['spdxElementId'], r['relationshipType'], r['relatedSpdxElement']) for r in doc['relationships']}
        self.assertIn(('SPDXRef-Outfitter', 'DEPENDS_ON', 'SPDXRef-Native-1'), relationships)
        self.assertIn(('SPDXRef-Outfitter', 'CONTAINS', 'SPDXRef-Go-1'), relationships)


if __name__ == '__main__':
    unittest.main()
