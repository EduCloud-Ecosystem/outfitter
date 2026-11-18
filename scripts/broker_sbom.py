#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bind a broker SPDX inventory to the binary exercised by local CI acceptance."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

MODULE = 'github.com/EduCloud-Ecosystem/outfitter'
ROOT = Path(__file__).resolve().parent.parent
OUTPUT = Path('.runtime/broker-sbom')
BINARY = Path('bin/outfitter')
SUITES = {'docker': 'verify_docker.py', 'recovery': 'verify_recovery.py', 'consumer': 'verify_consumer.py'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(*args):
    return subprocess.check_output(args, text=True, env={**os.environ, 'LC_ALL': 'C'}, timeout=60)


def hashes(path):
    require(path.is_file() and not path.is_symlink(), 'expected a regular file')
    raw = path.read_bytes()
    return {'sha256': hashlib.sha256(raw).hexdigest(), 'sha1': hashlib.sha1(raw).hexdigest()}


def write_json(path, value):
    with path.open('x') as output:
        output.write(json.dumps(value, indent=2, sort_keys=True) + '\n')


def json_stream(raw):
    decoder = json.JSONDecoder()
    records = []
    while raw.strip():
        item, end = decoder.raw_decode(raw.lstrip())
        records.append(item)
        raw = raw.lstrip()[end:]
    return records


def verify_build(info, modules, env, revision):
    require(info['Path'] == MODULE + '/cmd/outfitter', 'wrong Go executable')
    require(info['Main']['Path'] == MODULE and not info['Main'].get('Replace'), 'wrong main module')
    settings = {item['Key']: item.get('Value', '') for item in info['Settings']}
    require(len(settings) == len(info['Settings']), 'duplicate build settings')
    require(settings.get('GOOS') == 'linux' and settings.get('GOARCH') == 'amd64', 'wrong Go platform')
    require(settings.get('vcs') == 'git' and settings.get('vcs.revision') == revision
            and settings.get('vcs.modified') == 'false', 'wrong or dirty source revision')
    require(re.fullmatch(r'go1\.\d+\.\d+', info['GoVersion']), 'unrecognized Go toolchain')
    require(info['GoVersion'] == env['GOVERSION'], 'Go toolchain mismatch')
    require(all(settings.get(key) == env[key] for key in ('GOOS', 'GOARCH', 'CGO_ENABLED')),
            'Go build environment mismatch')
    require(env['CGO_ENABLED'] in ('0', '1') and not env['GOFLAGS']
            and env['GOWORK'] in ('', 'off'), 'unsupported Go overrides')
    require(Path(env['GOMOD']).resolve() == (ROOT / 'go.mod').resolve(), 'foreign module file')
    require(sum(bool(m.get('Main')) for m in modules) == 1
            and any(m.get('Main') and m['Path'] == MODULE for m in modules), 'wrong module graph')
    require(not any(m.get('Replace') for m in modules), 'module replacements require separate provenance')
    expected = {(m['Path'], m['Version'], m['Sum']) for m in modules if not m.get('Main')}
    dependencies = info.get('Deps') or []
    require(not any(m.get('Replace') for m in dependencies), 'replaced embedded module')
    observed = {(m['Path'], m['Version'], m['Sum']) for m in dependencies}
    require(len(observed) == len(dependencies) and observed == expected, 'embedded modules disagree with graph')
    return settings


def elf_metadata(text):
    require(re.search(r'Class:\s+ELF64\b', text)
            and re.search(r'Data:.*little endian', text)
            and re.search(r'Machine:\s+Advanced Micro Devices X86-64\b', text), 'wrong ELF platform')
    require(not re.search(r'\((?:RPATH|RUNPATH)\)', text), 'custom ELF library search path')
    needed = re.findall(r'\(NEEDED\).*Shared library: \[([^\]]+)\]', text)
    require(len(needed) == len(set(needed)) and all('/' not in name for name in needed), 'invalid ELF dependencies')
    interpreters = re.findall(r'Requesting program interpreter: ([^\]]+)\]', text)
    require(len(interpreters) <= 1, 'multiple ELF interpreters')
    return {'needed': sorted(needed), 'interpreter': interpreters[0] if interpreters else None}


def resolved_libraries(text, interpreter):
    paths = {}
    for line in text.splitlines():
        line = line.strip()
        if re.fullmatch(r'linux-vdso\.so\.1 \(0x[0-9a-f]+\)', line):
            continue
        match = re.fullmatch(r'(\S+) => (/\S+) \(0x[0-9a-f]+\)', line)
        if match:
            name, path = match.groups()
        else:
            match = re.fullmatch(r'(/\S+) \(0x[0-9a-f]+\)', line)
            require(match and match[1] == interpreter, 'unresolved or unexpected native library')
            path = match[1]
            name = Path(path).name
        require(name not in paths, 'duplicate native resolution')
        paths[name] = path
    require(interpreter and interpreter in paths.values(), 'missing ELF interpreter resolution')
    return paths


def native_package(path):
    target = Path(path).resolve(strict=True)
    candidates = [path, str(target), '*/' + target.name]
    owners = set()
    for candidate in dict.fromkeys(candidates):
        result = subprocess.run(['dpkg-query', '--search', candidate], capture_output=True, text=True, check=False)
        for line in result.stdout.splitlines():
            owner, separator, owned_path = line.partition(': ')
            if (separator and owned_path.startswith('/')
                    and re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9-]+)?', owner)):
                try:
                    if Path(owned_path).resolve(strict=True) == target:
                        owners.add(owner)
                except OSError:
                    continue
    require(len(owners) == 1, f'native library has no unique Debian package owner: {path} ({sorted(owners)})')
    owner, = owners
    record = run('dpkg-query', '--show', '--showformat=${binary:Package}\t${Version}\t${Architecture}\t${db:Status-Status}', owner)
    name, version, architecture, status = record.split('\t')
    require(name == owner and version and architecture == 'amd64' and status == 'installed',
            'native package is not installed for amd64')
    return {'name': name, 'version': version, 'architecture': architecture}


def inspect_native(binary):
    require(not any(key.startswith('LD_') for key in os.environ), 'dynamic loader environment override')
    inspect = lambda path: elf_metadata(run('readelf', '--wide', '--file-header', '--program-headers', '--dynamic', str(path)))
    metadata = inspect(binary)
    if not metadata['interpreter']:
        require(not metadata['needed'], 'native dependencies without an interpreter')
        return {'linkage': 'static', 'binary_elf': metadata, 'libraries': []}
    paths = resolved_libraries(run('ldd', str(binary.resolve())), metadata['interpreter'])
    libraries = []
    for name, path in sorted(paths.items()):
        resolved = Path(path).resolve(strict=True)
        elf = inspect(resolved)
        require(elf['interpreter'] in (None, metadata['interpreter']),
                'native library uses a different ELF interpreter')
        libraries.append({'soname': name, 'path': path, 'resolved': str(resolved),
                          'hashes': hashes(resolved), 'elf': elf, 'package': native_package(path)})
    required = set(metadata['needed']) | {n for lib in libraries for n in lib['elf']['needed']}
    require(required <= paths.keys(), 'native dependency closure is incomplete')
    return {'linkage': 'dynamic', 'binary_elf': metadata, 'libraries': libraries}


def verify_linkage(settings, native):
    require(native['linkage'] != 'static' or settings['CGO_ENABLED'] == '0',
            'statically linked CGO libraries need separate provenance')


def capture():
    revision = run('git', 'rev-parse', 'HEAD').strip()
    require(re.fullmatch('[0-9a-f]{40}', revision), 'invalid Git revision')
    require(not run('git', 'status', '--porcelain', '--untracked-files=normal'), 'capture needs a clean checkout')
    before = hashes(BINARY)
    info = json.loads(run('go', 'version', '-m', '-json', str(BINARY)))
    env = json.loads(run('go', 'env', '-json', 'GOOS', 'GOARCH', 'CGO_ENABLED', 'GOVERSION', 'GOFLAGS', 'GOWORK', 'GOMOD'))
    modules = json_stream(run('go', 'list', '-m', '-json', 'all'))
    settings = verify_build(info, modules, env, revision)
    native = inspect_native(BINARY)
    verify_linkage(settings, native)
    require(hashes(BINARY) == before, 'binary changed during inspection')
    OUTPUT.mkdir(parents=True)
    write_json(OUTPUT / 'provenance.json', {'revision': revision, 'binary': before,
               'build_info': info, 'modules': modules, 'environment': env, 'native': native})


def snapshot():
    value = json.loads((OUTPUT / 'provenance.json').read_text())
    require(run('git', 'rev-parse', 'HEAD').strip() == value['revision'], 'source changed after capture')
    require(hashes(BINARY) == value['binary'], 'tested binary changed')
    return value


def verify_report(report):
    checks = report['checks']
    require(checks and report['passed'] == len(checks)
            and all(c['passed'] is True for c in checks), 'acceptance report is incomplete or failed')


def exercise(suite, image):
    value = snapshot()
    report_path = OUTPUT / f'{suite}-report.json'
    require(not report_path.exists(), 'acceptance report already exists')
    command = [sys.executable, str(ROOT / 'scripts' / SUITES[suite]), '--binary', str(BINARY),
               '--image', image, '--output', str(report_path)]
    subprocess.run(command, check=True, timeout=180)
    require(snapshot() == value, 'provenance changed during acceptance')
    report = json.loads(report_path.read_text())
    verify_report(report)
    require(report['image'] == image, 'wrong acceptance workload image')
    write_json(OUTPUT / f'{suite}-receipt.json', {'suite': suite, 'binary': value['binary'],
               'report': hashes(report_path), 'revision': value['revision'], 'image': image})


def verify_receipts(value):
    for suite in SUITES:
        receipt = json.loads((OUTPUT / f'{suite}-receipt.json').read_text())
        report = OUTPUT / f'{suite}-report.json'
        require(receipt['suite'] == suite and receipt['revision'] == value['revision']
                and receipt['binary'] == value['binary'], 'acceptance receipt belongs to another binary')
        require(receipt['report'] == hashes(report), 'acceptance report changed')
        data = json.loads(report.read_text())
        verify_report(data)
        require(data['image'] == receipt['image'], 'acceptance image changed')


def document(value, created):
    packages, files, relationships = [], [], []

    def relate(source, kind, target):
        relationships.append({'spdxElementId': source, 'relationshipType': kind, 'relatedSpdxElement': target})

    def package(identifier, name, version, purpose):
        item = {'SPDXID': identifier, 'name': name, 'versionInfo': version,
                'primaryPackagePurpose': purpose, 'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False, 'licenseDeclared': 'NOASSERTION',
                'licenseConcluded': 'NOASSERTION', 'copyrightText': 'NOASSERTION'}
        packages.append(item)
        return item

    def checksums(digests):
        return [{'algorithm': algorithm.upper(), 'checksumValue': digest} for algorithm, digest in sorted(digests.items())]

    def file(identifier, name, digests, parent):
        files.append({'SPDXID': identifier, 'fileName': './' + name.removeprefix('./').lstrip('/'), 'checksums': checksums(digests),
                      'licenseConcluded': 'NOASSERTION', 'licenseInfoInFiles': ['NOASSERTION'],
                      'copyrightText': 'NOASSERTION'})
        relate(parent, 'CONTAINS', identifier)

    broker_id = 'SPDXRef-Outfitter'
    broker = package(broker_id, 'outfitter-linux-amd64', value['revision'], 'APPLICATION')
    broker['checksums'] = checksums(value['binary'])
    broker['sourceInfo'] = f'https://{MODULE}/commit/' + value['revision']
    relate('SPDXRef-DOCUMENT', 'DESCRIBES', broker_id)
    file('SPDXRef-Binary', './outfitter', value['binary'], broker_id)
    package('SPDXRef-Go-Stdlib', 'Go standard library', value['build_info']['GoVersion'], 'LIBRARY')
    relate(broker_id, 'CONTAINS', 'SPDXRef-Go-Stdlib')
    for i, module in enumerate(value['build_info'].get('Deps') or [], 1):
        identifier = f'SPDXRef-Go-{i}'
        item = package(identifier, module['Path'], module['Version'], 'LIBRARY')
        item['sourceInfo'] = 'Go module checksum ' + module['Sum']
        relate(broker_id, 'CONTAINS', identifier)
    native_ids = {}
    for i, library in enumerate(value['native']['libraries'], 1):
        native = library['package']
        key = (native['name'], native['version'])
        if key not in native_ids:
            identifier = f'SPDXRef-Native-{len(native_ids) + 1}'
            native_ids[key] = identifier
            package(identifier, native['name'], native['version'], 'LIBRARY')
            relate(broker_id, 'DEPENDS_ON', identifier)
        file(f'SPDXRef-Native-File-{i}', library['resolved'], library['hashes'], native_ids[key])
    file_hashes = {f['SPDXID']: next(c['checksumValue'] for c in f['checksums'] if c['algorithm'] == 'SHA1') for f in files}
    for item in packages:
        contained = [file_hashes[r['relatedSpdxElement']] for r in relationships
                     if r['spdxElementId'] == item['SPDXID'] and r['relationshipType'] == 'CONTAINS'
                     and r['relatedSpdxElement'] in file_hashes]
        if contained:
            item['filesAnalyzed'] = True
            item['packageVerificationCode'] = {
                'packageVerificationCodeValue': hashlib.sha1(''.join(sorted(contained)).encode()).hexdigest()}
    result = {'spdxVersion': 'SPDX-2.3', 'SPDXID': 'SPDXRef-DOCUMENT', 'dataLicense': 'CC0-1.0',
              'name': 'Outfitter Linux/amd64 ' + value['binary']['sha256'],
              'creationInfo': {'created': created, 'creators': ['Tool: outfitter-broker-sbom-1']},
              'packages': packages, 'files': files, 'relationships': relationships}
    result['documentNamespace'] = f'https://{MODULE}/spdx/' + hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    return result


def finish():
    value = snapshot()
    verify_receipts(value)
    require(inspect_native(BINARY) == value['native'], 'native libraries changed after acceptance')
    shutil.copyfile(BINARY, OUTPUT / 'outfitter')
    require(hashes(OUTPUT / 'outfitter') == value['binary'], 'retained binary differs from tested binary')
    created = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    write_json(OUTPUT / 'outfitter-linux-amd64.spdx.json', document(value, created))
    with (OUTPUT / 'SHA256SUMS').open('x') as output:
        for path in sorted(OUTPUT.iterdir()):
            if path.name != 'SHA256SUMS':
                output.write(f"{hashes(path)['sha256']}  {path.name}\n")
    print('Verified exercised broker SHA-256 ' + value['binary']['sha256'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['capture', 'test', 'finish'])
    parser.add_argument('--suite', choices=SUITES)
    parser.add_argument('--image')
    args = parser.parse_args()
    os.chdir(ROOT)
    try:
        if args.stage == 'capture':
            capture()
        elif args.stage == 'test':
            require(args.suite and args.image, 'test requires suite and image')
            exercise(args.suite, args.image)
        else:
            finish()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Broker SBOM verification failed: {error}\n')


if __name__ == '__main__':
    main()
