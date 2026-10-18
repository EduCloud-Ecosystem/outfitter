#!/usr/bin/env python3
"""Bounded actual Docker evidence for a Cairn-shaped synthetic fixture, not Cairn wiring."""
import argparse
import copy
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('--binary', default='bin/outfitter')
parser.add_argument('--image', required=True)
parser.add_argument('--output', default='.runtime/consumer-acceptance.json')
args = parser.parse_args()
binary = str(Path(args.binary).resolve())
# Cooperative local serialization; no host restart, image pull or broad cleanup.
lock = open('/tmp/educloud-docker-acceptance.lock', 'a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
root = (Path('.runtime') / ('consumer-' + uuid.uuid4().hex)).resolve()
root.mkdir(parents=True, mode=0o700)
state = root / 'state'
checks, controls, processes, request_ids = [], [], [], []
started = time.monotonic()
sha = lambda data: hashlib.sha256(data).hexdigest()

def check(name, value):
    checks.append({'name': name, 'passed': bool(value)})
    if not value:
        raise AssertionError(name)

def invoke(command, *flags, directory=None, failure=False):
    result = subprocess.run([binary, '--state-dir', str(directory or state), command, *flags],
                            capture_output=True, text=True, timeout=45)
    data = json.loads(result.stdout)
    if failure:
        check(command + ' refuses changed contract', result.returncode == 2 and 'error' in data)
    elif result.returncode:
        raise AssertionError((command, result.returncode, data))
    return data

def content_manifest(directory):
    return [{'path': p.relative_to(directory).as_posix(), 'size': p.stat().st_size,
             'sha256': sha(p.read_bytes())} for p in sorted(directory.rglob('*')) if p.is_file()]

def request(job, answer=42, sleep=False, project='synthetic'):
    base = root / job
    for name in ['input', 'recipe']:
        (base / name).mkdir(parents=True)
    (base / 'input' / 'value.txt').write_text(str(answer) + '\n')
    (base / 'recipe' / 'check.py').write_text(
        'import json, pathlib, sys, time\n'
        'if len(sys.argv) > 2: time.sleep(60)\n'
        'print(json.dumps({"answer": int(pathlib.Path(sys.argv[1]).read_text())}))\n')
    command = ['python', '/recipe/check.py', '/input/value.txt'] + (['sleep'] if sleep else [])
    req = dict(schema_version=1, consumer='cairn-synthetic-v1', project_id=project,
               job_id=job, attempt=1, submission_commit='a' * 40, policy_commit='b' * 40,
               policy_manifest_sha256=sha(b'synthetic policy v1'),
               input_files=content_manifest(base / 'input'), recipe_files=content_manifest(base / 'recipe'),
               workload=dict(schema_version=1, image=args.image, command=command,
                             input_dir=str(base / 'input'), recipe_dir=str(base / 'recipe'),
                             cpus=1, memory_mb=256, pids_limit=64, timeout_seconds=30,
                             max_output_bytes=4096, isolation='reviewed-local', allow_supervised_reaper=True))
    return req

def write_request(req, suffix=''):
    path = root / (req['job_id'] + suffix + '.json')
    path.write_text(json.dumps(req))
    return path

def preflight(req):
    result = invoke('consumer-preflight', '--request', str(write_request(req)))
    request_ids.append(result['request_id'])
    return result

def status(req, directory=None):
    return invoke('consumer-status', '--consumer', req['consumer'], '--project-id', req['project_id'],
                  '--job-id', req['job_id'], directory=directory)

def run(req, suffix=''):
    return invoke('consumer-run', '--request', str(write_request(req, suffix)))

def launch(req):
    proc = subprocess.Popen([binary, '--state-dir', str(state), 'consumer-run', '--request', str(write_request(req))],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(proc)
    return proc

def finish(proc):
    out, err = proc.communicate(timeout=40)
    if proc.returncode:
        raise AssertionError((proc.returncode, err, out))
    return json.loads(out)

def absent(request_id):
    owner = json.loads((state / 'ledger.json').read_text())['owner']
    name = 'outfitter-' + sha((owner + ':' + request_id).encode())[:32]
    found = subprocess.run(['docker', 'container', 'ls', '--all', '--filter', 'name=^/' + name + '$',
                            '--format', '{{.Names}}'], capture_output=True, text=True, timeout=10)
    return found.returncode == 0 and not found.stdout.strip()

outcome, problem = 'PASS', None
try:
    inspected = subprocess.run(['docker', 'image', 'inspect', args.image], capture_output=True, text=True, timeout=10)
    check('pinned image already installed', inspected.returncode == 0)
    good = request('correct')
    declared = preflight(good)
    check('preflight is pure and does not initialize authority', not state.exists() and declared['reserved_cpu_seconds'] == 30)
    invoke('project-init', '--project-id', 'synthetic', '--budget-cpu-seconds', '150')
    first = run(good)
    check('correct fixture compute and explicit deterministic result', first['status'] == 'succeeded'
          and json.loads(first['stdout']) == {'answer': 42} and first['cleanup_verified'] and not first['grade_published'])
    controls.append({'fixture': 'correct', 'expected_answer': 42, 'observed_answer': 42, 'deterministic_match': True,
                     'compute_status': first['status'], 'grade_published': False})
    shutil.rmtree(good['workload']['input_dir'])
    shutil.rmtree(good['workload']['recipe_dir'])
    replay = run(good)
    check('same binding replays receipt with original input paths gone', replay['reused'] and replay['binding_sha256'] == first['binding_sha256']
          and replay['compute'] == first['compute'] and replay['stdout'] == first['stdout'])
    for label, change in [('policy', lambda r: r.update(policy_commit='c' * 40)),
                          ('input', lambda r: r['input_files'][0].update(sha256=sha(b'43\n'))),
                          ('limits', lambda r: r['workload'].update(memory_mb=128))]:
        changed = copy.deepcopy(good)
        change(changed)
        invoke('consumer-run', '--request', str(write_request(changed, '-' + label)), failure=True)
    wrong = request('wrong', answer=41)
    preflight(wrong)
    observed = run(wrong)
    wrong_answer = json.loads(observed['stdout'])['answer']
    match = observed['status'] == 'succeeded' and wrong_answer == 42
    controls.append({'fixture': 'wrong', 'expected_answer': 42, 'observed_answer': wrong_answer, 'deterministic_match': match,
                     'compute_status': observed['status'], 'grade_published': False})
    check('wrong-answer control remains a failed result despite successful compute', observed['status'] == 'succeeded' and wrong_answer == 41 and not match and not observed['grade_published'])
    concurrent = request('concurrent')
    preflight(concurrent)
    one = launch(concurrent)
    two = launch(concurrent)
    receipts = [finish(one), finish(two)]
    check('concurrent duplicate has one fresh admission', sum(not r['reused'] for r in receipts) == 1
          and len({r['binding_sha256'] for r in receipts}) == 1)
    final = status(concurrent)
    check('concurrent job ends with one 30-second charge', final['cleanup_verified'] and final['compute']['charged_cpu_seconds'] == 30)
    crash = request('killed', sleep=True)
    crash_id = preflight(crash)['request_id']
    proc = launch(crash)
    until = time.monotonic() + 10
    running = None
    while time.monotonic() < until:
        try:
            running = status(crash)
        except AssertionError:
            time.sleep(.1)
            continue
        if running['execution_started']:
            break
        time.sleep(.1)
    check('killed-consumer fixture reached execution', running is not None and running['execution_started'])
    os.kill(proc.pid, signal.SIGKILL)
    proc.communicate(timeout=5)
    shutil.rmtree(crash['workload']['input_dir'])
    shutil.rmtree(crash['workload']['recipe_dir'])
    until = time.monotonic() + 12
    while time.monotonic() < until:
        recovered = status(crash)
        if recovered['cleanup_verified'] and not recovered['unresolved_liability']:
            break
        time.sleep(.2)
    check('fresh process resolves killed consumer receipt and watchdog cleanup', recovered['binding_sha256'] == running['binding_sha256']
          and recovered['cleanup_verified'] and not recovered['unresolved_liability'])
    check('independent Docker observation confirms killed resource absent', absent(crash_id))
    recovered_replay = run(crash)
    check('killed consumer replay never dispatches again', recovered_replay['reused'] and not recovered_replay['grade_published']
          and recovered_replay['compute']['charged_cpu_seconds'] == 30 and absent(crash_id))
    replaced = request('replaced')
    replaced_id = preflight(replaced)['request_id']
    Path(replaced['workload']['input_dir'], 'value.txt').write_text('43\n')
    refused = run(replaced)
    check('same-path replacement after preflight refuses before launch', refused['status'] == 'failed'
          and not refused['execution_started'] and 'snapshot' in refused['error'] and absent(replaced_id))
    invoke('project-init', '--project-id', 'exhausted', '--budget-cpu-seconds', '1')
    limited = request('budget', project='exhausted')
    limited_id = preflight(limited)['request_id']
    refused = run(limited)
    check('insufficient allowance creates no job or resource', not refused['admitted'] and refused['compute']['reserved_cpu_seconds'] == 0
          and limited_id not in json.loads((state / 'ledger.json').read_text())['runs'] and absent(limited_id))
    usage = invoke('project-status', '--project-id', 'synthetic')
    check('finite harness budget exhausted by exactly five reservations', usage['budget_cpu_seconds'] == 150
          and usage['charged_cpu_seconds'] == 150 and usage['remaining_cpu_seconds'] == 0 and usage['reserved_cpu_seconds'] == 0)
    cp = root / 'checkpoint'
    saved = invoke('checkpoint', '--output', str(cp))
    dest = root / 'restored'
    restored = invoke('restore', '--checkpoint', str(cp), '--checkpoint-sha256', saved['checkpoint_sha256'], directory=dest)
    before = (dest / 'ledger.json').read_bytes()
    held = status(good, directory=dest)
    check('held restore preserves receipt binding and read-only status', restored['admission'] == 'held'
          and held['binding_sha256'] == first['binding_sha256'] and held['stdout'] == first['stdout']
          and (dest / 'ledger.json').read_bytes() == before)
    record = json.loads(before)['runs'][first['request_id']]
    (dest / 'outputs' / (record['output_sha256'] + '.json')).unlink()
    missing = status(good, directory=dest)
    check('missing diagnostics remain visible without changing accounting', missing['output_error']
          and missing['reconciliation_required'] and missing['compute'] == first['compute']
          and (dest / 'ledger.json').read_bytes() == before)
except Exception as exc:
    outcome, problem = 'FAIL', str(exc)
finally:
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate(timeout=5)
    cleanup = []
    if (state / 'ledger.json').exists():
        for request_id in sorted(set(request_ids)):
            try:
                record = json.loads((state / 'ledger.json').read_text()).get('runs', {}).get(request_id)
                if record and (not record['cleanup_verified'] or record['unresolved_liability']):
                    response = subprocess.run([binary, '--state-dir', str(state), 'cancel', '--request-id', request_id],
                                              capture_output=True, text=True, timeout=45)
                    receipt = json.loads(response.stdout)
                    cleanup.append({'request_id': request_id, 'exit_code': response.returncode, 'receipt': receipt})
                    if response.returncode or not receipt.get('cleanup_verified') or receipt.get('unresolved_liability'):
                        outcome, problem = 'FAIL', problem or 'cleanup remains unresolved'
            except Exception as exc:
                cleanup.append({'request_id': request_id, 'error': str(exc)})
                outcome, problem = 'FAIL', problem or 'cleanup observation failed'
    result = dict(schema_version=1, timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  outcome=outcome, error=problem, image=args.image, scope='local synthetic consumer contract; Cairn-shaped fixture only',
                  concurrency=1, per_job_limits=dict(cpus=1, memory_mb=256, pids_limit=64, timeout_seconds=30),
                  aggregate_budget_cpu_seconds=151, checks=checks, passed=sum(c['passed'] for c in checks),
                  deterministic_controls=controls, emergency_cleanup=cleanup, elapsed_seconds=round(time.monotonic()-started, 3),
                  live_jetstream2='BLOCKED pending separate approval; no calls/resources/spending',
                  limitations=['No production Cairn wiring, real student data, course admission or grade publication',
                               'No native/provider qualification or host/watchdog/daemon-loss self-expiry',
                               'Conservative CPU-second reservations, not measured CPU or provider SUs',
                               'Mock ambiguity controls are in the Go test suite, not live provider evidence'])
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'outcome': outcome, 'passed': result['passed'], 'output': str(output), 'runtime': str(root), 'error': problem}))
    fcntl.flock(lock, fcntl.LOCK_UN)
if outcome != 'PASS':
    raise SystemExit(1)
