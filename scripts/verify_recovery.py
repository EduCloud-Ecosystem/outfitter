#!/usr/bin/env python3
"""Bounded local checkpoint/restore acceptance. No cloud calls or ledger edits."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--binary', default='bin/outfitter')
parser.add_argument('--image', required=True)
parser.add_argument('--output', default='.runtime/recovery-acceptance.json')
args = parser.parse_args()
binary = str(Path(args.binary).resolve())
runtime = Path('.runtime')
runtime.mkdir(mode=0o700, exist_ok=True)
root = Path(tempfile.mkdtemp(prefix='recovery-', dir=runtime)).resolve()
source, restored = root / 'source', root / 'restored'
checks = []
start = time.monotonic()

def check(name, passed):
    checks.append({'name': name, 'passed': bool(passed)})
    if not passed:
        raise AssertionError(name)

def call(state, command, *flags, fail=False):
    process = subprocess.run([binary, '--state-dir', str(state), command, *map(str, flags)], capture_output=True, text=True, timeout=80)
    data = json.loads(process.stdout)
    check(command + (' refuses safely' if fail else ' returns successful JSON'), process.returncode != 0 if fail else process.returncode == 0)
    return data

(root / 'input').mkdir()
(root / 'recipe').mkdir()
def request(name, answer):
    data = dict(schema_version=1, request_id=name, project_id='study', image=args.image,
                command=['python', '-c', 'import json; print(json.dumps({"answer":' + str(answer) + '}))'],
                input_dir=str(root / 'input'), recipe_dir=str(root / 'recipe'), cpus=1,
                memory_mb=128, pids_limit=32, timeout_seconds=30, max_output_bytes=4096,
                isolation='reviewed-local', allow_supervised_reaper=True)
    path = root / (name + '.json')
    path.write_text(json.dumps(data))
    os.chmod(path, 0o600)
    return path

call(source, 'project-init', '--project-id', 'study', '--budget-cpu-seconds', 90)
first = request('before-cutover', 42)
receipt = call(source, 'run', '--request', first)
check('real pre-cutover run completed and cleaned up', receipt['status'] == 'succeeded' and receipt['cleanup_verified'] and not receipt['unresolved_liability'] and json.loads(receipt['stdout']) == {'answer': 42})
cp = root / 'checkpoint'
snapshot = call(source, 'checkpoint', '--output', cp)
status = call(source, 'recovery-status')
check('checkpoint matches current accounting authority', snapshot['authority_sha256'] == status['authority_sha256'])
held = call(restored, 'restore', '--checkpoint', cp, '--checkpoint-sha256', snapshot['checkpoint_sha256'])
check('restored authority is held with original allowance', held['admission'] == 'held' and held['projects'][0]['charged_cpu_seconds'] == 30 and held['projects'][0]['remaining_cpu_seconds'] == 60)
check('private recovery directories', cp.stat().st_mode & 0o077 == 0 and restored.stat().st_mode & 0o077 == 0)
check('completed receipt and diagnostic bytes survive restore', call(restored, 'status', '--request-id', 'before-cutover') == receipt)
refusal = call(restored, 'run', '--request', first)
check('held duplicate request cannot execute', refusal['status'] == 'failed' and not refusal['admitted'] and refusal['compute']['charged_cpu_seconds'] == 0)
call(restored, 'project-init', '--project-id', 'study', '--budget-cpu-seconds', 90, fail=True)
call(source, 'project-init', '--project-id', 'later', '--budget-cpu-seconds', 10)
call(restored, 'activate', '--source-state-dir', source, '--checkpoint', cp, '--checkpoint-sha256', snapshot['checkpoint_sha256'], fail=True)
check('stale checkpoint leaves source active', call(source, 'recovery-status')['admission'] == 'active')
cp2 = root / 'fresh-checkpoint'
snapshot2 = call(source, 'checkpoint', '--output', cp2)
destination = root / 'fresh-destination'
call(destination, 'restore', '--checkpoint', cp2, '--checkpoint-sha256', snapshot2['checkpoint_sha256'])
activated = call(destination, 'activate', '--source-state-dir', source, '--checkpoint', cp2, '--checkpoint-sha256', snapshot2['checkpoint_sha256'])
fenced = call(source, 'recovery-status')
check('exactly one authority admits after cutover', activated['admission'] == 'active' and fenced['admission'] == 'fenced' and fenced['fence']['destination_authority_id'] == activated['authority_id'])
call(source, 'project-init', '--project-id', 'study', '--budget-cpu-seconds', 90, fail=True)
refusal = call(source, 'run', '--request', first)
check('source fence also refuses duplicate dispatch', not refusal['admitted'] and refusal['compute']['charged_cpu_seconds'] == 0)
check('activated duplicate returns original receipt without reexecution', call(destination, 'run', '--request', first) == receipt)
second = request('after-cutover', 43)
receipt2 = call(destination, 'run', '--request', second)
check('real post-cutover run completed and cleaned up', receipt2['status'] == 'succeeded' and receipt2['cleanup_verified'] and not receipt2['unresolved_liability'] and json.loads(receipt2['stdout']) == {'answer': 43})
usage = call(destination, 'project-status', '--project-id', 'study')
check('cutover preserved prior charges and charged new work once', usage['charged_cpu_seconds'] == 60 and usage['remaining_cpu_seconds'] == 30)
retry = call(destination, 'activate', '--source-state-dir', source, '--checkpoint', cp2, '--checkpoint-sha256', snapshot2['checkpoint_sha256'])
check('same-pair retry preserves new usage', retry['admission'] == 'active' and next(p for p in retry['projects'] if p['project_id'] == 'study')['charged_cpu_seconds'] == 60)
other = root / 'other-destination'
call(other, 'restore', '--checkpoint', cp2, '--checkpoint-sha256', snapshot2['checkpoint_sha256'])
call(other, 'activate', '--source-state-dir', source, '--checkpoint', cp2, '--checkpoint-sha256', snapshot2['checkpoint_sha256'], fail=True)
check('second activation target stays held', call(other, 'recovery-status')['admission'] == 'held')
report = {'schema_version': 1, 'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'image': args.image, 'checks': checks, 'passed': sum(x['passed'] for x in checks),
          'elapsed_seconds': round(time.monotonic() - start, 3), 'observed_accounting_loss_cpu_seconds': 0,
          'limitations': ['Same local host and filesystem only; no remote disaster recovery or live cloud acceptance',
                          'Synthetic operator-driven exercise; no independent second operator or hosted recovery target',
                          'Unknown jobs remain held until source reconciliation and fresh checkpoint',
                          'Checksums require a separately retained trusted digest; not protection from a privileged rollback']}
output = Path(args.output)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2) + '\n')
os.chmod(output, 0o600)
print(json.dumps({'passed': report['passed'], 'elapsed_seconds': report['elapsed_seconds'], 'output': str(output)}))
