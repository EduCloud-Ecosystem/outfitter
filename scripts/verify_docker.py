#!/usr/bin/env python3
"""Real local Docker lifecycle acceptance. Never pulls images or broadly cleans resources."""
import argparse
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

p = argparse.ArgumentParser()
p.add_argument('--binary', default='bin/outfitter')
p.add_argument('--image', required=True)
p.add_argument('--output', default='.runtime/docker-conformance.json')
a = p.parse_args()
binary = str(Path(a.binary).resolve())
root = Path('.runtime') / ('verify-' + uuid.uuid4().hex)
root.mkdir(parents=True, mode=0o700)
root = root.resolve()
(root/'input').mkdir(); (root/'recipe').mkdir()
state = root/'state'
checks = []
def check(name, result):
    checks.append({'name': name, 'passed': bool(result)})
    if not result: raise AssertionError(name)
def call(command, *args, fail=False):
    result = subprocess.run([binary, '--state-dir', str(state), command, *args], capture_output=True, text=True)
    data = json.loads(result.stdout)
    if not fail: check(command+' returns JSON success', result.returncode == 0)
    return data
call('project-init', '--project-id', 'test', '--budget-cpu-seconds', '180')
def request(name, code, timeout=15, output=4096):
    r = dict(schema_version=1,request_id=name,project_id='test',image=a.image,
      command=['python','-c',code],input_dir=str(root/'input'),recipe_dir=str(root/'recipe'),
      cpus=1,memory_mb=128,pids_limit=32,timeout_seconds=timeout,max_output_bytes=output,
      isolation='reviewed-local',allow_supervised_reaper=True)
    path=root/(name+'.json');path.write_text(json.dumps(r));return path
normal=request('normal','import json,os; print(json.dumps({"answer":6*7,"uid":os.getuid()}))')
r=call('run','--request',str(normal));check('known result and nonroot user',json.loads(r['stdout'])=={'answer':42,'uid':65532});check('completion cleanup and conservative charge',r['cleanup_verified'] and not r['unresolved_liability'] and r['compute']['charged_cpu_seconds']==15)
r2=call('run','--request',str(normal));check('duplicate returns original receipt',r==r2)
u=call('project-status','--project-id','test');check('duplicate charged only once',u['charged_cpu_seconds']==15)
noisy=request('output','print("x"*100000)',output=100)
r=call('run','--request',str(noisy));check('bounded combined output',len(r['stdout'])+len(r['stderr'])<=100 and r['status']=='failed' and r['cleanup_verified'])
slow=request('timeout','import time; time.sleep(60)',timeout=3)
r=call('run','--request',str(slow));check('deadline cleanup',r['status'] in ('failed','cancelled') and r['cleanup_verified'])

def running(name, timeout=8):
    path=request(name,'import time; time.sleep(60)',timeout=timeout)
    proc=subprocess.Popen([binary,'--state-dir',str(state),'run','--request',str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    end=time.monotonic()+6
    while time.monotonic()<end:
        status=call('status','--request-id',name,fail=True)
        if status.get('status')=='running':
            ledger=json.loads((state/'ledger.json').read_text())
            container=ledger['runs'][name]['container_name']
            inspected=subprocess.run(['docker','container','inspect',container],capture_output=True,text=True)
            if inspected.returncode==0:
                obj=json.loads(inspected.stdout)[0]
                if obj['State']['Running']: return proc,container,obj
        time.sleep(.1)
    raise AssertionError('workload did not start')
proc,container,obj=running('cancel',15)
host=obj['HostConfig'];check('Docker containment flags observed',host['NetworkMode']=='none' and host['ReadonlyRootfs'] and host['CapDrop']==['ALL'] and host['PidsLimit']==32 and host['Memory']==128*1024*1024 and host['RestartPolicy']['Name']=='no' and obj['Config']['User']=='65532:65532')
r=call('cancel','--request-id','cancel');proc.communicate(timeout=15);check('operator cancel cleanup',r['cleanup_verified'] and r['status']=='cancelled')
proc,container,obj=running('crash',6)
os.kill(proc.pid,signal.SIGKILL);proc.communicate(timeout=10)
end=time.monotonic()+15
while time.monotonic()<end:
    r=call('status','--request-id','crash')
    if r['cleanup_verified']:break
    time.sleep(.3)
check('watchdog survives broker SIGKILL',r['cleanup_verified'] and not r['unresolved_liability'])
observed=subprocess.run(['docker','container','ls','--all','--filter','name=^/'+container+'$','--format','{{.Names}}'],capture_output=True,text=True)
check('fresh observer verifies killed-broker resource absent',observed.returncode==0 and observed.stdout.strip()=='')
call('project-init','--project-id','small','--budget-cpu-seconds','1')
limited=json.loads(normal.read_text());limited.update(request_id='budget-refused',project_id='small');path=root/'refused.json';path.write_text(json.dumps(limited))
r=call('run','--request',str(path));check('budget refused before admission',not r['admitted'] and r['cleanup_verified'] and not r['unresolved_liability'] and r['compute']['charged_cpu_seconds']==0)
call('project-init','--project-id','parallel','--budget-cpu-seconds','30')
processes=[]
for i in range(3):
    req=json.loads(normal.read_text());req.update(request_id='parallel-'+str(i),project_id='parallel')
    path=root/('parallel-'+str(i)+'.json');path.write_text(json.dumps(req))
    processes.append(subprocess.Popen([binary,'--state-dir',str(state),'run','--request',str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
receipts=[json.loads(proc.communicate(timeout=30)[0]) for proc in processes]
check('separate processes cannot double reserve remaining budget',sum(bool(r.get('admitted')) for r in receipts)==2)
u=call('project-status','--project-id','parallel');check('cross-process budget exhausted exactly once per reservation',u['charged_cpu_seconds']==30 and u['remaining_cpu_seconds']==0)
result={'schema_version':1,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'image':a.image,'checks':checks,'passed':sum(c['passed'] for c in checks),'provider_self_expiry':'unsupported: supervised external watchdog exception only','limitations':['No Jetstream2 or other live provider acceptance','No host/watchdog/Docker-daemon loss self-expiry','CPU allowance conservatively charged; actual CPU not measured']}
Path(a.output).parent.mkdir(parents=True,exist_ok=True);Path(a.output).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'passed':result['passed'],'output':a.output}))
