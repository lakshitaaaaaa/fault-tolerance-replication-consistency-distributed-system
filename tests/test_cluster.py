"""End-to-end crash/recovery checks against three real local processes."""
import json, os, socket, subprocess, sys, tempfile, time, urllib.request, urllib.error

from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]; NODE=ROOT/'src'/'fault_tolerance_replication_consistency_ds'/'node.py'

def free_port():
 s=socket.socket(); s.bind(('127.0.0.1',0)); p=s.getsockname()[1]; s.close(); return p
def request(port,path,method='GET',payload=None,timeout=1):
 data=json.dumps(payload).encode() if payload is not None else None
 req=urllib.request.Request(f'http://127.0.0.1:{port}{path}',data=data,headers={'Content-Type':'application/json'} if data else {},method=method)
 try:
  with urllib.request.urlopen(req,timeout=timeout) as r: return r.status,json.loads(r.read())
 except urllib.error.HTTPError as e: return e.code,json.loads(e.read())
def main():
 ports=[free_port() for _ in range(3)]; procs={}; tmp=tempfile.TemporaryDirectory(prefix='raft-test-'); data=Path(tmp.name)
 def launch(i):
  peers=','.join(f'{j+1}=http://127.0.0.1:{ports[j]}' for j in range(3) if j!=i-1)
  procs[i]=subprocess.Popen([sys.executable,str(NODE),'--id',str(i),'--port',str(ports[i-1]),'--peers',peers,'--data-dir',str(data/str(i))],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 def stop(i):
  if i in procs and procs[i].poll() is None: procs[i].terminate(); procs[i].wait(timeout=3)
 def states():
  out=[]
  for i,p in enumerate(ports,1):
   try: out.append((i,request(p,'/health')[1]))
   except Exception: pass
  return out
 def wait_for(fn,timeout=8):
  end=time.time()+timeout
  while time.time()<end:
   x=fn()
   if x: return x
   time.sleep(.12)
  raise AssertionError('condition timed out')
 try:
  for i in range(1,4): launch(i)
  leader=wait_for(lambda: next((i for i,s in states() if s['role']=='leader'),None))
  # Replicate and wait until each follower's committed state catches up.
  status,result=request(ports[leader-1],'/kv','POST',{'key':'phase','value':'replicated'})
  assert status==200 and result['ok'] and result['replicas']>=2, result
  wait_for(lambda: len([1 for _,s in states() if s['values'].get('phase')=='replicated'])==3)
  follower=next(i for i in range(1,4) if i!=leader)
  code,redirect=request(ports[follower-1],'/kv','POST',{'key':'wrong','value':'route'})
  assert code==409 and redirect['error']=='not leader', (code,redirect)
  stop(leader)
  new_leader=wait_for(lambda: next((i for i,s in states() if s['role']=='leader'),None),10)
  assert new_leader!=leader
  code,result=request(ports[new_leader-1],'/kv','POST',{'key':'after-failure','value':'available'})
  assert code==200 and result['ok'], result
  wait_for(lambda: all(s['values'].get('after-failure')=='available' for _,s in states()))
  launch(leader)
  wait_for(lambda: next((s for i,s in states() if i==leader and s['values'].get('phase')=='replicated' and s['values'].get('after-failure')=='available'),None),10)
  print('PASS: elected a leader; majority-committed and replicated a write; rejected follower write; elected replacement after leader crash; continued writes; recovered restarted replica.')
 finally:
  for i in list(procs): stop(i)
  tmp.cleanup()
if __name__=='__main__': main()
