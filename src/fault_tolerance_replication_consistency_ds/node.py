"""Educational three-node Raft-style replicated key/value service (stdlib only)."""
import argparse, json, os, random, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

class Node:
    def __init__(self, node_id, peers, data_dir):
        self.id, self.peers, self.data_dir = node_id, peers, data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f'{node_id}.json')
        self.lock = threading.RLock()
        self.state = {'term':0,'voted_for':None,'log':[],'commit_index':-1,'last_applied':-1,'kv':{}}
        if os.path.exists(self.path):
            try:
                with open(self.path) as f: self.state.update(json.load(f))
            except (ValueError, OSError): pass
        self.role='follower'; self.leader_id=None; self.next_index={}; self.match_index={}
        self.last_contact=time.monotonic(); self.election_deadline=self._deadline(); self.last_heartbeat=0
        self.alive=True
        threading.Thread(target=self._ticker, daemon=True).start()
    def _deadline(self): return time.monotonic()+random.uniform(1.1,1.9)
    def persist(self):
        tmp=self.path+'.tmp'
        with open(tmp,'w') as f: json.dump(self.state,f); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,self.path)
    def apply(self):
        while self.state['last_applied'] < self.state['commit_index']:
            self.state['last_applied']+=1; e=self.state['log'][self.state['last_applied']]
            if e['op']=='put': self.state['kv'][e['key']]=e['value']
            elif e['op']=='delete': self.state['kv'].pop(e['key'],None)
    def rpc(self, peer, path, body, timeout=.35):
        data=json.dumps(body).encode(); req=Request(peer+path,data=data,headers={'Content-Type':'application/json'})
        try:
            with urlopen(req,timeout=timeout) as r: return json.loads(r.read())
        except Exception: return None
    def _ticker(self):
        while self.alive:
            time.sleep(.05)
            with self.lock:
                now=time.monotonic()
                if self.role=='leader':
                    if now-self.last_heartbeat>.18:
                        self.last_heartbeat=now; threading.Thread(target=self._replicate_all,daemon=True).start()
                elif now>=self.election_deadline:
                    threading.Thread(target=self._election,daemon=True).start(); self.election_deadline=self._deadline()
    def _election(self):
        with self.lock:
            self.state['term']+=1; term=self.state['term']; self.state['voted_for']=self.id
            self.role='candidate'; self.leader_id=None; self.persist()
            last_i=len(self.state['log'])-1; last_t=self.state['log'][last_i]['term'] if last_i>=0 else 0
        votes=1
        for peer in self.peers.values():
            result=self.rpc(peer,'/raft/request-vote',{'term':term,'candidate_id':self.id,'last_log_index':last_i,'last_log_term':last_t})
            if result and result.get('term',0)>term:
                with self.lock: self.state['term']=result['term']; self.state['voted_for']=None; self.role='follower'; self.persist()
                return
            if result and result.get('vote_granted'): votes+=1
        with self.lock:
            if self.role!='candidate' or self.state['term']!=term: return
            if votes>=(len(self.peers)+1)//2+1:
                self.role='leader'; self.leader_id=self.id
                self.next_index={p:len(self.state['log']) for p in self.peers}; self.match_index={p:-1 for p in self.peers}
                self.match_index[self.id]=len(self.state['log'])-1
                self.last_heartbeat=0
    def request_vote(self,b):
        with self.lock:
            term=b['term']
            if term>self.state['term']:
                self.state['term']=term; self.state['voted_for']=None; self.role='follower'
            last_i=len(self.state['log'])-1; last_t=self.state['log'][last_i]['term'] if last_i>=0 else 0
            up=(b['last_log_term'],b['last_log_index']) >= (last_t,last_i)
            grant=term==self.state['term'] and up and self.state['voted_for'] in (None,b['candidate_id'])
            if grant: self.state['voted_for']=b['candidate_id']; self.last_contact=time.monotonic(); self.election_deadline=self._deadline()
            self.persist(); return {'term':self.state['term'],'vote_granted':grant}
    def append_entries(self,b):
        with self.lock:
            term=b['term']
            if term<self.state['term']: return {'term':self.state['term'],'success':False}
            if term>self.state['term']: self.state['term']=term; self.state['voted_for']=None
            self.role='follower'; self.leader_id=b['leader_id']; self.last_contact=time.monotonic(); self.election_deadline=self._deadline()
            prev=b['prev_log_index']; log=self.state['log']
            if prev>=0 and (prev>=len(log) or log[prev]['term']!=b['prev_log_term']):
                self.persist(); return {'term':self.state['term'],'success':False,'match_index':len(log)-1}
            idx=prev+1
            for e in b['entries']:
                if idx<len(log) and log[idx]['term']!=e['term']: log[:]=log[:idx]
                if idx>=len(log): log.append(e)
                idx+=1
            self.state['commit_index']=min(b['leader_commit'],len(log)-1)
            self.apply(); self.persist()
            return {'term':self.state['term'],'success':True,'match_index':idx-1}
    def _send_peer(self, peer):
        with self.lock:
            if self.role!='leader': return False
            ni=self.next_index.get(peer,len(self.state['log'])); ni=max(0,min(ni,len(self.state['log'])))
            prev=ni-1; pt=self.state['log'][prev]['term'] if prev>=0 else 0
            b={'term':self.state['term'],'leader_id':self.id,'prev_log_index':prev,'prev_log_term':pt,'entries':self.state['log'][ni:],'leader_commit':self.state['commit_index']}
        result=self.rpc(self.peers[peer],'/raft/append',b)
        if not result: return False
        with self.lock:
            if result.get('term',0)>self.state['term']:
                self.state['term']=result['term']; self.state['voted_for']=None; self.role='follower'; self.leader_id=None; self.persist(); return False
            if result.get('success'):
                self.match_index[peer]=result['match_index']; self.next_index[peer]=result['match_index']+1; return True
            self.next_index[peer]=max(0,ni-1); return False
    def _replicate_all(self):
        with self.lock:
            if self.role!='leader': return
            peers=list(self.peers)
        threads=[]; results=[]
        for p in peers:
            t=threading.Thread(target=lambda p=p: results.append(self._send_peer(p))); t.start(); threads.append(t)
        for t in threads: t.join(.5)
        with self.lock:
            if self.role!='leader': return
            for idx in range(len(self.state['log'])-1,self.state['commit_index'],-1):
                count=1+sum(1 for m in self.match_index.values() if m>=idx)
                if count>=(len(self.peers)+1)//2+1 and self.state['log'][idx]['term']==self.state['term']:
                    self.state['commit_index']=idx; self.apply(); self.persist(); break
    def put(self,key,value,op='put'):
        with self.lock:
            if self.role!='leader': return None
            entry={'term':self.state['term'],'op':op,'key':key,'value':value}
            self.state['log'].append(entry); self.persist(); idx=len(self.state['log'])-1
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            self._replicate_all()
            with self.lock:
                if self.role!='leader': return None
                if self.state['commit_index']>=idx: return {'ok':True,'index':idx,'term':self.state['term'],'replicas':1+sum(m>=idx for m in self.match_index.values())}
            time.sleep(.04)
        return {'ok':False,'error':'majority unavailable; write not committed','status':503}
    def status(self):
        with self.lock: return {'id':self.id,'role':self.role,'leader':self.leader_id,'term':self.state['term'],'commit_index':self.state['commit_index'],'log_length':len(self.state['log']),'values':dict(self.state['kv'])}

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def send(self,obj,status=200,ctype='application/json'):
        data=(json.dumps(obj,indent=2) if ctype=='application/json' else obj).encode(); self.send_response(status); self.send_header('Content-Type',ctype); self.send_header('Content-Length',str(len(data))); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Methods','GET, POST, OPTIONS'); self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers(); self.wfile.write(data)
    def do_OPTIONS(self):
        self.send_response(204); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Methods','GET, POST, OPTIONS'); self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers()
    def body(self): return json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))))
    def do_GET(self):
        n=self.server.node
        if self.path=='/': return self.send(DASH,200,'text/html; charset=utf-8')
        if self.path=='/health': return self.send(n.status())
        if self.path=='/kv':
            with n.lock: return self.send({'values':n.state['kv'],'commit_index':n.state['commit_index'],'role':n.role,'leader':n.leader_id})
        if self.path.startswith('/kv/'): 
            key=self.path[4:]
            with n.lock:
                return self.send({'key':key,'value':n.state['kv'].get(key),'commit_index':n.state['commit_index'],'role':n.role})
        return self.send({'error':'not found'},404)
    def do_POST(self):
        n=self.server.node
        try: b=self.body()
        except Exception: return self.send({'error':'invalid JSON'},400)
        if self.path=='/raft/request-vote': return self.send(n.request_vote(b))
        if self.path=='/raft/append': return self.send(n.append_entries(b))
        if self.path=='/kv':
            if not b.get('key'): return self.send({'error':'key is required'},400)
            result=n.put(str(b['key']),b.get('value'))
            if result is None: return self.send({'error':'not leader','leader':n.leader_id,'hint':'retry against the elected leader'},409)
            return self.send(result,200 if result.get('ok') else 503)
        if self.path.startswith('/kv/delete/'):
            result=n.put(self.path[len('/kv/delete/'):],None,'delete')
            if result is None: return self.send({'error':'not leader','leader':n.leader_id},409)
            return self.send(result,200 if result.get('ok') else 503)
        return self.send({'error':'not found'},404)

DASH='''<!doctype html>
        <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width">
                <title>Replicated KV | Raft</title>
                <style>
                    *{box-sizing:border-box}
                    body{margin:0;background:#0c111c;color:#edf2ff;font:15px system-ui}
                    main{max-width:1100px;margin:auto;padding:42px 24px}.eyebrow{color:#8c9bb7;text-transform:uppercase;letter-spacing:.14em;font-size:12px}
                    h1{font-size:34px;margin:8px 0}.sub{color:#aab6ca;margin-bottom:28px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
                    .card,.panel{background:#151e2e;border:1px solid #263349;border-radius:14px;padding:18px}
                    .card h2{font-size:15px;margin:0 0 12px;color:#aab6ca}
                    .role{font-size:21px;font-weight:700}.leader{color:#62e6b2}
                    .follower{color:#92b4ff}.meta{color:#9eacc1;font-size:13px;margin-top:10px}
                    .panel{margin-top:18px}.panel h2{margin:0 0 14px}
                    .row{display:flex;gap:10px;margin-bottom:12px}input{background:#0d1421;border:1px solid #31415c;color:white;border-radius:8px;padding:11px;flex:1}
                    button{background:#7c91ff;color:#101629;border:0;border-radius:8px;padding:0 18px;font-weight:700;cursor:pointer}
                    .values{font-family:ui-monospace,monospace;color:#c9d5ea;white-space:pre-wrap}
                    .pill{display:inline-block;background:#1c2b41;padding:5px 9px;border-radius:99px;color:#b9c9e4;font-size:12px}@media(max-width:700px){.grid{grid-template-columns:1fr}
                    main{padding:26px 16px}}
                </style>
            </head>

            <body>
                <main>
                    <div class="eyebrow">Distributed Systems · Raft</div>
                    <h1>Replicated key / value service</h1>
                    <div class="sub">Three nodes · majority commit · automatic leader election</div>
                    <div id="nodes" class="grid"></div>
                    <section class="panel">
                        <h2>Write a value</h2>
                        <div class="row">
                            <input id="key" placeholder="Key">
                            <input id="value" placeholder="Value">
                            <button onclick="put()">Replicate</button>
                        </div>
                        <div id="result" class="meta">A successful write is acknowledged after majority replication.</div>
                    </section>
                    <section class="panel">
                        <h2>Committed state</h2>
                        <div id="values" class="values">Loading…</div>
                    </section>
                    <div class="meta">
                        <span class="pill">Linearizable writes</span> 
                        <span class="pill">Crash recovery</span> 
                        <span class="pill">Majority quorum</span>
                    </div>
                </main>
            
                <script>
                    const ports=[
                                8001,
                                8002,
                                8003
                            ];
                        async function refresh()
                        {
                            let xs=await Promise.all
                                (
                                    ports.map
                                    (
                                        async p=>
                                        {
                                            try
                                            {
                                                return await fetch(`http://${location.hostname}:${p}/health`).then(r=>r.json())
                                            }
                                            catch{return {id:p-8000,role:'offline'}}
                                        }
                                    )
                                );
                                document.querySelector('#nodes')
                                        .innerHTML=xs
                                        .map
                                            (x=>`
                                                <article class="card">
                                                    <h2>Replica ${x.id}</h2>
                                                    <div class="role ${x.role==='leader'?'leader':'follower'}">${x.role.toUpperCase()}</div>
                                                    <div class="meta">Term ${x.term??'—'} · Commit index ${x.commit_index??'—'}</div>
                                                    <div class="meta">${x.leader?'Leader: node '+x.leader:'Awaiting election'}</div>
                                                </article>`
                                            )
                                        .join('');
                                        let l=xs
                                            .find(x=>x.role==='leader');
                                                document.querySelector('#values')
                                            .textContent=l?JSON.stringify(l.values,null,2):'No leader available';}async function put()
                                                {let key=document.querySelector('#key').value,value=document.querySelector('#value').value;let leaders=[];for(let p of ports){try{let h=await fetch(`http://${location.hostname}:${p}/health`).then(r=>r.json());if(h.role==='leader')leaders.push(p)}catch{}}if(!leaders.length){document.querySelector('#result').textContent='No leader available';return}
                                        let r=await fetch(`http://${location.hostname}:${leaders[0]}/kv`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key,value})});
                                                document.querySelector('#result').textContent=JSON.stringify(await r.json());refresh()}refresh();setInterval(refresh,1000)
                </script>
            </body>
        </html>
    '''

def main():
 p=argparse.ArgumentParser(); p.add_argument('--id',type=int,required=True); p.add_argument('--port',type=int,required=True); p.add_argument('--peers',required=True,help='id=url,id=url'); p.add_argument('--data-dir',default='data'); a=p.parse_args()
 peers={int(k):v for k,v in (x.split('=',1) for x in a.peers.split(',') if x)}; n=Node(a.id,peers,a.data_dir)
 s=ThreadingHTTPServer(('127.0.0.1',a.port),Handler); s.node=n; print(f'node {a.id} listening on 127.0.0.1:{a.port}',flush=True)
 try: s.serve_forever()
 finally: n.alive=False; s.server_close()
if __name__=='__main__': main()
