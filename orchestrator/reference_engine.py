"""Persistent task DAG. Run via reference_cli; provider and runner are injectable."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import copy, json, os, re, shutil, sqlite3, subprocess, time, uuid, threading
from . import core

DAG = {'requirements': (), 'plan': ('requirements',), 'design': ('plan',),
       'design_gate': ('design',), 'code': ('design_gate',), 'tests': ('design_gate',),
       'candidate': ('code','tests'), 'verification': ('candidate',),
       'review': ('verification',), 'docs': ('review',), 'release_gate': ('docs',)}
GATES = {'design_gate','release_gate'}

def descendants(stage):
    result={stage}
    for node, deps in DAG.items():
        if result.intersection(deps): result.add(node)
    return result

class Engine:
    def __init__(self, root, agent=None, runner=None, max_attempts=2, max_revisions=3):
        self.root=Path(root).resolve(); self.root.mkdir(parents=True,exist_ok=True)
        self.agent=agent or self.generate; self.runner=runner or self.maven
        self.max_attempts=max_attempts; self.max_revisions=max_revisions
        self.lock=threading.RLock()
        self.db=sqlite3.connect(self.root/'engine.sqlite',check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, state TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, run TEXT, body TEXT, hash TEXT)'); self.db.commit()
    def load(self, rid):
        row=self.db.execute('SELECT state FROM runs WHERE id=?',(rid,)).fetchone()
        if not row: raise KeyError(rid)
        return json.loads(row[0])
    def save(self,s,event,**data):
        with self.lock,self.db:
            prev=self.db.execute('SELECT hash FROM events WHERE run=? ORDER BY seq DESC LIMIT 1',(s['run_id'],)).fetchone()
            body={'event':event,'time':time.time(),'version':s['version'],'previous':prev[0] if prev else None,**data}
            self.db.execute('INSERT INTO events(run,body,hash) VALUES(?,?,?)',(s['run_id'],json.dumps(body),core.digest(body)))
            self.db.execute('INSERT OR REPLACE INTO runs VALUES(?,?)',(s['run_id'],json.dumps(s)))
    def start(self,scenario,requirement):
        if scenario not in ('greenfield','brownfield','ambiguous') or not requirement.strip(): raise ValueError('Invalid scenario or requirement')
        s={'run_id':str(uuid.uuid4()),'scenario':scenario,'requirement':requirement,'version':1,'tasks':{},'attempts':{},'status':'running','started':time.time(),'revisions':0,'clarification':''}
        self.save(s,'start'); return self.advance(s['run_id'])
    def context(self,s):
        c={k:v for k,v in s.items() if k not in ('tasks','attempts')}
        c.update({k:v['output'] for k,v in s['tasks'].items()}); return c
    def generate(self,node,s):
        c=self.context(s)
        if node in ('code','tests'):
            # core batch cache is durable; namespace by approved design digest.
            c['generation_epoch']=core.digest([c['requirements'],c['plan'],c['design'],s.get('epochs',{}).get(node,0)])
            return core.generate_batches(node,c)
        schema={'requirements':core.Analysis,'plan':core.Plan,'design':core.Plan,'review':core.Review,'docs':core.Documentation}[node]
        return core.generate(node,c,schema)
    def workspace(self,s): return self.root/s['run_id']/'candidate'
    def candidate(self,s):
        work=self.workspace(s); tmp=work.with_name('staging'); shutil.rmtree(tmp,ignore_errors=True)
        shutil.copytree(core.baseline(s),tmp,ignore=shutil.ignore_patterns('target','.git'))
        code=s['tasks']['code']['output']; tests=s['tasks']['tests']['output']
        if s['scenario']=='brownfield' and code.get('files'): raise ValueError('Brownfield is test-only')
        if code.get('files'): core.apply_patch(tmp,code,'src/main/java')
        elif s['scenario']!='brownfield': raise ValueError('Missing code')
        core.apply_patch(tmp,tests,'src/test/java')
        if work.exists(): shutil.rmtree(work)
        tmp.rename(work); return {'candidate_hash':core.tree_hash(work)}
    def maven(self,s):
        if os.getenv('ALLOW_LOCAL_MAVEN')!='1': raise RuntimeError('Set ALLOW_LOCAL_MAVEN=1 only in an isolated disposable environment')
        work=self.workspace(s); before=core.tree_hash(work)
        if before!=s['tasks']['candidate']['output']['candidate_hash']: raise ValueError('Candidate changed')
        env={k:v for k,v in os.environ.items() if k in ('PATH','JAVA_HOME','HOME','USERPROFILE','SYSTEMROOT','TEMP','TMP','DOCKER_HOST','DOCKER_API_VERSION','JAVA_TOOL_OPTIONS','MAVEN_HOME','M2_HOME','COMSPEC')}
        executable=shutil.which('mvn') or shutil.which('mvn.cmd')
        if not executable: raise RuntimeError('Maven not installed')
        log=work.parent/f'verification-v{s["version"]}.log'
        with log.open('wb') as f:
            result=subprocess.run([executable,'-B','clean','verify'],cwd=work,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=900)
        raw=log.read_bytes(); text=raw.decode(errors='replace'); counts=re.findall(r'Tests run:\s*(\d+),\s*Failures:\s*(\d+),\s*Errors:\s*(\d+),\s*Skipped:\s*(\d+)',text)
        passed=result.returncode==0 and 'BUILD SUCCESS' in text and bool(counts) and any(int(c[0])>0 for c in counts) and all(not any(map(int,c[1:])) for c in counts) and before==core.tree_hash(work)
        return {'passed':passed,'candidate_hash':before,'log_sha256':__import__('hashlib').sha256(raw).hexdigest(),'exit_code':result.returncode,'log':str(log)}
    def execute(self,node,s):
        if node=='candidate': return self.candidate(s)
        if node=='verification': return self.runner(s)
        out=self.agent(node,s)
        if node=='docs':
            report=self.workspace(s).parent/f'reports-v{s["version"]}'; report.mkdir(parents=True,exist_ok=True)
            for key in ('summary','architecture','api_contract','test_report','release_notes'):
                if key in out: (report/(key+'.md')).write_text(str(out[key]),encoding='utf8')
        return out
    def binding(self,s,node):
        return core.digest([s['run_id'],s['version'],node,{k:v['hash'] for k,v in s['tasks'].items()}])
    def advance(self,rid):
        with self.lock:
            s=self.load(rid)
            if s['status'] in ('release_ready','safe_stop','waiting'): return s
            s['status']='running'; s.pop('pending',None)
            while True:
                ready=[n for n,d in DAG.items() if n not in s['tasks'] and all(x in s['tasks'] for x in d)]
                if not ready:
                    s['status']='release_ready'; self.save(s,'terminal',status=s['status'],duration=time.time()-s['started']); return s
                if ready[0] in GATES:
                    n=ready[0]; s['status']='waiting'; s['pending']={'type':n,'hash':self.binding(s,n)}; self.save(s,'gate',gate=n); return s
                # Persist attempt before dispatch, then commit each completed sibling independently.
                for n in ready:
                    if s['attempts'].get(n,0)>=self.max_attempts:
                        s['status']='retryable'; s['pending']={'type':'retry_or_revise','tasks':[n]}; self.save(s,'budget_exhausted',task=n); return s
                    s['attempts'][n]=s['attempts'].get(n,0)+1
                self.save(s,'dispatch',tasks=ready)
                failed=[]
                with ThreadPoolExecutor(max_workers=2) as pool:
                    from concurrent.futures import as_completed
                    futures={pool.submit(self.execute,n,copy.deepcopy(s)):n for n in ready}
                    for future in as_completed(futures):
                        n=futures[future]
                        try:
                            out=future.result()
                            if n=='verification' and (not out.get('passed') or out.get('candidate_hash')!=s['tasks']['candidate']['output']['candidate_hash']): raise ValueError('Verification failed')
                            if n=='review' and out.get('blocking'): raise ValueError('Blocking review')
                            s['tasks'][n]={'output':out,'hash':core.digest(out),'inputs':{d:s['tasks'][d]['hash'] for d in DAG[n]}}
                            self.save(s,'completed',task=n,output_hash=core.digest(out))
                        except Exception as e:
                            failed.append(n); s.setdefault('failure_history',[]).append({'task':n,'error':str(e)[:1000]}); self.save(s,'failure',task=n,error=type(e).__name__,detail=str(e)[:1000])
                if failed:
                    s['status']='retryable'; s['pending']={'type':'retry_or_revise','tasks':failed}; self.save(s,'paused_failure')
                    if all(s['attempts'][n]<self.max_attempts for n in failed):
                        self.save(s,'automatic_retry',tasks=failed); continue
                    return s
                if 'requirements' in ready and s['tasks']['requirements']['output'].get('ambiguities'):
                    s['status']='waiting'; s['pending']={'type':'clarification','questions':s['tasks']['requirements']['output']['ambiguities']}; self.save(s,'clarification'); return s
    def respond(self,rid,answer):
        with self.lock:
            s=self.load(rid); p=s.get('pending',{}); kind=p.get('type')
            if s['status']!='waiting': raise ValueError('Not waiting for approval')
            if kind=='clarification':
                if not isinstance(answer,str) or not answer.strip(): raise ValueError('Clarification required')
                return self.revise(rid,'requirements',clarification=answer)
            if kind not in GATES: raise ValueError('Unknown gate')
            if not isinstance(answer,dict) or answer.get('hash')!=p['hash'] or answer.get('hash')!=self.binding(s,kind): raise ValueError('Stale or invalid approval')
            if answer.get('approve') is not True:
                self.save(s,'rejected',gate=kind); return self.stop(rid)
            if kind=='release_gate' and core.tree_hash(self.workspace(s))!=s['tasks']['candidate']['output']['candidate_hash']: raise ValueError('Candidate changed after verification')
            s['tasks'][kind]={'output':{'approved':True,'binding':p['hash']},'hash':p['hash'],'inputs':{d:s['tasks'][d]['hash'] for d in DAG[kind]}}
            s['status']='running'; self.save(s,'approved',gate=kind); return self.advance(rid)
    def revise(self,rid,stage,requirement=None,clarification=None):
        with self.lock:
            s=self.load(rid)
            if s['status'] not in ('waiting','retryable'): raise ValueError('Revision only at checkpoint')
            if stage not in ('requirements','plan','design','code','tests'): raise ValueError('Invalid revision stage')
            if (requirement is not None or clarification is not None) and stage!='requirements': raise ValueError('Requirement change must invalidate requirements')
            if s['revisions']>=self.max_revisions: raise ValueError('Revision budget exhausted')
            invalid=descendants(stage); work=self.workspace(s)
            if 'candidate' in invalid and work.exists(): work.rename(work.with_name(f'archive-v{s["version"]}-{uuid.uuid4().hex[:8]}'))
            for n in invalid:
                s['tasks'].pop(n,None); s['attempts'].pop(n,None)
                s.setdefault('epochs',{})[n]=s.get('epochs',{}).get(n,0)+1
            if requirement is not None: s['requirement']=requirement
            if clarification is not None: s['clarification']+='\n'+clarification
            s['version']+=1; s['revisions']+=1; s['status']='running'; self.save(s,'revision',stage=stage,invalidated=sorted(invalid)); return self.advance(rid)
    def events(self,rid):
        return [{'body':json.loads(b),'hash':h} for b,h in self.db.execute('SELECT body,hash FROM events WHERE run=? ORDER BY seq',(rid,))]

    def metrics(self,rid):
        events=[e['body'] for e in self.events(rid)]; s=self.load(rid)
        failures=[e['time'] for e in events if e['event']=='failure']; recovered=[e['time'] for e in events if e['event']=='terminal' and e.get('status')=='release_ready']
        return {'status':s['status'],'success':int(s['status']=='release_ready'),'retries':sum(e['event']=='automatic_retry' for e in events),'revisions':sum(e['event']=='revision' for e in events),'failures':len(failures),'elapsed_seconds':(events[-1]['time']-events[0]['time']) if events else 0,'recovery_seconds':recovered[-1]-failures[0] if failures and recovered else None}

    def stop(self,rid):
        with self.lock:
            s=self.load(rid)
            if s['status']=='release_ready': raise ValueError('Already released')
            work=self.workspace(s)
            if work.exists(): work.rename(work.with_name(f'archive-stop-{uuid.uuid4().hex[:8]}'))
            s['status']='safe_stop'; s.pop('pending',None); self.save(s,'terminal',status='safe_stop'); return s
