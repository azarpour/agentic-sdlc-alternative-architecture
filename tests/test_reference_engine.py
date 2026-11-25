from orchestrator.reference_engine import Engine,descendants
from orchestrator import core
import pytest

def agent(node,s):
    if node=='requirements': return {'ambiguities':[]}
    if node=='code': return {'files':{'src/main/java/demo/Shortener.java':(core.BASE/'src/main/java/demo/Shortener.java').read_text()}}
    if node=='tests': return {'files':{'src/test/java/demo/NewTest.java':'package demo; class NewTest {}'}}
    return {'summary':node}
def runner(s): return {'passed':True,'candidate_hash':s['tasks']['candidate']['output']['candidate_hash']}
def approve(e,s): return e.respond(s['run_id'],{'approve':True,'hash':s['pending']['hash']})
def test_lifecycle(tmp_path):
    e=Engine(tmp_path,agent,runner); s=e.start('greenfield','demo'); assert s['pending']['type']=='design_gate'
    s=approve(e,s); assert s['pending']['type']=='release_gate'; assert approve(e,s)['status']=='release_ready'
def test_stale(tmp_path):
    e=Engine(tmp_path,agent,runner); s=e.start('greenfield','demo'); old=s['pending']['hash']; s=e.revise(s['run_id'],'design'); assert s['pending']['hash']!=old
    with pytest.raises(ValueError): e.respond(s['run_id'],{'approve':True,'hash':old})
def test_selective(tmp_path):
    e=Engine(tmp_path,agent,runner); s=approve(e,e.start('greenfield','demo')); old=s['tasks']['code']; s=e.revise(s['run_id'],'tests'); assert s['tasks']['code']==old; assert s['pending']['type']=='release_gate'
def test_retry_restart(tmp_path):
    calls=[]
    def flaky(n,s):
        calls.append(n)
        if n=='tests' and calls.count(n)<=2: raise RuntimeError('injected')
        return agent(n,s)
    e=Engine(tmp_path,flaky,runner); s=approve(e,e.start('greenfield','demo')); assert s['status']=='retryable'; e.db.close()
    e=Engine(tmp_path,flaky,runner,max_attempts=3); s=e.advance(s['run_id']); assert s['pending']['type']=='release_gate'; assert calls.count('code')==1
def test_tamper(tmp_path):
    e=Engine(tmp_path,agent,runner); s=approve(e,e.start('greenfield','demo')); (e.workspace(s)/'pom.xml').write_text('tamper')
    with pytest.raises(ValueError): approve(e,s)
def test_clarify(tmp_path):
    def ambiguous(n,s):
        if n=='requirements': return {'ambiguities':[] if s['clarification'] else ['lifetime?']}
        return agent(n,s)
    e=Engine(tmp_path,ambiguous,runner); s=e.start('ambiguous','demo'); assert s['pending']['type']=='clarification'; s=e.respond(s['run_id'],'30 days'); assert s['pending']['type']=='design_gate'
def test_audit(tmp_path):
    e=Engine(tmp_path,agent,runner); s=e.start('greenfield','demo'); events=e.events(s['run_id']); assert all(x['hash']==core.digest(x['body']) for x in events); assert all(b['body']['previous']==a['hash'] for a,b in zip(events,events[1:]))
def test_automatic_retry(tmp_path):
    calls=[]
    def flaky(n,s):
        calls.append(n)
        if n=='tests' and calls.count(n)==1: raise RuntimeError('injected')
        return agent(n,s)
    e=Engine(tmp_path,flaky,runner); s=approve(e,e.start('greenfield','demo')); assert s['pending']['type']=='release_gate'; assert calls.count('code')==1; assert e.metrics(s['run_id'])['retries']==1

def test_stop(tmp_path):
    e=Engine(tmp_path,agent,runner); s=approve(e,e.start('greenfield','demo')); s=e.stop(s['run_id']); assert s['status']=='safe_stop'; assert not e.workspace(s).exists()
