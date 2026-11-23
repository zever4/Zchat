#!/usr/bin/env python3
"""Short PostgreSQL/Go profiling on the owned synthetic lab, never ordinary .env."""
import argparse, concurrent.futures, datetime, hashlib, json, pathlib, subprocess, threading, time
import lab

BASE=lab.LAB/'profiling'
SQL_CALLS=0

def sql(query):
    global SQL_CALLS
    SQL_CALLS+=1
    r=subprocess.run(['docker','exec','-e','PGAPPNAME=zchat-profiler','-i','zchat-load-db-1','psql','-XqAt','-v','ON_ERROR_STOP=1','-U','zchat_load','-d','zchat_load'],input=query,text=True,capture_output=True,check=True,timeout=15)
    return r.stdout.strip()

def snapshot():
    return json.loads(sql("""SELECT json_build_object(
      'database',(SELECT row_to_json(s) FROM (SELECT sessions,sessions_abandoned,sessions_fatal,xact_commit,xact_rollback,blks_read,blks_hit,deadlocks,temp_bytes,blk_read_time,blk_write_time FROM pg_stat_database WHERE datname=current_database()) s),
      'wal',(SELECT row_to_json(w) FROM pg_stat_wal w),
      'wal_io',(SELECT row_to_json(i) FROM pg_stat_io i WHERE backend_type='client backend' AND object='wal' AND context='normal'),
      'stats',(SELECT row_to_json(i) FROM pg_stat_statements_info i),
      'settings',(SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('jit','shared_preload_libraries','track_io_timing','track_wal_io_timing','pg_stat_statements.track_planning','max_connections','shared_buffers','work_mem','fsync','synchronous_commit','wal_sync_method'))
    )"""))

def statements():
    return json.loads(sql("""SELECT COALESCE(json_agg(t),'[]'::json) FROM (
      SELECT queryid::text,query,calls,plans,total_plan_time,total_exec_time,mean_plan_time,mean_exec_time,
        rows,shared_blks_hit,shared_blks_read,temp_blks_written,shared_blk_read_time,shared_blk_write_time,
        wal_bytes::text,jit_functions,jit_generation_time,jit_inlining_time,jit_optimization_time,jit_emission_time
      FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
      ORDER BY total_plan_time+total_exec_time DESC LIMIT 100
    ) t"""))

def activity(stop, folder):
    with (folder/'activity.jsonl').open('w') as f:
        while not stop.is_set():
            try:
                data=json.loads(sql("""SELECT COALESCE(json_agg(s),'[]'::json) FROM (
                  SELECT state,wait_event_type,wait_event,count(*) AS connections
                  FROM pg_stat_activity WHERE datname=current_database() AND application_name!='zchat-profiler'
                  GROUP BY 1,2,3) s"""))
                f.write(json.dumps({'time':time.time(),'activity':data})+'\n');f.flush()
            except (OSError,subprocess.SubprocessError,ValueError) as e:
                f.write(json.dumps({'time':time.time(),'error':type(e).__name__})+'\n');f.flush()
            stop.wait(2)

def capture(stop, folder, delay):
    if stop.wait(delay):return {'status':'skipped: workload ended before capture'}
    result={'startedAt':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        for name,path in [('goroutine','goroutine'),('heap-before','heap'),('allocs-before','allocs'),('cpu','profile?seconds=30'),('goroutine-late','goroutine'),('heap-after','heap'),('allocs-after','allocs')]:
            raw=subprocess.run(['docker','exec','zchat-load-server-1','wget','-q','-O','-','http://127.0.0.1:6060/debug/pprof/'+path],capture_output=True,check=True,timeout=40).stdout
            (folder/(name+'.pprof')).write_bytes(raw)
        result['status']='captured'
    except (OSError,subprocess.SubprocessError) as e:result['status']='failed: '+type(e).__name__
    result['finishedAt']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['up','run']);parser.add_argument('--kind',choices=['read-chats','read-history','write','mixed'],default='read-chats');args=parser.parse_args()
    env=lab.config();lab.FILES.append('compose.profile.yaml')
    if args.action=='up':
        lab.compose(env,'up','-d','--build','--wait','--wait-timeout','180')
        sql('CREATE EXTENSION IF NOT EXISTS pg_stat_statements;');print('Diagnostic extension enabled; data preserved.');return 0
    if lab.output(['docker','ps','-aq','--filter','name=^/zchat-load-k6$']):raise RuntimeError('Another lab generator exists')
    if sql('SHOW shared_preload_libraries')!='pg_stat_statements':raise RuntimeError('Run profile.py up first')
    BASE.mkdir(mode=0o700,parents=True,exist_ok=True)
    profile='profile-'+args.kind
    folder=BASE/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+args.kind);folder.mkdir(mode=0o700)
    # Copy the exact binary used for symbolization; profiles stay private in out/.
    subprocess.run(['docker','cp','zchat-load-server-1:/bin/server',str(folder/'server')],check=True,capture_output=True)
    (folder/'source.json').write_text(json.dumps({'sourceSha256':lab.source_digest(),'binarySha256':hashlib.sha256((folder/'server').read_bytes()).hexdigest(),'limitations':['Diagnostic overhead: pg_stat_statements planning and I/O timing enabled.','Read:100req/s45s. Write:50pairs30s ramp+45s hold+10s down, up to200messages/s. Mixed adds200HTTPreq/s.','Source hash describes the working tree; binary hash identifies the exact running executable, including experimental overlays. Not a repeat of the original400WS profile.']},indent=2)+'\n')
    sql('SELECT pg_stat_statements_reset();');before=snapshot();(folder/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    stop=threading.Event();observer=threading.Thread(target=activity,args=(stop,folder),daemon=True);observer.start()
    code=1;captures={}
    print('Profiling '+args.kind+'; artifacts: '+str(folder.relative_to(lab.ROOT)),flush=True)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(capture,stop,folder,8 if args.kind.startswith('read') else 35)
            try:code=lab.execute(env,profile)
            finally:stop.set();captures=future.result()
    finally:
        stop.set();observer.join(timeout=20)
        rows=statements();after=snapshot();(folder/'statements.json').write_text(json.dumps(rows,indent=2)+'\n');(folder/'after.json').write_text(json.dumps(after,indent=2)+'\n')
        metadata={'kind':args.kind,'exitCode':code,'profiles':captures,'workloadReport':(lab.LAB/'last-run.txt').read_text().strip(),'diagnosticSQLConnections':SQL_CALLS,'finishedAt':datetime.datetime.now(datetime.timezone.utc).isoformat()}
        (folder/'profile.json').write_text(json.dumps(metadata,indent=2)+'\n');(BASE/'last-profile.txt').write_text(str(folder)+'\n')
    print('Profile artifacts: '+str(folder.relative_to(lab.ROOT))+'; workload exit='+str(code),flush=True)
    return code

if __name__=='__main__':
    try:raise SystemExit(main())
    except (RuntimeError,OSError,subprocess.SubprocessError) as e:
        print('Profiling failed: '+type(e).__name__);raise SystemExit(1)
