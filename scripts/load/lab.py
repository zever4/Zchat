#!/usr/bin/env python3
"""Own local Docker lab. Never reads the developer's .env or databases."""
import argparse, base64, datetime, hashlib, json, os, pathlib, secrets, subprocess, sys, threading, time, urllib.request

ROOT=pathlib.Path(__file__).resolve().parents[2]
LAB=ROOT/'out/load'
PROJECT='zchat-load'
FILES=['compose.yaml','compose.calls.yaml','compose.monitoring.yaml','compose.load.yaml']
LIMITS={'smoke':6,'baseline':20,'stress':240,'spike':120,'soak':40,
        'profile-read-chats':40,'profile-read-history':40,'profile-write':50,'profile-mixed':90}

def run(args, **kw):
    return subprocess.run(args, cwd=ROOT, check=True, **kw)

def output(args):
    return subprocess.check_output(args,cwd=ROOT,text=True).strip()

def owned():
    if not (LAB/'.owned').is_file() or (LAB/'.owned').read_text()!='zchat-load-v1\n':
        raise RuntimeError('Run make load-init first; refusing an unowned lab directory')

def config():
    owned()
    return dict(line.split('=',1) for line in (LAB/'lab.env').read_text().splitlines() if line and not line.startswith('#'))

def compose(env, *args, **kw):
    # Explicit files/project and env override prevent ambient developer settings
    # from silently redirecting the lab to an ordinary DB or reusing credentials.
    # Retain Redis explicitly so historical lab resource comparisons stay comparable.
    command=['docker','compose','--profile','redis','--env-file',str(LAB/'lab.env'),'-p',PROJECT]
    for file in FILES:command+=['-f',str(ROOT/file)]
    return run(command+list(args),env={**os.environ,**env}, **kw)

def init():
    if (LAB/'lab.env').exists():owned();print('Existing private lab configuration preserved.');return
    if (LAB/'.owned').exists():raise RuntimeError('Lab env is missing; restore it before reusing its data')
    for kind in ['ps','volume']:
        args=['docker',kind,'ls','-q','--filter',f'label=com.docker.compose.project={PROJECT}'] if kind=='volume' else ['docker','ps','-aq','--filter',f'label=com.docker.compose.project={PROJECT}']
        if output(args):raise RuntimeError('Existing lab resources found without ownership/config; refusing to replace credentials')
    LAB.mkdir(parents=True,exist_ok=True);os.chmod(LAB,0o700)
    for name in ['data','reports/current']:(LAB/name).mkdir(parents=True,exist_ok=True)
    values=dict(DB_USER='zchat_load',DB_NAME='zchat_load',DB_PASS=secrets.token_hex(32),JWT_SECRET=secrets.token_hex(32),GRAFANA_ADMIN_PASSWORD=secrets.token_hex(24),NGINX_PORT='38080',SERVER_HTTP_PORT='38081',SERVER_GRPC_PORT='35051',DB_HOST_PORT='35432',REDIS_HOST_PORT='36379',PROMETHEUS_PORT='39090',GRAFANA_PORT='33000',LIVEKIT_API_KEY=secrets.token_hex(16),LIVEKIT_API_SECRET=secrets.token_hex(32),LIVEKIT_URL='ws://localhost:38080/rtc/',LIVEKIT_NODE_IP='127.0.0.1',LIVEKIT_MEDIA_BIND_IP='127.0.0.1',ENABLE_EXPERIMENTAL_E2EE='false',LOCAL_UID=str(os.getuid()),LOCAL_GID=str(os.getgid()),LOAD_RESULTS_DIR=str(LAB/'reports/current'))
    path=LAB/'lab.env';fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:f.write(''.join(f'{k}={v}\n' for k,v in values.items()))
    (LAB/'.owned').write_text('zchat-load-v1\n')
    print('Created private synthetic lab config in out/load/lab.env (no secrets printed).')

def byte_size(text):
    import re
    match=re.fullmatch(r'([\d.]+)\s*([kMGT]?i?B)',text)
    if not match:raise ValueError('Unknown Docker memory unit')
    value,unit=match.groups();powers={'B':1,'kB':1000,'MB':1000**2,'GB':1000**3,'TB':1000**4,'KiB':1024,'MiB':1024**2,'GiB':1024**3,'TiB':1024**4}
    return float(value)*powers[unit]

def source_digest():
    """Fingerprint versionable source, including new files in a dirty tree."""
    names=subprocess.check_output(['git','ls-files','-cz','--others','--exclude-standard'],cwd=ROOT).split(b'\0')
    digest=hashlib.sha256()
    for name in sorted(set(names)-{b''}):
        path=ROOT/os.fsdecode(name)
        if path.is_file():
            digest.update(name+b'\0'+hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()

def container_states():
    ids=output(['docker','ps','-aq','--filter',f'label=com.docker.compose.project={PROJECT}']).splitlines()
    if not ids:return []
    # Explicit allowlist: docker inspect also contains private environment values.
    return [{'service':v['Config']['Labels'].get('com.docker.compose.service'),
             'id':v['Id'],'image':v['Image'],'restartCount':v['RestartCount'],
             'status':v['State']['Status'],'oomKilled':v['State']['OOMKilled'],
             'exitCode':v['State']['ExitCode'],'startedAt':v['State']['StartedAt']}
            for v in json.loads(output(['docker','inspect',*ids]))]

def sample_resources(stop, report):
    with (report/'docker-stats.jsonl').open('w') as log:
        while not stop.is_set():
            now=time.time()
            try:
                listing=output(['docker','ps','--filter',f'label=com.docker.compose.project={PROJECT}','--format','{{.ID}} {{.Label "com.docker.compose.service"}}'])
                services=dict(line.split(' ',1) for line in listing.splitlines() if line)
                samples=[]
                if services:
                    try:
                        result=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',*services],text=True,timeout=8,stderr=subprocess.PIPE)
                    except subprocess.CalledProcessError:
                        # A disappearing --rm container can end the stats stream.
                        # Retry once with a fresh set; a persistent error remains
                        # an explicit telemetry failure and fails this lab run.
                        listing=output(['docker','ps','--filter',f'label=com.docker.compose.project={PROJECT}','--format','{{.ID}} {{.Label "com.docker.compose.service"}}'])
                        services=dict(line.split(' ',1) for line in listing.splitlines() if line)
                        if not services:raise
                        result=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',*services],text=True,timeout=8,stderr=subprocess.PIPE)
                        log.write(json.dumps(dict(time=now,recovered_stats_retry=True))+'\n')
                    for line in result.splitlines():
                        v=json.loads(line);service=services.get(v['ID'])
                        if v['PIDs']=='--':continue
                        if service not in ['server','nginx','db','redis','livekit','prometheus','grafana','postgres-exporter','pushgateway','k6']:continue
                        memory,limit=v['MemUsage'].split(' / ')
                        samples.append(dict(service=service,cpu_cores=float(v['CPUPerc'].rstrip('%'))/100,memory_bytes=byte_size(memory),memory_limit_bytes=byte_size(limit),pids=int(v['PIDs'])))
                log.write(json.dumps(dict(time=now,containers=samples))+'\n');log.flush()
                body=f'zchat_load_sampler_heartbeat_timestamp_seconds {now}\n'
                for service in ['server','db','nginx','redis','livekit','prometheus','grafana','postgres-exporter','pushgateway','k6']:
                    body+=f'zchat_container_running{{service="{service}"}} {int(service in services.values())}\n'
                for v in samples:
                    for metric in ['cpu_cores','memory_bytes','memory_limit_bytes','pids']:body+=f'zchat_container_{metric}{{service="{v["service"]}"}} {v[metric]}\n'
                req=urllib.request.Request('http://127.0.0.1:39091/metrics/job/zchat-load-resources',data=body.encode(),method='PUT',headers={'Content-Type':'text/plain; version=0.0.4'})
                with urllib.request.urlopen(req,timeout=3) as response:response.read()
            except Exception as e:
                # Error type only: exception strings may expose URLs/credentials.
                log.write(json.dumps(dict(time=now,collector_error=type(e).__name__))+'\n');log.flush()
            stop.wait(2)

def execute(env, profile):
    if output(['docker','ps','-aq','--filter','name=^/'+PROJECT+'-k6$']):
        raise RuntimeError('Another lab generator exists; finish that run first')
    data=json.loads((LAB/'data/accounts.json').read_text())
    if len(data['pairs'])<LIMITS[profile]:raise RuntimeError(f'Seed at least {LIMITS[profile]} pairs for {profile}')
    for pair in data['pairs']:
        for role in ['sender','receiver']:
            part=pair[role]['token'].split('.')[1];claim=json.loads(base64.urlsafe_b64decode(part+'='*(-len(part)%4)))
            if claim['exp']<time.time()+1200:raise RuntimeError('Fixture sessions expire soon; run load-seed again')
    # Ready endpoint is text, not JSON.
    with urllib.request.urlopen('http://127.0.0.1:'+env['PROMETHEUS_PORT']+'/-/ready',timeout=5) as r:assert r.status==200
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    report=LAB/'reports'/f'{stamp}-{profile}';report.mkdir(mode=0o700)
    meta={'profile':profile,'startedAt':stamp,'sourceCommit':output(['git','rev-parse','HEAD']),'sourceDirty':bool(output(['git','status','--porcelain'])),'fixturePairs':len(data['pairs']),'docker':json.loads(output(['docker','info','--format','{"cpus":{{.NCPU}},"memoryBytes":{{.MemTotal}},"version":"{{.ServerVersion}}"}'])),'limitsFile':'compose.load.yaml','limitations':['Local Docker/WSL, not VPS capacity.','k6 measures HTTP/WS chat, not WebRTC RTP or native image compression.','Pair = two independent accounts and two WebSocket connections; groups have up to ten members.']}
    resolved=json.loads(compose(env,'config','--format','json',stdout=subprocess.PIPE,text=True).stdout)
    meta['resourceLimits']={name:{key:value for key,value in service.items() if key in ['cpus','mem_limit']} for name,service in resolved['services'].items()}
    meta['sourceSha256']=source_digest();meta['containersBefore']=container_states()
    (report/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
    stop=threading.Event();collector=threading.Thread(target=sample_resources,args=(stop,report),daemon=True);collector.start()
    result=0
    try:
        try:
            command=['run','--out','experimental-prometheus-rw','/scripts/profile-read.js'] if profile.startswith('profile-read-') else []
            compose({**env,'LOAD_PROFILE':profile,'LOAD_RESULTS_DIR':str(report)},'run','--rm','--name',PROJECT+'-k6','k6',*command)
        except subprocess.CalledProcessError as e:result=e.returncode
        except KeyboardInterrupt:
            # Only this fixed project's named generator; no ordinary containers.
            subprocess.run(['docker','stop',PROJECT+'-k6'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=False)
            result=130
    finally:
        stop.set();collector.join(timeout=25)
        meta['finishedAt']=datetime.datetime.now(datetime.timezone.utc).isoformat();meta['exitCode']=result
        meta['resourceSamplerErrors']=sum('collector_error' in json.loads(line) for line in (report/'docker-stats.jsonl').read_text().splitlines())
        meta['containersAfter']=container_states()
        (report/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
    (LAB/'last-run.txt').write_text(str(report)+'\n')
    print(f'Reports: {report.relative_to(ROOT)}; k6 exit={result}; resource sampler errors={meta["resourceSamplerErrors"]}')
    return result or (1 if meta['resourceSamplerErrors'] else 0)

def observe(seconds):
    report=LAB/'reports'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-observe')
    report.mkdir(mode=0o700)
    stop=threading.Event();collector=threading.Thread(target=sample_resources,args=(stop,report),daemon=True);collector.start()
    print(f'Sampling own lab resources for up to {seconds}s; Ctrl+C stops sampling. Report: {report.relative_to(ROOT)}',flush=True)
    try:stop.wait(seconds)
    except KeyboardInterrupt:pass
    finally:stop.set();collector.join(timeout=25)
    errors=sum('collector_error' in json.loads(line) for line in (report/'docker-stats.jsonl').read_text().splitlines())
    print(f'Resource sampler errors: {errors}')
    return int(errors>0)

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['init','up','stop','down','reset','seed','run','info','verify','observe']);parser.add_argument('--pairs',type=int,default=10);parser.add_argument('--seconds',type=int,default=900);parser.add_argument('--profile',choices=LIMITS,default='smoke');args=parser.parse_args()
    if args.action=='init':init();return 0
    env=config()
    if args.action=='up':compose(env,'config','--quiet');compose(env,'up','-d','--build','--wait','--wait-timeout','180')
    elif args.action=='stop':compose(env,'stop')
    elif args.action=='down':compose(env,'down')
    elif args.action=='reset':
        compose(env,'down','--volumes');(LAB/'data/accounts.json').unlink(missing_ok=True);(LAB/'data/manual-accounts.txt').unlink(missing_ok=True);(LAB/'lab.env').unlink();(LAB/'.owned').unlink();print('Own synthetic data removed; historical reports preserved.')
    elif args.action=='seed':
        if not 1<=args.pairs<=500:raise RuntimeError('Pairs must be 1..500')
        compose({**env,'LOAD_PAIRS':str(args.pairs)},'run','--rm','seed')
    elif args.action=='run':return execute(env,args.profile)
    elif args.action=='observe':
        if not 1<=args.seconds<=86400:raise RuntimeError('Observation duration must be 1..86400 seconds')
        return observe(args.seconds)
    elif args.action=='verify':
        before=output(['docker','inspect',PROJECT+'-server-1','--format','{{.State.StartedAt}}'])
        compose(env,'stop','prometheus')
        try:compose(env,'run','--rm','seed','node','/scripts/verify.mjs','--without-prometheus')
        finally:compose(env,'up','-d','prometheus')
        for attempt in range(30):
            try:
                with urllib.request.urlopen('http://127.0.0.1:'+env['PROMETHEUS_PORT']+'/-/ready',timeout=2) as r:
                    if r.status==200:break
            except OSError:time.sleep(1)
        compose(env,'run','--rm','-e','GRAFANA_ADMIN_PASSWORD','seed','node','/scripts/verify.mjs')
        after=output(['docker','inspect',PROJECT+'-server-1','--format','{{.State.StartedAt}}'])
        if before!=after:raise RuntimeError('Backend restarted during Prometheus independence check')
        print('PASS: backend did not restart while Prometheus was stopped/restarted')
    else:
        print(f'Chat http://localhost:{env["NGINX_PORT"]}\nGrafana http://localhost:{env["GRAFANA_PORT"]} (admin; password in out/load/lab.env)\nPrometheus http://localhost:{env["PROMETHEUS_PORT"]}\nOnly synthetic test data. Stop with make load-stop; never use ordinary .env here.')
    return 0
if __name__=='__main__':
    try:sys.exit(main())
    except (RuntimeError,subprocess.CalledProcessError,OSError,ValueError) as e:
        print(f'Lab failed: {type(e).__name__}: {e}' if isinstance(e,RuntimeError) else f'Lab failed: {type(e).__name__}',file=sys.stderr);sys.exit(1)
