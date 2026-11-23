#!/usr/bin/env python3
"""Short, opt-in WS/TLS-gRPC/PG diagnostic; never starts the normal app or changes its config."""
import argparse
from contextlib import contextmanager
import fcntl
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid

import lab

ROOT = Path(__file__).resolve().parents[2]
CONTAINER = 'zchat-load-db-1'


def command(args, *, timeout=15, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True,
                                   stderr=subprocess.PIPE, timeout=timeout).strip()


def bounded_process(args, log, timeout, env=None):
    """Timeout also stops compiler/test children before database cleanup."""
    with log.open('wb') as output:
        child = subprocess.Popen(args, cwd=ROOT, env=env, stdout=output,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return child.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            raise


def validate_container(value, env):
    labels = value['Config'].get('Labels') or {}
    if labels.get('com.docker.compose.project') != lab.PROJECT or labels.get('com.docker.compose.service') != 'db':
        raise RuntimeError('Refusing a container outside the owned load lab')
    ports = value['HostConfig'].get('PortBindings', {}).get('5432/tcp') or []
    if not ports or any(p.get('HostIp') != '127.0.0.1' or p.get('HostPort') != env['DB_HOST_PORT'] for p in ports):
        raise RuntimeError('Expected the lab PostgreSQL port bound only to loopback')
    return {key: value['HostConfig'][key] for key in ['Memory', 'NanoCpus']}


def connection_url(env, database):
    return 'postgresql://{}:{}@127.0.0.1:{}/{}?sslmode=disable'.format(
        urllib.parse.quote(env['DB_USER'], safe=''), urllib.parse.quote(env['DB_PASS'], safe=''),
        int(env['DB_HOST_PORT']), urllib.parse.quote(database, safe=''))


def psql(env, sql):
    return command(['docker', 'exec', CONTAINER, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                    '-U', env['DB_USER'], '-d', env['DB_NAME'], '-tA', '-c', sql])


def sample(stop, path, env, database):
    with path.open('w') as output:
        while not stop.is_set():
            value = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            try:
                raw = json.loads(command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', CONTAINER], timeout=5))
                value['db'] = {k: raw[k] for k in ['CPUPerc', 'MemUsage', 'BlockIO', 'PIDs']}
                # Event/category/count only: never SQL text, users, credentials or payloads.
                value['pgWaits'] = json.loads(psql(env, "SELECT COALESCE(json_agg(s),'[]') FROM "
                    "(SELECT state,wait_event_type,wait_event,count(*) FROM pg_stat_activity "
                    f"WHERE datname='{database}' GROUP BY 1,2,3) s"))
            except (OSError, ValueError, subprocess.SubprocessError) as err:
                value['collectorError'] = type(err).__name__
            output.write(json.dumps(value) + '\n')
            output.flush()
            stop.wait(1)



@contextmanager
def lab_lock():
    with (lab.LAB / '.mixed-load.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another mixed diagnostic owns this lab') from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, help='1..30 seconds per steady phase (default8, race1)')
    parser.add_argument('--workers', type=int, help='1..64 accounts pairs/concurrency (default32, race4)')
    parser.add_argument('--race', action='store_true', help='short correctness check, not a performance comparison')
    parser.add_argument('--admission', choices=['enabled', 'disabled'], default='enabled', help='shared server budget; disabled is the comparison control')
    args = parser.parse_args()
    seconds = args.seconds if args.seconds is not None else (1 if args.race else 8)
    workers = args.workers if args.workers is not None else (4 if args.race else 32)
    if not 1 <= seconds <= 30 or not 1 <= workers <= 64:
        parser.error('seconds must be1..30, workers1..64')
    if args.race and (seconds > 3 or workers > 8):
        parser.error('race diagnostic is limited to3seconds/8workers')
    env = lab.config()  # requires out/load/.owned; never reads ordinary .env
    with lab_lock():
        return execute(env, args, seconds, workers)


def execute(env, args, seconds, workers):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    report = lab.LAB / 'mixed' / (stamp + '-' + uuid.uuid4().hex[:8])
    report.mkdir(parents=True, mode=0o700)
    (report / '.owned').write_text('zchat-mixed-load-v1\n')
    executable = report / 'mixed.test'
    meta = {'sourceHead': command(['git', 'rev-parse', 'HEAD']), 'sourceDigest': lab.source_digest(),
            'go': command(['go', 'version']), 'seconds': seconds, 'workers': workers,
            'race': args.race, 'admission': args.admission, 'limits': ['in-process client+server, no main/nginx/native/Grafana capacity proof',
            'synthetic ciphertext/simulated recipient ACK, no libsignal cost',
            'latencies sampled; exact counts/mean/max; short diagnostic, no soak'],
            'report': str(report.relative_to(ROOT))}
    database = 'zchat_mixed_' + uuid.uuid4().hex
    marker = 'zchat-mixed-load-v1:' + uuid.uuid4().hex
    meta['ownedDatabase'] = database
    meta['cleanupComplete'] = False
    created = started = False
    sampler = None
    stop = threading.Event()
    container_id = None
    try:
        # Compile before starting PostgreSQL, so build time isn't measured or sampled.
        build = ['go', 'test', '-c', '-o', str(executable)]
        if args.race:
            build.append('-race')
        build.append('./internal/grpc')
        if bounded_process(build, report / 'compile.log', 180):
            raise RuntimeError('Compile failed; inspect private compile.log')
        meta['executableSha256'] = hashlib.sha256(executable.read_bytes()).hexdigest()
        value = json.loads(command(['docker', 'inspect', CONTAINER]))[0]
        container_id = value['Id']
        meta['databaseLimits'] = validate_container(value, env)
        meta['databaseImage'] = value['Image']
        meta['databaseOriginallyRunning'] = value['State']['Running']
        if not value['State']['Running']:
            command(['docker', 'start', CONTAINER])
            started = True
        for _ in range(30):
            try:
                psql(env, 'SELECT 1')
                break
            except subprocess.SubprocessError:
                time.sleep(.5)
        else:
            raise RuntimeError('Owned lab PostgreSQL did not become ready')
        meta['postgresConfig'] = json.loads(psql(env, "SELECT json_build_object('version',current_setting('server_version'),'shared_buffers',current_setting('shared_buffers'),'max_connections',current_setting('max_connections'),'shared_preload_libraries',current_setting('shared_preload_libraries'),'track_io_timing',current_setting('track_io_timing'))"))
        # Separate nonce-named DATABASE as well as testutil's disposable schema.
        # Neither ordinary app data nor historical lab account data is modified.
        psql(env, f'CREATE DATABASE "{database}"')
        created = True
        psql(env, f'COMMENT ON DATABASE "{database}" IS \'{marker}\'')
        sampler = threading.Thread(target=sample, args=(stop, report / 'resources.jsonl', env, database))
        sampler.start()
        runtime_env = {**os.environ, 'ZCHAT_TEST_DATABASE_URL': connection_url(env, database),
                       'ZCHAT_RUN_MIXED_LOAD': '1', 'ZCHAT_MIXED_LOAD_SECONDS': str(seconds),
                       'ZCHAT_MIXED_LOAD_WORKERS': str(workers), 'ZCHAT_MIXED_LOAD_REPORT': str(report / 'result.json')}
        runtime_env['ZCHAT_MIXED_ADMISSION'] = 'on' if args.admission == 'enabled' else 'off'
        print('Short mixed diagnostic:', report.relative_to(ROOT), flush=True)
        pattern = '^TestMixedMessagingLoadPostgres$'
        if args.race:
            pattern = '^Test(MixedMessagingLoadPostgres|MixedLoadConfigurationBounds|MixedLoadStatisticsBoundedAndExactCounters|E2EEEpochRPCRecoveryAndRetainedDeliveryTLSPostgres|E2EEEpochRPCAuthAndV1RetirementTLSPostgres|E2EEAdmissionRejectsBeforeCommitAndPreservesExactRetryPostgres)$'
        meta['testPattern'] = pattern
        meta['exitCode'] = bounded_process([str(executable), '-test.run=' + pattern,
                                           '-test.v', '-test.count=1', '-test.timeout=360s'],
                                          report / 'run.log', 380, runtime_env)
        if (report / 'result.json').exists():
            result = json.loads((report / 'result.json').read_text())
            meta['failedCycles'] = sum(v['Attempts'] - v['Completed'] for p in result['Phases'] for v in p['Lanes'].values())
            for phase in result['Phases']:
                print('{}: {:.2f}s pool={}/25 waits={} {}'.format(phase['Name'], phase['ElapsedSeconds'],
                      phase['PoolPeakInUse'], phase['PoolWaitCount'],
                      ', '.join('{}={}/{} p95={:.1f}ms'.format(k, v['Completed'], v['Attempts'], v['P95Ms']) for k,v in phase['Lanes'].items())), flush=True)
        return meta['exitCode']
    finally:
        stop.set()
        if sampler:
            sampler.join(timeout=25)
            if sampler.is_alive():
                raise RuntimeError('Resource sampler did not retire; refusing database cleanup race')
        try:
            if created:
                current = json.loads(command(['docker', 'inspect', CONTAINER]))[0]
                if current['Id'] != container_id:
                    raise RuntimeError('Database container changed; refusing cleanup')
                owner = psql(env, f"SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname='{database}'")
                if owner != marker:
                    raise RuntimeError('Disposable database marker changed; refusing cleanup')
                psql(env, f'DROP DATABASE "{database}" WITH (FORCE)')
                meta['disposableDatabaseRemoved'] = True
        except (RuntimeError, OSError, subprocess.SubprocessError) as err:
            meta['cleanupError'] = type(err).__name__
            raise
        finally:
            if started:
                current = json.loads(command(['docker', 'inspect', CONTAINER]))[0]
                if current['Id'] != container_id:
                    raise RuntimeError('Container changed; refusing stop')
                command(['docker', 'stop', '--time', '10', CONTAINER], timeout=20)
                meta['databaseRestoredStopped'] = True
            executable.unlink(missing_ok=True)  # no pprof here; keep hash, not a redundant binary
            meta['cleanupComplete'] = 'cleanupError' not in meta and (not created or meta.get('disposableDatabaseRemoved', False))
            meta['finishedAt'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            (report / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as err:
        # Child arguments/env may contain a DSN. Do not echo exceptions or argv.
        print('Mixed diagnostic failed:', type(err).__name__, '(inspect private reports)', file=sys.stderr)
        sys.exit(1)
