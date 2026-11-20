#!/usr/bin/env python3
"""Real Linux migration: retain job PID, deadline and Docker workloads; test rollback."""
import copy
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid

prefix = 'runner-migration-test-' + uuid.uuid4().hex[:10]
root = Path(__file__).resolve().parents[2]
temporary = tempfile.TemporaryDirectory(prefix=prefix)
data = Path(temporary.name) / 'data'
data.mkdir(mode=0o700)
lock = threading.Lock()
reports, checks = [], []
desired = {}
legacy = prefix + '-legacy'
helper_image, legacy_image = prefix + ':helper', prefix + ':legacy'

def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.STDOUT).strip()

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        self.send_error(404)
    def do_POST(self):
        if self.path != '/api/sync':
            self.send_error(404)
            return
        assert self.headers['authorization'] == 'Bearer fixture-join'
        body = json.loads(self.rfile.read(int(self.headers['content-length'])))
        with lock:
            reports.append(body)
            del reports[:-100]
            plan = {'domain': 'example.test', 'desired': copy.deepcopy(desired), 'poll': .2}
        payload = json.dumps(plan).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
control = 'http://127.0.0.1:' + str(server.server_port)
owner = 'runner-' + hashlib.sha256(control.encode()).hexdigest()[:12]
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    router_port = listener.getsockname()[1]

def latest():
    with lock:
        return copy.deepcopy(reports[-1]) if reports else {}

def until(predicate, message, timeout=100):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.3)
    raise AssertionError(message + '; last status=' + json.dumps(latest().get('status')))

def workload():
    return docker('ps', '-q', '--filter', 'label=io.runners.owner=' + owner,
                  '--filter', 'label=io.runners.copy=worker')

def state():
    return json.loads(docker('exec', legacy, 'cat', str(data / 'agent-state.json')))

def process_identity():
    return docker('inspect', '--format', '{{.State.Pid}}', legacy)

def migrate(expected=0, mode='apply', token='fixture-join', slot='1', assets=None):
    arguments = ['run', '--rm', '--network', 'host', '--pid', 'host', '--privileged',
                 '-v', '/var/run/docker.sock:/var/run/docker.sock',
                 '-e', 'CONTROL_URL=' + control, '-e', 'MIGRATION_FLEET_TOKEN=' + token,
                 '-e', 'FLEET_MACHINE=' + slot, '-e', 'MIGRATION_MODE=' + mode]
    if assets:
        arguments += ['-v', f'{assets}:/native:ro']
    result = subprocess.run(['docker', *arguments, helper_image], capture_output=True, text=True, timeout=90)
    print(result.stdout.strip(), result.stderr.strip(), flush=True)
    assert result.returncode == expected, result
    records = [json.loads(line) for line in (result.stdout + result.stderr).splitlines() if line.startswith('{')]
    return records[-1]

def fixture_artifact(name, invalid_checksum=False):
    directory = Path(temporary.name) / name
    directory.mkdir()
    binary = directory / ('runner-agent-linux-' + arch)
    binary.write_text('#!/bin/sh\nif [ "${1:-}" = --version ]; then echo "runner-agent fixture"; exit 0; fi\nexit 17\n')
    binary.chmod(0o755)
    checksum = hashlib.sha256(binary.read_bytes()).hexdigest()
    (directory / 'manifest.json').write_text(json.dumps({'schema': 1, 'runtime': 'rust', 'targets': {
        target: {'path': binary.name, 'sha256': '0' * 64 if invalid_checksum else checksum, 'size': binary.stat().st_size}}}))
    return directory

try:
    for stage, image in [('release', helper_image), ('legacy-fixture', legacy_image)]:
        docker('build', '-f', 'control/migration/agent.Dockerfile', '--target', stage, '-t', image, str(root))
    for dependency in ['busybox:1.37', 'caddy:2']:
        docker('pull', dependency)
    arch = 'arm64' if docker('info', '--format', '{{.Architecture}}') in ['arm64', 'aarch64'] else 'amd64'
    target = ('aarch64' if arch == 'arm64' else 'x86_64') + '-unknown-linux-musl'
    docker('run', '-d', '--name', owner + '-tunnel', '--label', 'io.runners.owner=' + owner,
           '--label', 'io.runners.component=tunnel', 'busybox:1.37', 'sleep', '3600')
    now = int(time.time() * 1000)
    seed = {'v': 2, 'bootId': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'control': control, 'started': now - 10000, 'installedRevision': 'legacy-fixture',
            'agent': prefix, 'run': prefix, 'machine': 1, 'tunnelToken': 'fixture-token',
            'domain': 'example.test', 'wasReady': True, 'projects': [], 'desired': {}, 'metrics': []}
    (data / 'agent-state.json').write_text(json.dumps(seed))
    managed = {'services': {'worker': {'image': 'busybox:1.37', 'command': ['sleep', '3600']}}}
    desired = {'worker': {'v': 1, 'app': 'worker', 'replica': 1, 'port': None,
                         'compose': json.dumps(managed), 'files': {}, 'managedCompose': json.dumps(managed),
                         'dataFiles': {}, 'bindSources': [], 'revision': 'first'}}
    docker('run', '-d', '--name', legacy, '--network', 'host',
           '-v', '/var/run/docker.sock:/var/run/docker.sock', '-v', f'{data}:{data}',
           '-e', 'CONTROL_URL=' + control, '-e', 'JOIN_TOKEN=fixture-join', '-e', 'RUNNER_DATA=' + str(data),
           '-e', 'SERVER_TAG=standalone', '-e', 'LIFETIME_MIN=355', '-e', 'ROUTER_PORT=' + str(router_port), legacy_image)
    # The agent creates private directories owned by root; the fixture's seed must match.
    # This bind is exclusively the fixture's temporary data, never production files.
    docker('exec', legacy, 'chown', '-R', '0:0', str(data))
    until(lambda: latest().get('status', {}).get('worker', {}).get('s') == 'healthy', 'legacy workload did not become healthy')
    initial_state, initial_pid, worker = state(), process_identity(), workload()
    assert worker and initial_state['run'] == seed['run']
    inspection = migrate(mode='inspect')
    assert inspection['status'] == 'inspected' and inspection['workloads'] >= 3, inspection
    checks.append('read-only discovery identifies the exact credential, machine and legacy process')
    assert migrate(expected=1, token='wrong')['status'] == 'failed'
    assert migrate(expected=1, slot='2')['status'] == 'failed'
    assert process_identity() == initial_pid and workload() == worker
    checks.append('wrong credentials and slots cannot signal an agent')
    assert migrate(expected=1, assets=fixture_artifact('bad-checksum', True))['status'] == 'failed'
    assert process_identity() == initial_pid and workload() == worker
    checks.append('bad binary checksum is rejected before process changes')
    failure = migrate(expected=1, assets=fixture_artifact('failed-startup'))
    assert 'restored' in failure['error'], failure
    assert process_identity() == initial_pid and workload() == worker
    assert docker('exec', legacy, 'readlink', '/proc/1/exe').endswith('/node')
    restored = state()
    assert (restored['run'], restored['started'], restored['installedRevision']) == (seed['run'], seed['started'], seed['installedRevision'])
    checks.append('crashing native replacement restores Node with the original PID, deadline and containers')
    result = migrate()
    assert result['status'] == 'migrated', result
    assert process_identity() == initial_pid and workload() == worker
    assert docker('exec', legacy, 'readlink', '/proc/1/exe').endswith('/runner-agent')
    final = state()
    assert (final['run'], final['started']) == (seed['run'], seed['started'])
    until(lambda: latest().get('status', {}).get('worker', {}).get('s') == 'healthy', 'native workload health missing')
    assert latest()['deadline'] == seed['started'] + 355 * 60000
    checks.append('native exec retains the original job PID, run, deadline and Docker workload')
    assert migrate()['status'] == 'already-native'
    assert process_identity() == initial_pid and workload() == worker
    checks.append('repeated migration is a verified no-op')
    print(json.dumps({'checks': checks, 'result': result}), flush=True)
except BaseException:
    for name in [legacy, owner + '-router', owner + '-tunnel']:
        try:
            print(name, docker('inspect', '--format', '{{json .State}}', name), flush=True)
            print(docker('logs', '--tail', '45', name), flush=True)
        except subprocess.CalledProcessError:
            pass
    raise
finally:
    subprocess.run(['docker', 'rm', '-f', legacy], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ids = docker('ps', '-aq', '--filter', 'label=io.runners.owner=' + owner).split()
    if ids:
        subprocess.run(['docker', 'rm', '-f', *ids], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    networks = docker('network', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + owner + '-worker').split()
    if networks:
        subprocess.run(['docker', 'network', 'rm', *networks], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    server.shutdown()
    server.server_close()
    subprocess.run(['docker', 'image', 'rm', helper_image, legacy_image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(['sudo', 'chown', '-R', '--no-dereference', f'{os.getuid()}:{os.getgid()}', temporary.name], check=True)
    temporary.cleanup()
