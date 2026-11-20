// One-shot, administrator-deployed migration of this fleet's existing agent.
// It never signals another process unless its credentials, slot, entrypoint and
// saved inventory identify exactly one authorized agent. No public API is opened.
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { createHash, timingSafeEqual } from 'node:crypto';

const env = process.env;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const equal = (a, b) => typeof a === 'string' && typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));
const fail = message => { throw new Error(message); };
const quote = value => "'" + String(value).replaceAll("'", "'\\''") + "'";
const read = (file, limit = 64 << 20) => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY);
  try { if (fs.fstatSync(fd).size > limit) fail('input exceeds its size limit'); const bytes = fs.readFileSync(fd); if (bytes.length > limit) fail('input exceeds its size limit'); return bytes; }
  finally { fs.closeSync(fd); }
};
function processInfo(pid) {
  const prefix = `/proc/${pid}`;
  const variables = Object.fromEntries(read(prefix + '/environ', 1 << 20).toString().split('\0').filter(Boolean).map(item => { const cut = item.indexOf('='); return [item.slice(0, cut), item.slice(cut + 1)]; }));
  if (!equal(variables.CONTROL_URL, env.CONTROL_URL) || !equal(variables.JOIN_TOKEN, env.MIGRATION_FLEET_TOKEN)) return null;
  const argv = read(prefix + '/cmdline', 1 << 16).toString().split('\0').filter(Boolean);
  const executable = fs.readlinkSync(prefix + '/exe');
  const legacy = /\/node$/.test(executable) && argv.some(arg => /(?:^|\/)(?:agent|runner-agent-launcher)\.mjs$/.test(arg));
  const native = /\/(?:runner-agent|native-base|runner-agent-linux-(?:amd64|arm64))$/.test(executable);
  if (!legacy && !native) return null;
  const base = variables.RUNNER_DATA || '/var/lib/runner';
  if (!path.isAbsolute(base) || base.split('/').includes('..') || base.includes('\0')) fail('invalid agent data path');
  const root = prefix + '/root';
  const state = JSON.parse(read(root + base + '/agent-state.json'));
  if (state.control !== env.CONTROL_URL || Number(state.machine) !== Number(env.FLEET_MACHINE)) return null;
  const status = read(prefix + '/status', 1 << 16).toString();
  return { pid, variables, argv, executable, legacy, native, base, root, state,
    uid: Number(status.match(/^Uid:\s+(\d+)/m)?.[1]), gid: Number(status.match(/^Gid:\s+(\d+)/m)?.[1]),
    namespacePid: Number(status.match(/^NSpid:\s+(.+)$/m)?.[1].trim().split(/\s+/).at(-1) || pid),
    rssBytes: Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] || 0) * 1024,
    startTicks: read(prefix + '/stat', 1 << 16).toString().split(') ').at(-1).split(' ')[19] };
}
function agent() {
  const found = [];
  for (const name of fs.readdirSync('/proc')) {
    if (!/^\d+$/.test(name) || Number(name) === process.pid) continue;
    try { const item = processInfo(Number(name)); if (item) found.push(item); } catch {}
  }
  if (found.length !== 1) fail('expected exactly one agent with this fleet credential and machine slot');
  return found[0];
}
async function dockerContainers(owner) {
  const filters = encodeURIComponent(JSON.stringify({ label: ['io.runners.owner=' + owner] }));
  return new Promise((resolve, reject) => {
    const request = http.get({ socketPath: '/var/run/docker.sock', path: '/containers/json?all=1&filters=' + filters, timeout: 10000 }, response => {
      let size = 0; const chunks = [];
      response.on('data', chunk => { size += chunk.length; if (size > (4 << 20)) response.destroy(new Error('inventory too large')); else chunks.push(chunk); });
      response.on('error', reject);
      response.on('end', () => { try { if (response.statusCode !== 200) fail('Docker inventory unavailable'); resolve(JSON.parse(Buffer.concat(chunks))); } catch (error) { reject(error); } });
    });
    request.on('timeout', () => request.destroy(new Error('Docker inventory timed out')));
    request.on('error', reject);
  });
}
function directory(name, uid, gid) {
  if (!fs.existsSync(name)) { fs.mkdirSync(name, { mode: 0o700 }); fs.chownSync(name, uid, gid); }
  const meta = fs.lstatSync(name);
  if (!meta.isDirectory() || meta.isSymbolicLink() || meta.uid !== uid) fail('unsafe migration directory');
}
function install(file, bytes, uid, gid) {
  if (fs.existsSync(file)) {
    const meta = fs.lstatSync(file);
    if (!meta.isFile() || meta.isSymbolicLink() || meta.uid !== uid || digest(read(file)) !== digest(bytes)) fail('existing migration file differs');
    return;
  }
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o700);
  try { fs.fchownSync(fd, uid, gid); fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}
async function inspector() {
  const response = await fetch('http://127.0.0.1:9229/json/list', { signal: AbortSignal.timeout(800) });
  if (!response.ok) fail('inspector unavailable');
  const list = await response.json();
  const url = new URL(list[0]?.webSocketDebuggerUrl);
  if (url.protocol !== 'ws:' || !['127.0.0.1', 'localhost'].includes(url.hostname) || url.port !== '9229') fail('unexpected inspector endpoint');
  return url.href;
}
async function connectInspector(url) {
  const ws = new WebSocket(url); const pending = new Map(); let next = 1;
  await new Promise((resolve, reject) => { const timer = setTimeout(() => { ws.close(); reject(new Error('inspector connection timed out')); }, 5000); ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true }); ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error('inspector connection failed')); }, { once: true }); });
  ws.addEventListener('message', event => {
    try { const message = JSON.parse(event.data); if (pending.has(message.id)) { const item = pending.get(message.id); pending.delete(message.id); clearTimeout(item.timer); message.error || message.result?.exceptionDetails ? item.reject(new Error('agent migration evaluation failed: ' + String(message.result?.exceptionDetails?.exception?.description || message.error?.message || 'unknown').split('\n')[0].slice(0,200))) : item.resolve(message.result?.result?.value); } } catch {}
  });
  return { close: () => ws.close(), evaluate: expression => new Promise((resolve, reject) => {
    const id = next++; const timer = setTimeout(() => { pending.delete(id); reject(new Error('agent evaluation timed out')); }, 10000);
    pending.set(id, { resolve, reject, timer }); ws.send(JSON.stringify({ id, method: 'Runtime.evaluate', params: { expression, awaitPromise: true, returnByValue: true } }));
  }) };
}
function wrapper(info, binary, revision, marker) {
  const original = [info.executable, ...info.argv.slice(1)].map(quote).join(' ');
  const state = info.base + '/agent-state.json';
  const restore = `const f=require('node:fs'),p=${JSON.stringify(state)},s=JSON.parse(f.readFileSync(p,'utf8'));${info.state.installedRevision === undefined ? 'delete s.installedRevision;' : 's.installedRevision=' + JSON.stringify(info.state.installedRevision) + ';'}const t=p+'.rollback-'+process.pid;f.writeFileSync(t,JSON.stringify(s),{mode:384,flag:'wx'});f.renameSync(t,p);`;
  const native = `RUNNER_INSTALL_REVISION=${quote(revision)} RUNNER_ACTIVE_BINARY_REVISION=${quote(revision)} RUNNER_CODE_MANAGED=1 ${quote(binary)}`;
  return `#!/bin/sh\nset -u\nrollback() {\n  ${quote(info.executable)} -e ${quote(restore)} || exit 1\n  printf '%s\\n' '{"status":"rolled-back"}' > ${quote(marker)}\n  exec ${original}\n}\n${native} &\nchild=$!\ntrap 'kill -TERM "$child" 2>/dev/null || true; wait "$child"; exit 143' TERM INT\ncount=0\nwhile [ "$count" -lt 10 ]; do\n  if ! kill -0 "$child" 2>/dev/null; then wait "$child"; result=$?; [ "$result" -eq 0 ] && exit 0; rollback; fi\n  sleep 1\n  count=$((count + 1))\ndone\nkill -TERM "$child" 2>/dev/null || rollback\nwait "$child" || rollback\ntrap - TERM INT\nexec env RUNNER_INSTALL_REVISION=${quote(revision)} RUNNER_ACTIVE_BINARY_REVISION=${quote(revision)} RUNNER_CODE_MANAGED=1 ${quote(binary)}\n`;
}

async function main() {
  if (process.platform !== 'linux' || !env.CONTROL_URL || !env.MIGRATION_FLEET_TOKEN || !/^[1-9]\d*$/.test(env.FLEET_MACHINE || '')) fail('explicit Linux fleet and slot credentials are required');
  const target = `${process.arch === 'x64' ? 'x86_64' : process.arch === 'arm64' ? 'aarch64' : 'unsupported'}-unknown-linux-musl`;
  const manifest = JSON.parse(read('/native/manifest.json', 65536)); const artifact = manifest.targets?.[target];
  if (manifest.schema !== 1 || manifest.runtime !== 'rust' || !/^[a-f0-9]{64}$/.test(artifact?.sha256 || '') || !(artifact.size > 0 && artifact.size <= (32 << 20))) fail('invalid native release manifest');
  const source = '/native/' + path.basename(artifact.path); const bytes = read(source, 32 << 20);
  if (bytes.length !== artifact.size || digest(bytes) !== artifact.sha256) fail('native release checksum failed');
  let info = agent();
  if (info.native) {
    if (digest(read(`/proc/${info.pid}/exe`, 32 << 20)) !== artifact.sha256) fail('running native agent differs from the selected release');
    console.log(JSON.stringify({ status: 'already-native', machine: Number(env.FLEET_MACHINE), pid: info.pid, rssBytes: info.rssBytes, revision: artifact.sha256 })); return;
  }
  for (let attempts = 0; ; attempts++) {
    const busy = (info.state.projects || []).some(([, value]) => value.busy);
    if (!busy && info.state.wasReady && !info.state.restartRequested) break;
    if (attempts >= 30) fail('agent has pending workload changes; no migration attempted');
    await sleep(1000); info = agent();
  }
  const owner = 'runner-' + digest(new URL(env.CONTROL_URL).origin).slice(0, 12);
  const before = (await dockerContainers(owner)).filter(c => !env.MIGRATION_APP || (c.Labels?.['io.runners.app'] !== env.MIGRATION_APP && c.Labels?.['io.runners.copy'] !== env.MIGRATION_APP)).map(c => c.Id);
  if (env.MIGRATION_MODE !== 'apply') { console.log(JSON.stringify({ status: 'inspected', machine: Number(env.FLEET_MACHINE), pid: info.pid, rssBytes: info.rssBytes, workloads: before.length, ready: info.state.wasReady })); return; }
  try { await inspector(); fail('an inspector is already active; no migration attempted'); } catch (error) { if (error.message === 'an inspector is already active; no migration attempted') throw error; }
  const baseStat = fs.lstatSync(info.root + info.base);
  if (!baseStat.isDirectory() || baseStat.isSymbolicLink() || baseStat.uid !== info.uid) fail('unsafe agent data directory');
  const code = info.base + '/agent-code'; const directoryPath = code + '/migration-' + artifact.sha256;
  directory(info.root + code, info.uid, info.gid); directory(info.root + directoryPath, info.uid, info.gid);
  const binary = directoryPath + '/runner-agent'; const launch = directoryPath + '/start.sh'; const marker = directoryPath + '/result.json';
  if (fs.existsSync(info.root + marker) && JSON.parse(read(info.root + marker, 8192)).status === 'rolled-back') fail('previous migration rolled back; operator review required');
  install(info.root + binary, bytes, info.uid, info.gid);
  install(info.root + launch, Buffer.from(wrapper(info, binary, artifact.sha256, marker)), info.uid, info.gid);
  const pointer = info.root + code + '/rust-current.json';
  if (fs.existsSync(pointer) && JSON.parse(read(pointer, 4096)).revision !== artifact.sha256) fail('another native release is selected');
  const current = processInfo(info.pid);
  if (!current?.legacy || current.startTicks !== info.startTicks) fail('the agent changed before migration');
  let debuggerSession = null; let scheduled = false;
  try {
    process.kill(info.pid, 'SIGUSR1');
    let url;
    for (let attempt = 0; attempt < 30 && !url; attempt++) { try { url = await inspector(); } catch { await sleep(100); } }
    if (!url) fail('the agent inspector did not become available');
    debuggerSession = await connectInspector(url);
    const metadata = await debuggerSession.evaluate('({pid:process.pid,execve:typeof process.execve,term:process.listenerCount("SIGTERM"),platform:process.platform})');
    if (metadata?.pid !== info.namespacePid || metadata.execve !== 'function' || metadata.term < 1 || metadata.platform !== 'linux') fail('the original job cannot preserve its process identity');
    const originalArgv = [info.executable, ...info.argv.slice(1)];
    const expression = `(async()=>{const f=process.getBuiltinModule('fs'),c=process.getBuiltinModule('crypto'),p=process.getBuiltinModule('child_process');const binary=${JSON.stringify(binary)},launch=${JSON.stringify(launch)},state=${JSON.stringify(info.base + '/agent-state.json')},expected=${JSON.stringify(artifact.sha256)};if(c.createHash('sha256').update(f.readFileSync(binary)).digest('hex')!==expected)throw Error('checksum');if(!p.execFileSync(binary,['--version'],{timeout:5000,encoding:'utf8'}).startsWith('runner-agent '))throw Error('version');if(f.realpathSync(${JSON.stringify(info.base)})!==${JSON.stringify(info.base)})throw Error('data path contains a link');process.once('exit',()=>{try{const s=JSON.parse(f.readFileSync(state,'utf8'));if(s.run!==${JSON.stringify(info.state.run)}||s.started!==${info.state.started}||s.restartRequested)throw Error('inventory changed');s.installedRevision=expected;const temp=state+'.native-'+process.pid;const fd=f.openSync(temp,'wx',384);f.writeFileSync(fd,JSON.stringify(s));f.fsyncSync(fd);f.closeSync(fd);f.renameSync(temp,state);const parent=f.openSync(${JSON.stringify(info.base)},'r');f.fsyncSync(parent);f.closeSync(parent);process.execve('/bin/sh',['/bin/sh',launch],process.env);}catch{const s=JSON.parse(f.readFileSync(state,'utf8'));${info.state.installedRevision === undefined ? 'delete s.installedRevision;' : 's.installedRevision=' + JSON.stringify(info.state.installedRevision) + ';'}const t=state+'.abort-'+process.pid;f.writeFileSync(t,JSON.stringify(s),{mode:384,flag:'wx'});f.renameSync(t,state);process.execve(${JSON.stringify(info.executable)},${JSON.stringify(originalArgv)},process.env);}});setTimeout(()=>process.emit('SIGTERM'),250);return {scheduled:true};})()`;
    const result = await debuggerSession.evaluate(expression); if (!result?.scheduled) fail('migration was not scheduled'); scheduled = true;
    debuggerSession.close(); debuggerSession = null;
    let after;
    for (let attempt = 0; attempt < 90; attempt++) {
      await sleep(500);
      if (fs.existsSync(info.root + marker) && JSON.parse(read(info.root + marker, 8192)).status === 'rolled-back') fail('native startup failed; the original Node agent was restored');
      try { after = processInfo(info.pid); } catch { after = null; }
      if (after?.native && after.startTicks === info.startTicks && digest(read(`/proc/${info.pid}/exe`, 32 << 20)) === artifact.sha256) break;
      after = null;
    }
    if (!after) fail('native agent did not retain the original job process');
    await sleep(3000); after = processInfo(info.pid);
    if (!after?.native || after.state.run !== info.state.run || after.state.started !== info.state.started) fail('agent identity or original lifetime changed');
    const existing = new Set((await dockerContainers(owner)).map(c => c.Id));
    if (before.some(id => !existing.has(id))) fail('a pre-existing workload changed during verification');
    const resultRecord = { status: 'migrated', machine: Number(env.FLEET_MACHINE), pid: info.pid, runPreserved: true, lifetimePreserved: true, containersPreserved: before.length, beforeRssBytes: info.rssBytes, rssBytes: after.rssBytes, revision: artifact.sha256 };
    fs.writeFileSync(info.root + marker, JSON.stringify(resultRecord), { mode: 0o600 }); fs.chownSync(info.root + marker, info.uid, info.gid);
    console.log(JSON.stringify(resultRecord));
  } finally {
    if (debuggerSession) { if (!scheduled) await debuggerSession.evaluate("(()=>{const i=process.getBuiltinModule('inspector');setTimeout(()=>i.close(),50);return true})()").catch(() => {}); debuggerSession.close(); }
  }
}
main().catch(error => { console.error(JSON.stringify({ status: 'failed', machine: Number(env.FLEET_MACHINE), error: error.message })); process.exitCode = 1; });
