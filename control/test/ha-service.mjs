import assert from 'node:assert/strict';
import http from 'node:http';
import { fork, spawn, execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { once } from 'node:events';
import { mkdtemp, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { D1Client } from '../src/d1-client.mjs';
import { bootstrapManifest } from '../cluster-init.mjs';
import { D1Store } from '../src/d1-store.mjs';
import { PlanAuthority } from '../../agent/runtime.mjs';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const root = await realpath(await mkdtemp(tmpdir() + '/runner-ha-'));
const fixture = fork(new URL('./d1-fixture.mjs', import.meta.url), { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
const [{ port: d1Port }] = await once(fixture, 'message');
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return server.address().port; };
const freePort = async () => { const server = http.createServer(); const port = await listen(server); await new Promise(resolve => server.close(resolve)); return port; };
const cloudflare = http.createServer((req, res) => {
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ success: true, result: [], result_info: { total_pages: 1 } }));
});
const cfPort = await listen(cloudflare), members = [], endpoints = new Map(), proxies = [], processes = new Map(), logs = new Map();
let isolated = new Set(), checks = 0, corruptSender = null, corruptRejected = 0;
const linkFaults = new Map(), blockedPulls = new Set(), blockedJoins = new Set(), networkTimers = new Set();
const networkStats = { dropped: 0, delayed: 0, lateDelivered: 0 };
let capturePackets = false;
const capturedPackets = [];
const checkResults = [], testStarted = Date.now();
let passed = false, failure = null;
const memorySamples=[];
const check = (name, fn) => { fn(); checks++; checkResults.push({ name, elapsedMs: Date.now() - testStarted }); console.log('ok ' + name);
  if(process.env.HA_MEMORY_RESULT){
    const rows=execFileSync('ps',['-axo','pid=,ppid=,rss='],{encoding:'utf8'}).trim().split('\n').map(line=>{const[pid,parent,rssKiB]=line.trim().split(/\s+/).map(Number);return{pid,parent,rssKiB};});
    const replicas=[...processes.values()].map(p=>{const selected=new Set([p.child.pid]);let count;do{count=selected.size;for(const row of rows)if(selected.has(row.parent))selected.add(row.pid);}while(selected.size!==count);const processes=rows.filter(row=>selected.has(row.pid));return{id:p.id,name:p.name,rssKiB:processes.reduce((sum,p)=>sum+p.rssKiB,0),processes};});
    memorySamples.push({name,replicas});
  }
};
for (let id = 1; id <= 7; id++) {
  const proxy = http.createServer((req, res) => {
    const from = Number(req.headers['x-cp-from']);
    if (from && isolated.size && isolated.has(from) !== isolated.has(id)) { res.writeHead(503); return res.end(); }
    const url = new URL(req.url, 'http://local');
    const fault = linkFaults.get(from + ':' + id);
    if (from && ((url.pathname === '/_cp/pull' && blockedPulls.has(url.searchParams.get('node'))) ||
        (url.pathname === '/_cp/join' && blockedJoins.has(from)) || fault?.drop ||
        (fault?.dropEvery && ++fault.calls % fault.dropEvery === 0))) {
      networkStats.dropped++; res.writeHead(503); return res.end();
    }
    const corrupted = corruptSender?.from === from && corruptSender?.to === id && req.url.startsWith('/_cp/raft');
    if (corrupted) req.headers['x-cp-boot'] = 'incorrect-boot-for-test';
    const forward = bytes => {
      const outgoing = http.request('http://127.0.0.1:' + endpoints.get(id) + req.url,
        { method: req.method, headers: req.headers }, incoming => {
          if (corrupted && incoming.statusCode === 409) corruptRejected++;
          if (res.destroyed) { if (bytes && incoming.statusCode < 300) networkStats.lateDelivered++; incoming.resume(); return; }
          const respond = () => {
            if (res.destroyed) return incoming.resume();
            res.writeHead(incoming.statusCode, incoming.headers); incoming.pipe(res);
          };
          if (fault?.responseDelay) {
            incoming.pause();
            const timer = setTimeout(() => { networkTimers.delete(timer); respond(); }, fault.responseDelay);
            networkTimers.add(timer);
          } else respond();
        });
      outgoing.on('error', () => { if (!res.destroyed) { if (!res.headersSent) res.writeHead(503); res.end(); } });
      if (bytes) outgoing.end(bytes); else req.pipe(outgoing);
    };
    if (fault?.delay || (capturePackets && from && url.pathname === '/_cp/raft')) {
      const chunks = []; let size = 0;
      req.on('data', chunk => { size += chunk.length; if (size > 1024 * 1024) req.destroy(); else chunks.push(chunk); });
      req.on('end', () => {
        const bytes = Buffer.concat(chunks);
        if (capturePackets && capturedPackets.length < 16) capturedPackets.push({ from, to: id, path: req.url, headers: { ...req.headers }, bytes });
        if (fault?.delay) {
          networkStats.delayed++;
          // Deliver even after the caller times out: a failed RPC may still
          // reach its receiver. This exercises delayed messages, not just loss.
          const timer = setTimeout(() => { networkTimers.delete(timer); forward(bytes); }, fault.delay);
          networkTimers.add(timer);
        } else forward(bytes);
      });
    } else forward();
  });
  proxies.push(proxy);
  members.push({ id, url: 'http://127.0.0.1:' + await listen(proxy), label: 'Replica ' + id });
}
const env = { ...process.env, CONFIG_STORAGE: 'd1', ACCOUNT_ID: 'test', D1_DATABASE_ID: 'test',
  D1_API_KEY: 'test-global-key', D1_API_EMAIL: 'test@example.com', D1_API_BASE: 'http://127.0.0.1:' + d1Port,
  CONTROL_HOST: 'runners.test.example', DOMAIN: 'test.example', ZONE: 'test', CF_API_TOKEN: 'test-token',
  CF_API_BASE: 'http://127.0.0.1:' + cfPort, DNS: 'off', AUTO_TUNNEL: '0', BIND_HOST: '127.0.0.1',
  CP_REPLICAS: '7', CP_CLUSTER_ID: 'test-cluster-generation', CP_CLUSTER_TOKEN: 'test-cluster-token-at-least-32-characters',
  CP_MEMBERS: JSON.stringify(members), CP_RAFT_BINARY: process.env.CP_RAFT_BINARY || '/tmp/runners-raft-memory-test',
  CP_RAFT_TICK_MS: '100', CP_RAFT_ELECTION_TICKS: '12', CP_RAFT_HEARTBEAT_TICKS: '2',
  STATE_SIGNING_KEY: 'test-signing-key', JOIN_TOKEN: 'test-join-token', ADMIN_PASSWORD: 'test-admin',
  RECOVERY_MS: '0', QUIET_MS: '0' };
const client = new D1Client(env);
const db = { query(sql) { return client.query(sql).flatMap(result => result.results); } };
async function start(id, name = String(id), forced = false, extraEnv = {}) {
  const port = await freePort(), raftPort = await freePort(), childPort = await freePort();
  const rustIds = new Set((process.env.RUST_CONTROL_IDS || '').split(',').filter(Boolean).map(Number));
  const native = process.env.RUST_CONTROL_BINARY && (!rustIds.size || rustIds.has(id));
  assert.ok(!forced || native, 'forced startup requires the native controller');
  const child = spawn(native ? process.env.RUST_CONTROL_BINARY : process.execPath, native ? (forced ? ['start', '--force-operations'] : []) : [new URL('../ha-server.mjs', import.meta.url).pathname], {
    env: { ...env, PORT: String(port), CP_RAFT_PORT: String(raftPort), CP_CHILD_PORT: String(childPort),
      CP_NODE_ID: String(id), DATA_DIR: root + '/' + name, ...extraEnv }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  logs.set(name, ''); for (const stream of [child.stdout, child.stderr]) stream.on('data', data => logs.set(name, (logs.get(name) + data).slice(-6000)));
  child.exited = new Promise(resolve => child.once('exit', resolve));
  const instance = { id, port, child, name }; processes.set(name, instance);
  if (name === String(id)) endpoints.set(id, port);
  return instance;
}
async function stop(name, signal = 'SIGTERM') {
  const process = processes.get(String(name)); if (!process) return;
  processes.delete(String(name)); process.child.kill(signal);
  const timer = setTimeout(() => process.child.kill('SIGKILL'), 15_000);
  await process.child.exited; clearTimeout(timer);
}
async function request(id, path, init = {}) {
  const url = typeof id === 'number' ? members.find(m => m.id === id).url : 'http://127.0.0.1:' + id.port;
  try {
    const response = await fetch(url + path, { signal: AbortSignal.timeout(8000), ...init,
      headers: { 'x-cp-token': env.CP_CLUSTER_TOKEN, 'content-type': 'application/json', ...init.headers } });
    return { status: response.status, body: await response.json() };
  } catch { return { status: 0 }; }
}
async function until(fn, timeout = 25_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const value = await fn(); if (value) return value; await sleep(150); }
  throw new Error('timed out waiting for replica condition');
}
const views = async () => (await Promise.all(members.map(m => request(m.id, '/_cp/status')))).filter(r => r.status === 200).map(r => r.body);
const primary = () => until(async () => { const leaders = (await views()).filter(v => v.primary); return leaders.length === 1 && leaders[0]; });
let staleStore;
try {
  for (let id = 1; id <= 7; id++) await start(id);
  await until(async () => (await views()).filter(v => v.raft?.phase === 'uninitialized').length === 7);
  const staged = await views();
  check('fresh replicas neither vote nor start a scheduler before explicit bootstrap', () => assert.equal(staged.some(v => v.primary || v.authority), false));
  const stagedShell = await fetch(members[0].url + '/');
  const stagedPage = await stagedShell.text();
  check('the shared Runners Fleet interface is available before a primary starts', () => {
    assert.equal(stagedShell.status, 200); assert.match(stagedPage, /function controlPlanePanel\(\)/);
    assert.match(stagedPage, /nav class="tabs"/); assert.doesNotMatch(stagedPage, /cp-cluster-link/);
  });
  const legacy = await fetch(members[0].url + '/cluster', { redirect: 'manual' });
  check('old replica dashboard links redirect into the main Fleet tab', () => {
    assert.equal(legacy.status, 302); assert.equal(legacy.headers.get('location'), '/#/fleet');
  });
  const seed = bootstrapManifest({ members, quorum: 4 }, staged.map(view => view.raft));
  for (const member of members) assert.equal((await request(member.id, '/_cp/bootstrap', { method: 'POST', body: JSON.stringify(seed) })).status, 200);
  const first = await primary();
  check('a bootstrapped quorum starts exactly one primary scheduler', () => assert.ok(first.authority?.epoch >= seed.term));
  const follower = members.find(m => m.id !== first.id).id;
  const dashboard = await request(follower, '/api/status');
  check('a standby forwards the regular dashboard to the primary', () => assert.equal(dashboard.status, 200));
  // The deployed Node proxy rewrites Host without carrying the original
  // browser origin. That pre-existing UI limitation is fixed by the native
  // proxy; exercise the new behavior when both endpoints implement it.
  const nativeIds = new Set((process.env.RUST_CONTROL_IDS || '').split(',').filter(Boolean).map(Number));
  const native = id => process.env.RUST_CONTROL_BINARY && (!nativeIds.size || nativeIds.has(id));
  if (native(first.id) && native(follower)) {
    const followerOrigin = members.find(m => m.id === follower).url;
    const authenticated = await request(follower, '/admin/api/unlock', { method: 'POST',
      headers: { origin: followerOrigin, 'x-admin-password': env.ADMIN_PASSWORD }, body: '{}' });
    check('a standby preserves browser origin for authenticated UI actions', () => assert.equal(authenticated.status, 200));
  }
  const insight = await request(follower, '/api/cluster');
  check('every replica exposes the complete membership and recent transitions', () => {
    assert.equal(insight.body.replicas.length, 7); assert.equal(insight.body.quorum, 4); assert.ok(insight.body.transitions.length);
  });
  const created = await request(first.id, '/api/projects/example', { method: 'PUT',
    headers: { 'x-admin-password': env.ADMIN_PASSWORD }, body: JSON.stringify({ compose: 'services:\n  app:\n    image: nginx\n', replicas: 1 }) });
  check('the active primary can commit application configuration to D1', () => assert.equal(created.status, 200));
  staleStore = new D1Store({ ...env, CP_HA_CHILD: '1', CP_HA_MANAGER: 'http://127.0.0.1:' + processes.get(String(first.id)).port,
    CP_HA_AUTHORITY: JSON.stringify(first.authority) });
  const agentAuthority = new PlanAuthority(members.find(m => m.id === first.id).url, env.JOIN_TOKEN);
  await agentAuthority.accept({ leadership: first.authority });
  isolated = new Set([first.id, ...members.filter(m => m.id !== first.id).slice(0, 2).map(m => m.id)]);
  const strandedJoin = await request(first.id, '/_cp/join', { method: 'POST',
    body: JSON.stringify({ id: first.id, node: 281474976700001, boot: 'isolated-join-test-incarnation' }) });
  check('an isolated leader cannot admit a fresh process into an uncommitted generation', () => assert.notEqual(strandedJoin.status, 200));
  const second = await until(async () => { const all = await views(); return all.find(v => !isolated.has(v.id) && v.primary && v.authority?.term > first.authority.term); });
  const refused = await request(first.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(first.authority) });
  check('an isolated old scheduler refuses authority after a new election', () => assert.notEqual(refused.status, 200));
  assert.throws(() => staleStore.mutate(() => staleStore.exec('INSERT INTO projects VALUES(?,?,?,?,?,?,?)', 'stale', 1, 1, 1, null, Date.now(), 1)), /primary authority/);
  check('a former primary cannot save configuration even before the new primary writes', () => {
    assert.equal(db.query("SELECT name FROM projects WHERE name='stale'").length, 0);
  });
  const newer = await request(second.id, '/api/projects/newer', { method: 'PUT', headers: { 'x-admin-password': env.ADMIN_PASSWORD },
    body: JSON.stringify({ compose: 'services:\n  app:\n    image: nginx\n', replicas: 1 }) });
  assert.equal(newer.status, 200);
  agentAuthority.url = members.find(m => m.id === second.id).url;
  await assert.rejects(agentAuthority.accept({ leadership: first.authority }));
  await agentAuthority.accept({ leadership: second.authority });
  await assert.rejects(agentAuthority.accept({ leadership: first.authority }), /obsolete/);
  await assert.rejects(agentAuthority.accept({}), /omitted/);
  check('agents reject obsolete and missing leadership proofs', () => assert.equal(agentAuthority.current.epoch, second.authority.epoch));
  isolated = new Set();
  await until(async () => (await views()).every(v => v.raft?.leader === second.id));
  const replace = second.id;
  const warm = await start(replace, 'replacement');
  await until(async () => (await request(warm, '/readyz')).body?.voterReady, 45000);
  const warmView = await request(warm, '/_cp/status');
  const previous = await request(replace, '/_cp/status');
  check('an ordinary app replacement is admitted before retiring its predecessor', () => {
    assert.equal(warmView.body.raft.identityValid, true); assert.equal(previous.body.raft.identityValid, false);
    assert.equal(previous.body.primary, false);
  });
  await stop(replace); endpoints.set(replace, warm.port);
  const afterMove = await primary();
  await until(async () => (await request(replace, '/_cp/status')).body?.raft?.leader === afterMove.id);
  check('moving a primary transfers leadership and lets the replacement take its released vote', () => {
    assert.ok(processes.has('replacement')); assert.notEqual(afterMove.id, replace);
  });
  if (members.every(m => native(m.id))) {
    const lagging = [1, 2, 3].find(id => id !== afterMove.id && id !== replace);
    const promotedSlot = [4, 5, 6, 7].find(id => id !== afterMove.id && id !== replace);
    isolated = new Set([lagging]);
    const promoted = await start(promotedSlot, 'promoted');
    await until(async () => (await request(promoted, '/readyz')).body?.voterReady, 45000);
    const promotedView = (await request(promoted, '/_cp/status')).body;
    await stop(promotedSlot); endpoints.set(promotedSlot, promoted.port);
    processes.delete('promoted'); processes.set(String(promotedSlot), promoted);
    const behind = (await request(lagging, '/_cp/status')).body;
    check('an isolated voter misses the replacement identity before it becomes leader', () => {
      assert.equal(behind.raft.registry.nodes[String(promotedView.raft.node)], undefined);
    });
    assert.equal((await request(afterMove.id, `/_cp/transfer?to=${promotedSlot}`, { method: 'POST' })).status, 200);
    await until(async () => (await views()).find(v => v.id === promotedSlot && v.primary));
    corruptSender = { from: promotedSlot, to: lagging };
    isolated = new Set();
    await until(async () => corruptRejected > 0);
    const refusedSender = (await request(lagging, '/_cp/status')).body;
    check('a lagging voter rejects a claimed replacement leader with the wrong boot identity', () => {
      assert.equal(refusedSender.raft.registry.nodes[String(promotedView.raft.node)], undefined);
    });
    corruptSender = null;
    const caughtUp = await until(async () => {
      const v = (await request(lagging, '/_cp/status')).body;
      return v?.raft?.leader === promotedSlot && v.raft.registry.slots[String(promotedSlot)] === promotedView.raft.node && v;
    });
    check('a voter catches up from a newly elected physical identity after a partition', () => {
      assert.ok(caughtUp.raft.identityValid);
    });
  }
  for (const id of [4, 5, 6, 7]) await stop(id === replace ? 'replacement' : id);
  await sleep(3000);
  const blocked = await Promise.all([1, 2, 3].map(id => request(id, '/_cp/verify')));
  check('three surviving Mac-side voters cannot authorize control changes', () => assert.ok(blocked.every(r => r.status !== 200)));
  const survivingShell = await fetch(members[0].url + '/');
  const survivingView = await request(1, '/api/cluster');
  check('the same Fleet interface and local peer view remain available without quorum', () => {
    assert.equal(survivingShell.status, 200); assert.equal(survivingView.status, 200);
    assert.equal(survivingView.body.self, 1); assert.equal(survivingView.body.replicas.length, 7);
  });
  check('D1 has no election, membership, ownership or authority tables', () => {
    assert.deepEqual(db.query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'cp_%'"), []);
    assert.equal(survivingView.body.persistence.electionMetadata, 'memory');
  });
  if (members.every(m => native(m.id))) {
    const forcedProcess = await start(7, 'forced', true);
    endpoints.set(7, forcedProcess.port);
    const forced = await until(async () => (await views()).find(v => v.id === 7 && v.primary && v.forced));
    check('the startup flag starts emergency operations without an elected vote or quorum', () => {
      assert.equal(forced.raft.identityValid, false);
      assert.ok(forced.authority.epoch > second.authority.epoch);
    });
    await until(async () => (await request(1, '/api/status')).status === 200);
    const changed = await request(1, '/api/projects/example', { method: 'PUT',
      headers: { 'x-admin-password': env.ADMIN_PASSWORD }, body: JSON.stringify({ replicas: 2 }) });
    check('ordinary replicas forward authorized operations to the forced startup replica', () => {
      assert.equal(changed.status, 200); assert.equal(changed.body.replicas, 2);
    });
    const unauthorized = await request(7, '/api/settings', { method: 'PUT', body: JSON.stringify({ rebalance: false }) });
    check('forcing authority does not bypass normal operation authentication', () => assert.equal(unauthorized.status, 401));
    agentAuthority.url = members.find(m => m.id === 7).url;
    await agentAuthority.accept({ leadership: forced.authority });
    check('existing agents accept the forced replica proof and fence prior plans', () => {
      assert.equal(agentAuthority.current.epoch, forced.authority.epoch);
    });
    await stop('forced');
    const fenced = await until(async () => {
      const all = (await views()).filter(v => v.id !== 7);
      return all.length === 3 && all.every(v => v.raft.term > forced.authority.epoch && !v.primary) && all;
    }, 35000);
    check('after the forced process stops, surviving voters fence its epoch and keep quorum checks', () => assert.ok(fenced.every(v => v.raft.term > forced.authority.epoch && !v.primary)));
    await Promise.all([...processes.keys()].map(stop));
    env.CP_FORCE_OPERATIONS = '1';
    for (let id = 1; id <= 7; id++) await start(id);
    delete env.CP_FORCE_OPERATIONS;
    const fresh = await until(async () => {
      const all = await views(); return all.length === 7 && all.every(v => v.raft.phase === 'uninitialized' && !v.primary && !v.forced) && all;
    });
    check('an environment variable alone cannot enable the emergency startup override', () => assert.ok(fresh.every(v => !v.forced)));
    const recoveredSeed = bootstrapManifest({ members, quorum: 4 }, fresh.map(v => v.raft), Math.max(Date.now(), forced.authority.epoch + 1));
    for (const m of members) assert.equal((await request(m.id, '/_cp/bootstrap', { method: 'POST', body: JSON.stringify(recoveredSeed) })).status, 200);
    const recovered = await primary();
    agentAuthority.url = members.find(m => m.id === recovered.id).url;
    await agentAuthority.accept({ leadership: recovered.authority });
    check('restart without the flag restores elected operation and agents accept its newer authority', () => {
      assert.equal(recovered.forced, false); assert.ok(recovered.authority.epoch > forced.authority.epoch);
    });
  }
  if (process.env.HA_FAULT_MATRIX === '1') {
    assert.ok(members.every(m => native(m.id)), 'fault matrix requires all native replicas');
    const { runFailureScenarios } = await import('./ha-failures.mjs');
    await runFailureScenarios({ members, processes, start, stop, request, views, primary, until, check, sleep, db,
      isolate(ids = []) { isolated = new Set(ids); },
      link(from, to, fault) { linkFaults.set(from + ':' + to, { calls: 0, ...fault }); },
      clearNetwork() { isolated = new Set(); linkFaults.clear(); blockedPulls.clear(); blockedJoins.clear(); },
      blockPull(node, blocked) { if (blocked) blockedPulls.add(String(node)); else blockedPulls.delete(String(node)); },
      blockJoin(id, blocked) { if (blocked) blockedJoins.add(id); else blockedJoins.delete(id); },
      capture(enabled) { capturePackets = enabled; capturedPackets.length = 0; },
      packets: capturedPackets, networkStats,
      route(id, instance) { endpoints.set(id, instance.port); },
    });
  }
  console.log(checks + ' HA service checks passed');
  passed = true;
} catch (error) {
  failure = String(error.stack || error);
  for (const [name, log] of logs) console.error('replica ' + name + ': ' + log);
  throw error;
} finally {
  if (process.env.HA_RESULT) writeFileSync(process.env.HA_RESULT, JSON.stringify({ passed, failure, checks,
    seed: process.env.HA_FAULT_SEED || null, elapsedMs: Date.now() - testStarted,
    platform: process.platform, arch: process.arch, node: process.version, checkResults, networkStats }, null, 2) + '\n');
  if(process.env.HA_MEMORY_RESULT)writeFileSync(process.env.HA_MEMORY_RESULT,JSON.stringify({checks,memorySamples},null,2)+'\n');
  staleStore?.close();
  client.close();
  for (const timer of networkTimers) clearTimeout(timer);
  await Promise.all([...processes.keys()].map(stop));
  for (const server of [...proxies, cloudflare]) { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
  fixture.kill('SIGTERM'); await rm(root, { recursive: true, force: true });
}
