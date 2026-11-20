import assert from 'node:assert/strict';
import { bootstrapManifest } from '../cluster-init.mjs';

// Real processes and HTTP transport, against the production native binary.
// Every endpoint, credential, database and network fault belongs to the fixture.
export async function runFailureScenarios(h) {
  const ids = h.members.map(m => m.id), headers = { 'x-admin-password': 'test-admin' };
  const seed = Number(process.env.HA_FAULT_SEED || 1);
  let randomState = seed >>> 0, floor = 0;
  const random = () => { randomState = (1664525 * randomState + 1013904223) >>> 0; return randomState / 2 ** 32; };
  console.log('fault matrix seed ' + seed);
  const status = async id => (await h.request(id, '/_cp/status')).body;
  const verify = id => h.request(id, '/_cp/verify');
  const active = async except => {
    const rows = await Promise.all(ids.filter(id => id !== except && h.processes.has(String(id))).map(status));
    return rows.find(v => v?.primary);
  };
  const nextPrimary = except => h.until(() => active(except), 45000);
  const aligned = () => h.until(async () => {
    const rows = await h.views();
    const leader = rows.find(v => v.primary);
    return rows.length === 7 && leader && rows.every(v => v.raft.identityValid && !v.raft.outgoing.length &&
      !v.raft.learners.length && v.raft.leader === leader.id &&
      leader.raft.registry.slots[String(v.id)] === v.raft.node) && leader;
  }, 60000);
  async function onlyAuthority(expected) {
    const replies = await Promise.all(ids.map(async id => ({ id, ...await verify(id) })));
    assert.deepEqual(replies.filter(r => r.status === 200).map(r => r.id), [expected]);
  }
  async function restart(id) {
    const before = (await status(id))?.raft.node;
    await h.stop(id, 'SIGKILL'); await h.start(id);
    const after = await h.until(async () => {
      const v = await status(id);
      return v?.raft.identityValid && !v.raft.outgoing.length && v;
    }, 60000);
    assert.notEqual(after.raft.node, before);
    return after;
  }
  async function reset() {
    h.clearNetwork();
    for (const v of await h.views()) floor = Math.max(floor, v.raft?.term || 0, v.authority?.epoch || 0);
    await Promise.all([...h.processes.keys()].map(key => h.stop(key, 'SIGKILL')));
    for (const id of ids) await h.start(id);
    const fresh = await h.until(async () => {
      const rows = await h.views();
      return rows.length === 7 && rows.every(v => v.raft.phase === 'uninitialized' && !v.primary) && rows;
    });
    const manifest = bootstrapManifest({ members: h.members, quorum: 4 }, fresh.map(v => v.raft), Math.max(Date.now(), floor + 1));
    for (const id of ids) assert.equal((await h.request(id, '/_cp/bootstrap', { method: 'POST', body: JSON.stringify(manifest) })).status, 200);
    floor = manifest.term;
    return aligned();
  }
  async function mutation(id, name) {
    return h.request(id, '/api/projects/' + name, { method: 'PUT', headers,
      body: JSON.stringify({ compose: 'services:\n  app:\n    image: nginx\n', replicas: 1 }) });
  }

  // Keep the old process alive after promotion. It may miss its final removal
  // and retry join with its old boot; that must never displace the replacement.
  let before = await aligned();
  const warmSlot = ids.find(id => id !== before.id);
  const oldWarm = await status(warmSlot);
  const warm = await h.start(warmSlot, 'warm-retirement-test');
  await h.until(async () => {
    const r = (await h.request(warm, '/_cp/status')).body?.raft;
    return r?.identityValid && !r.outgoing.length && !r.registry.nodes[String(oldWarm.raft.node)];
  }, 60000);
  h.route(warmSlot, warm);
  for (let attempt = 0; attempt < 3; attempt++) {
    const rejected = await h.request(before.id, '/_cp/join', { method: 'POST',
      body: JSON.stringify({ id: warmSlot, node: oldWarm.raft.node, boot: oldWarm.raft.boot }) });
    assert.equal(rejected.status, 409);
    assert.match(rejected.body.error, /retired incarnation/);
  }
  const handoffLeader = await aligned();
  const otherLeader = ids.find(id => id !== handoffLeader.id && id !== warmSlot);
  assert.equal((await h.request(handoffLeader.id, '/_cp/transfer?to=' + otherLeader, { method: 'POST' })).status, 200);
  await h.until(async () => (await status(otherLeader))?.primary);
  const afterTransfer = await h.request(otherLeader, '/_cp/join', { method: 'POST',
    body: JSON.stringify({ id: warmSlot, node: oldWarm.raft.node, boot: oldWarm.raft.boot }) });
  assert.equal(afterTransfer.status, 409);
  await h.sleep(3000);
  assert.equal((await h.request(warm, '/_cp/status')).body.raft.identityValid, true);
  await h.stop(warmSlot, 'SIGKILL'); await h.stop('warm-retirement-test', 'SIGKILL');
  await h.start(warmSlot); await aligned();
  h.check('a removed incarnation cannot reclaim its slot during warm overlap or after a primary handoff', () => assert.match(afterTransfer.body.error, /retired incarnation/));
  const snapshotLeader = await aligned();
  assert.equal((await h.request(snapshotLeader.id, '/_cp/transfer?to=' + warmSlot, { method: 'POST' })).status, 200);
  await h.until(async () => (await status(warmSlot))?.primary);
  const fromSnapshot = await h.request(warmSlot, '/_cp/join', { method: 'POST',
    body: JSON.stringify({ id: warmSlot, node: oldWarm.raft.node, boot: oldWarm.raft.boot }) });
  h.check('a cold replacement recovers revoked identities from its memory snapshot before becoming primary', () => {
    assert.equal(fromSnapshot.status, 409); assert.match(fromSnapshot.body.error, /retired incarnation/);
  });

  before = await aligned();
  const killed = before.id, oldNode = before.raft.node;
  await h.stop(killed, 'SIGKILL');
  const afterCrash = await nextPrimary(killed);
  assert.ok(afterCrash.authority.term > before.authority.term);
  await onlyAuthority(afterCrash.id);
  await h.start(killed); await aligned();
  assert.notEqual((await status(killed)).raft.node, oldNode);
  assert.notEqual((await h.request(afterCrash.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(before.authority) })).status, 200);
  h.check('SIGKILL of the primary elects one replacement and the lost voter rejoins with a new identity', () => {
    assert.notEqual(afterCrash.id, killed);
  });

  before = await aligned();
  const followers = ids.filter(id => id !== before.id).slice(0, 3);
  await Promise.all(followers.map(id => h.stop(id, 'SIGKILL')));
  const four = await nextPrimary();
  assert.equal((await mutation(four.id, 'four-voters-can-write')).status, 200);
  await onlyAuthority(four.id);
  for (const id of followers) { await h.start(id); await h.until(async () => (await status(id))?.raft.identityValid, 60000); }
  await aligned();
  h.check('three simultaneous follower crashes leave four voters able to commit and recover all copies', () => assert.equal(four.raft.quorum, 4));

  before = await aligned();
  const two = ids.filter(id => id !== before.id).slice(0, 2);
  const old = await Promise.all(two.map(status));
  await Promise.all(two.map(id => h.stop(id, 'SIGKILL')));
  await Promise.all(two.map(id => h.start(id)));
  await aligned();
  for (let i = 0; i < two.length; i++) assert.notEqual((await status(two[i])).raft.node, old[i].raft.node);
  h.check('two simultaneous cold restarts serialize learner admission and restore seven distinct voters', () => {
    assert.equal(new Set(old.map(v => v.raft.node)).size, 2);
  });

  // Exercise each direction separately: a one-way link is not a symmetric partition.
  for (const direction of ['outbound', 'inbound']) {
    before = await aligned();
    for (const id of ids.filter(id => id !== before.id)) h.link(direction === 'outbound' ? before.id : id,
      direction === 'outbound' ? id : before.id, { drop: true });
    const elected = await nextPrimary(before.id);
    await onlyAuthority(elected.id);
    assert.notEqual((await h.request(before.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(before.authority) })).status, 200);
    h.clearNetwork(); await aligned();
    h.check('a primary with only ' + direction + ' connectivity lost cannot retain acting authority', () => assert.notEqual(elected.id, before.id));
  }

  before = await aligned();
  const order = [before.id, ...ids.filter(id => id !== before.id)], groups = [order.slice(0, 3), order.slice(3, 5), order.slice(5)];
  const group = id => groups.findIndex(g => g.includes(id));
  for (const from of ids) for (const to of ids) if (group(from) !== group(to)) h.link(from, to, { drop: true });
  const noQuorum = await Promise.all(ids.map(verify));
  assert.ok(noQuorum.every(r => r.status !== 200));
  assert.notEqual((await mutation(before.id, 'partition-must-not-write')).status, 200);
  assert.equal(h.db.query("SELECT name FROM projects WHERE name='partition-must-not-write'").length, 0);
  h.clearNetwork(); h.isolate(groups[0]);
  const fourSide = await h.until(async () => {
    const rows = await h.views(); return rows.find(v => !groups[0].includes(v.id) && v.primary);
  }, 45000);
  await onlyAuthority(fourSide.id);
  h.clearNetwork(); await aligned();
  h.check('a 3/2/2 partition blocks writes and healing the two pairs restores exactly one authorized primary', () => assert.ok(!groups[0].includes(fourSide.id)));

  before = await aligned();
  const droppedBefore = h.networkStats.dropped, delayedBefore = h.networkStats.delayed;
  for (const from of ids) for (const to of ids) if (from !== to) h.link(from, to,
    { dropEvery: 4 + Math.floor(random() * 3), delay: 15 + Math.floor(random() * 50), responseDelay: 10 + Math.floor(random() * 30) });
  for (let i = 0; i < 5; i++) {
    await h.until(async () => {
      const leader = await active();
      if (!leader) return false;
      const result = await mutation(leader.id, 'lossy-link-' + String.fromCharCode(97 + i));
      assert.notEqual(result.status, 400, JSON.stringify(result.body));
      return result.status === 200;
    }, 45000);
    await h.sleep(200);
  }
  h.clearNetwork(); const afterLoss = await aligned(); await onlyAuthority(afterLoss.id);
  h.check('seeded packet loss and request/response latency allow guarded writes and converge after healing', () => {
    assert.ok(h.networkStats.dropped > droppedBefore); assert.ok(h.networkStats.delayed > delayedBefore);
  });

  before = await aligned();
  const lateBefore = h.networkStats.lateDelivered;
  for (const id of ids.filter(id => id !== before.id)) h.link(id, before.id, { delay: 5000 });
  const pending = h.request(before.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(before.authority) });
  await h.sleep(250);
  h.isolate([before.id, ...ids.filter(id => id !== before.id).slice(0, 2)]);
  const afterDelayed = await h.until(async () => (await h.views()).find(v => v.primary && v.authority.term > before.authority.term), 45000);
  assert.notEqual((await pending).status, 200);
  h.clearNetwork();
  await h.until(async () => h.networkStats.lateDelivered > lateBefore, 20000);
  await aligned();
  assert.notEqual((await h.request(before.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(before.authority) })).status, 200);
  h.check('quorum messages delivered after their RPC times out cannot revive a superseded primary proof', () => assert.ok(afterDelayed.authority.term > before.authority.term));

  before = await aligned();
  const paused = h.processes.get(String(before.id));
  paused.child.kill('SIGSTOP');
  let resumed;
  try {
    resumed = await nextPrimary(before.id);
    await h.sleep(15000); // exceed the old primary's 14-second local cache
  } finally { paused.child.kill('SIGCONT'); }
  await aligned();
  assert.notEqual((await h.request(before.id, '/_cp/verify', { method: 'POST', body: JSON.stringify(before.authority) })).status, 200);
  h.check('a frozen primary expires and resuming it cannot restore its obsolete authority', () => assert.ok(resumed.authority.term > before.authority.term));

  before = await aligned();
  const learnerSlot = ids.find(id => id !== before.id);
  h.blockJoin(learnerSlot, true);
  const doomed = await h.start(learnerSlot, 'doomed-learner');
  const doomedView = await h.until(async () => {
    const v = (await h.request(doomed, '/_cp/status')).body?.raft;
    return v?.node > 0 && v.boot && v;
  });
  h.blockPull(doomedView.node, true); h.blockJoin(learnerSlot, false);
  await h.until(async () => (await status(before.id))?.raft.learners.includes(doomedView.node));
  assert.equal((await h.request(doomed, '/readyz')).body.voterReady, false);
  await h.stop('doomed-learner', 'SIGKILL');
  await h.until(async () => {
    const v = await status(before.id);
    return v && !v.raft.learners.includes(doomedView.node) && !v.raft.outgoing.length;
  }, 115000);
  h.blockPull(doomedView.node, false);
  const replacement = await restart(learnerSlot); await aligned();
  h.check('a learner killed before catch-up is discarded and cannot strand subsequent replacements', () => assert.equal(replacement.raft.identityValid, true));

  before = await aligned();
  h.capture(true);
  await h.until(async () => h.packets.length > 0);
  const oldPacket = h.packets[0], oldGeneration = before.raft.generation;
  h.capture(false);
  const lost = ids.filter(id => id !== before.id).slice(0, 4);
  await Promise.all(lost.map(id => h.stop(id, 'SIGKILL')));
  assert.ok((await Promise.all(ids.map(verify))).every(r => r.status !== 200));
  assert.notEqual((await mutation(before.id, 'lost-majority-must-not-write')).status, 200);
  assert.equal(h.db.query("SELECT name FROM projects WHERE name='lost-majority-must-not-write'").length, 0);
  const freshFourth = await h.start(lost[0]);
  await h.until(async () => (await h.request(freshFourth, '/_cp/status')).body?.raft);
  await h.sleep(3000);
  assert.equal((await h.request(freshFourth, '/_cp/status')).body.raft.identityValid, false);
  assert.ok((await Promise.all(ids.map(verify))).every(r => r.status !== 200));
  h.check('losing four voters blocks writes and a fresh fourth process cannot manufacture the lost vote', () => assert.equal(h.processes.size, 4));

  const recovered = await reset();
  const replay = await fetch(h.members.find(m => m.id === oldPacket.to).url + oldPacket.path,
    { method: 'POST', headers: oldPacket.headers, body: oldPacket.bytes, signal: AbortSignal.timeout(6000) });
  assert.equal(replay.status, 409);
  h.capture(true); await h.until(async () => h.packets.length > 0); const currentPacket = h.packets[0]; h.capture(false);
  const wrongGeneration = await fetch(h.members.find(m => m.id === currentPacket.to).url + currentPacket.path,
    { method: 'POST', headers: { ...currentPacket.headers, 'x-cp-generation': oldGeneration },
      body: currentPacket.bytes, signal: AbortSignal.timeout(6000) });
  assert.equal(wrongGeneration.status, 409);
  assert.ok(h.db.query("SELECT name FROM projects WHERE name='four-voters-can-write'").length);
  await onlyAuthority(recovered.id);
  h.check('a clean bootstrap recovers after quorum memory loss and rejects old-incarnation and old-generation packets', () => assert.notEqual(recovered.raft.generation, oldGeneration));

  // The deployed layout is one Mac slot and six runner slots.
  const current = await aligned();
  if (current.id !== 1) assert.equal((await h.request(current.id, '/_cp/transfer?to=1', { method: 'POST' })).status, 200);
  await h.until(async () => (await status(1))?.primary);
  await h.stop(1, 'SIGKILL');
  const fleet = await nextPrimary(1); await onlyAuthority(fleet.id);
  await h.start(1); await aligned();
  h.check('the six runner replicas retain authority after the Mac primary is killed', () => assert.ok(fleet.id > 1));

  await Promise.all(ids.filter(id => id !== 1).map(id => h.stop(id, 'SIGKILL')));
  assert.notEqual((await verify(1)).status, 200);
  assert.notEqual((await mutation(1, 'lone-mac-must-not-write')).status, 200);
  assert.equal(h.db.query("SELECT name FROM projects WHERE name='lone-mac-must-not-write'").length, 0);
  await h.stop(1, 'SIGKILL'); // fence the old incarnation before the override
  await h.start(1, '1', true);
  const forced = await h.until(async () => { const v = await status(1); return v?.primary && v.forced && v; });
  assert.equal((await mutation(1, 'explicit-lone-mac-recovery')).status, 200);
  floor = Math.max(floor, forced.authority.epoch);
  const restored = await reset();
  assert.equal(restored.forced, false);
  await onlyAuthority(restored.id);
  h.check('a lone Mac refuses normal writes, permits the explicit startup override, and returns to quorum mode after bootstrap', () => assert.ok(restored.authority.epoch > forced.authority.epoch));

  const recoverySlot = ids.find(id => id !== restored.id);
  await h.stop(recoverySlot, 'SIGKILL');
  await h.start(recoverySlot, String(recoverySlot), false, { RECOVER_RUNTIME: '1', RECOVERY_MS: '60000' });
  await aligned();
  const incumbent = await aligned();
  assert.equal((await h.request(incumbent.id, '/_cp/transfer?to=' + recoverySlot, { method: 'POST' })).status, 200);
  const recovering = await h.until(async () => { const v = await status(recoverySlot); return v?.primary && v; });
  const initialStatus = await h.request(recoverySlot, '/api/status');
  const deadline = initialStatus.body.controlPlane.recoveryUntil;
  assert.ok(deadline > Date.now() + 5000);
  for (let attempt = 0; attempt < 3; attempt++) {
    const retry = await h.request(recoverySlot, '/api/join', { method: 'POST',
      headers: { authorization: 'Bearer test-join-token' }, body: JSON.stringify({ agent: 'recovery-test', tag: 'test' }) });
    assert.equal(retry.status, 503);
    assert.equal(retry.body.error, 'recovering fleet inventory; retry joining shortly');
    assert.equal((await h.request(recoverySlot, '/readyz')).status, 200);
    const after = await h.request(recoverySlot, '/api/status');
    assert.equal(after.status, 200);
    assert.equal(after.body.controlPlane.recoveryUntil, deadline);
    assert.equal((await status(recoverySlot)).raft.term, recovering.raft.term);
  }
  assert.equal((await mutation(recoverySlot, 'recovery-retries-keep-primary')).status, 200);
  h.check('repeated recovery 503 responses preserve the elected scheduler and its original recovery deadline', () => assert.ok(deadline > Date.now()));
  await Promise.all(ids.filter(id => id !== recoverySlot).slice(0, 4).map(id => h.stop(id, 'SIGKILL')));
  assert.notEqual((await verify(recoverySlot)).status, 200);
  assert.notEqual((await mutation(recoverySlot, 'recovery-quorum-lost-must-not-write')).status, 200);
  h.check('handling retryable recovery responses does not bypass lost-quorum fencing', () =>
    assert.equal(h.db.query("SELECT name FROM projects WHERE name='recovery-quorum-lost-must-not-write'").length, 0));
  console.log('fault matrix seed ' + seed + ' completed');
}
