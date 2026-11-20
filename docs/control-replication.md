# Replicated control plane

`CP_REPLICAS=1` runs one ordinary controller. An odd count from 3 to 51 enables
Raft; seven replicas require four votes. Each replica runs the Rust controller
and a supervised static helper using the official
[`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft) library in the same Docker
container. No etcd database server is required. Agent binaries and the default
agent image are unchanged; Fleet Provider keeps its own independent cluster.

## Consensus and authority

The upstream library handles elections, pre-voting, quorum checks, replication,
progress tracking, snapshots, leadership transfer and joint membership changes.
The controller uses quorum-confirmed `ReadIndex` checks before guarded work and
before releasing plans. A stale leader cannot authorize work just because its
last local status still says leader. Requests already submitted to D1 or
Cloudflare may finish after demotion; revision checks protect configuration and
the current primary reconciles routing.

The Rust adapter supervises the helper and forwards authenticated peer traffic.
If the helper exits, its controller stops; a killed controller also causes its
helper to exit. The helper listens only on loopback. `CP_ETCD_RAFT_BINARY` selects
its path for native development; the packaged path is
`/usr/local/bin/runner-etcd-raft`.

## Memory, rejoin and history

Raft uses `MemoryStorage`; terms, votes, protocol entries, membership and
snapshots are never written to disk or D1. Each new process receives a fresh
physical Raft ID and boot token. It joins a surviving quorum as a learner, catches
up using Raft replication/snapshots, then replaces its predecessor through joint
consensus. A forgotten process identity is never reused as a voter. This also
supports normal Docker app moves where predecessor and replacement overlap.
Removed identities remain rejected within the live generation.

Losing or truncating primary history is acceptable. The audit history contains
only primary changes, with no replicated reports, metrics or application events.
Every 30 minutes it keeps the newest 100 records; an emergency 512-record bound
limits memory and peer payloads. Audit history is independent of the Raft log and
never affects voting or scheduling authority. Restart may discard it completely.
D1 stores desired application configuration; agents supply signed runtime
inventory for recovery.

A surviving quorum can admit restarted replicas automatically. If a quorum loses
all its memory, replacement processes cannot recreate those votes by themselves.
Stop every old incarnation, then explicitly bootstrap a new generation. History
loss alone does not justify reusing a forgotten voter or bootstrapping across a
network partition. No disk persistence is required for this recovery policy.

## Deployment settings

All replicas share the existing D1, Cloudflare, admin, join-token, signing and
tunnel settings, plus:

- `CP_REPLICAS`: fixed odd number of logical replica slots.
- `CP_CLUSTER_ID`: stable cluster identity.
- `CP_CLUSTER_TOKEN`: private peer authentication secret, at least 32 characters.
- `CP_MEMBERS`: unique `{ "id": 1, "url": "https://cp-1.example.com" }` origins.
- `CP_NODE_ID`: this replica's logical slot.
- `CP_RAFT_TICK_MS`, `CP_RAFT_ELECTION_TICKS`, `CP_RAFT_HEARTBEAT_TICKS`: shared
  timing settings; defaults are 500 ms, 20 ticks and 2 ticks.

Use peer origins distinct from the shared primary hostname. `CP_PEER_OVERRIDES`
can provide private routes. Fleet copies are an ordinary app. With seven slots,
`CP_NODE_ID_OFFSET=1` maps app replicas 1–6 to slots 2–7 and replica 7 to slot 1.
This lets a seventh runner replace an external controller without renumbering
the six existing runner voters. An offset of zero maps replicas directly to
their slots. Replica numbers and the offset must fit the configured slot count.
`x-runner.spread-tags: true` prefers different server groups; it can reuse a group
when there is insufficient eligible capacity. Readiness requires completed voter admission and no pending joint change.
Liveness and the Fleet dashboard remain available without quorum.

For the first startup or a complete loss of quorum memory, start fresh replicas
and use the private deployment environment after stopping all old incarnations:

```sh
node control/cluster-init.mjs status
node control/cluster-init.mjs bootstrap --confirm-old-cluster-stopped
```

The manifest exists only in memory. Initialized replicas refuse a different
generation. After a clock rollback, set `CP_BOOTSTRAP_TERM` above epochs still
remembered by agents. Do not mix custom Paxos-lease replicas with Raft replicas.

The existing `start --force-operations` recovery override still requires stopping
all potentially active old schedulers first. It bypasses quorum for the selected
process, preserves ordinary authentication, and ends when that process restarts
without the flag. Never put it on a shared app definition.

## Inspection and verification

`/#/fleet` and `/api/cluster` show each replica's view. Private `/_cp/status` and
`/_cp/raft-status` show application and Raft state. `/_cp/history` returns the
disposable primary history. All `/_cp/*` endpoints require the cluster secret.

Build the native helper before running the Rust workspace tests:

```sh
CGO_ENABLED=0 go -C control/raft build -o /tmp/runner-etcd-raft .
CP_ETCD_RAFT_BINARY=/tmp/runner-etcd-raft \
  cargo test --locked --workspace --manifest-path rust/Cargo.toml
RUST_CONTROL_BINARY=/path/to/runner-control \
  CP_ETCD_RAFT_BINARY=/tmp/runner-etcd-raft \
  HA_FAULT_MATRIX=1 HA_FAULT_SEED=41 \
  node control/test/ha-service.mjs
```

The fault suite runs seven real controller/helper pairs with isolated D1 and
Cloudflare fixtures. It covers primary death and pause, partitions, stale
plans/writes, warm replacements, missed membership snapshots, dropped and delayed
traffic, restarted followers, quorum memory loss and explicit fresh bootstrap.
Docker CI builds Linux amd64/arm64 binaries and checks the packaged controller,
existing agent compatibility and installer rollback.
