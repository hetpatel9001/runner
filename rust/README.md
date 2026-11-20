# Native Runners runtime

This workspace implements the controller HTTP API, WebSockets, scheduler, SQLite/D1 adapter and Linux agent in Rust. Replicated controllers supervise the official Go `go.etcd.io/raft/v3` library helper inside the same container, with volatile Raft state and quorum-confirmed ReadIndex checks. Cloudflare's connector remains a separate upstream executable. Agents use the Docker API for observations and the Docker CLI for workload changes.

The existing application configuration, API, UI, recovery signatures, deployment ownership labels, and consensus wire format are retained. The `raft-proto` compatibility patch is documented in `vendor/raft-proto/RUNNERS-PATCH.md`.

## Build and test

Use Rust 1.100.0 or newer:

```sh
cd rust
cargo test --locked --workspace
cargo build --locked --release -p runner-control -p runner-agent -p runner-raft
```

The `Rust runtime` workflow builds and tests static Linux amd64 and arm64 binaries on native GitHub runners. It publishes versioned `control` and `agent` images under `ghcr.io/hetp4401/runner`. The controller uses a scratch image, with certificates, Cloudflare's connector, and verified agent downloads. The static official etcd Raft helper is included; no compiler or Node process runs in the container. The agent image contains its binary and Docker CLI. The old JavaScript download endpoints remain available during migration.

The controller image preserves UID 1000, `/data`, and the existing environment settings. `start`, `init`, `credentials`, `healthcheck`, and `--version` are native commands. Initialization is explicit; a missing configuration database never silently creates an empty fleet. Consensus bootstrap remains explicit through the compatible `/_cp/bootstrap` API and existing `control/cluster-init.mjs` operator tool.

The root `Dockerfile` pins the tested controller release by digest for the default Compose build. `rust/runtime-image` independently pins the agent bootstrap release; `rust/run-agent.sh` extracts its Linux agent on provider hosts, then `exec`s it without a resident launcher. GitHub machine jobs use this script and require a runtime published by their own repository. See [independent GitHub pools](../docs/independent-pools.md). Set `CONTROL_URL`, `JOIN_TOKEN`, `SERVER_TAG` and an absolute `RUNNER_DATA` as usual. `RUNNER_RUST_IMAGE` is an explicit release override, used by the Linux integration tests. Publishing source changes does not move the release pin automatically: update the relevant release pin after the complete Linux workflow passes.

The previous Node/Go image is retained in `control/legacy.Dockerfile`. The `control/legacy.compose.yaml` override selects it for offline SQLite-to-D1 migration and rollback tooling. Node-based operator and provider setup commands are one-shot tools, not resident services.

## Memory and failure handling

The processes use a single async runtime thread. Queues have count or byte limits, metric columns use compact numeric arrays, and history is compressed. Defaults include 24 MiB for compressed metric history, 8 MiB for the version cache, an 8 MiB compressed runtime tail, 4 MiB of pending metric input, and a 512 KiB global live-log relay. These are ceilings for particular buffers, not promises about total process memory.

Every configuration commit and issued plan checks current leadership. A lost D1 response is confirmed by its unique commit ID instead of replayed. Quorum loss stops the scheduler. Runtime observations and metrics remain in memory and never become durable election or assignment records.

D1 remains the source of truth. Historical application specifications use a private, disposable SQLite cache under `DATA_DIR/.native-config-cache`, with a 1 MiB page cache. Each startup reads D1 afresh; this cache is never used for recovery. Its files are removed on close, and abandoned caches are cleaned after crashes without touching caches held by another process. Scheduling, metrics, assignments, and consensus remain in memory. YAML aliases have limits on expanded size and nesting before an object tree can grow excessively.

Historical measurements of the earlier all-Rust consensus implementation (not this etcd-helper build): an isolated seven-replica test on an Apple Silicon Mac measured approximately 46 MiB total RSS for Rust versus 295 MiB for the original Node/Go processes after one application deployment, about 84% less. This excludes Cloudflare connectors and production history. A live rollout subsequently verified all 36 fleet agents: 32 were migrated in place, preserving their original process IDs, run lifetimes and 210 existing containers, while four already used Rust. Total agent RSS across those 32 machines fell from 3,342,479,360 to 163,299,328 bytes (about 95% less). This excludes Docker Engine, application and connector processes. In the completed three-Mac/four-fleet controller layout, the three Mac controller containers used about 117 MiB combined versus 939 MiB before migration, including their Cloudflare connectors. These production readings are snapshots from the November 14, 2026 rollout, not worst-case limits.

A read-only preflight loaded 18 real applications and 283 saved versions with zero D1 writes. Moving historical specifications out of the resident SQLite heap reduced that preflight from about 65 MiB to 24 MiB RSS. This measures configuration loading, not a fully active production controller. The Linux image smoke test measured approximately 6.4 MiB idle controller RSS and a 63 MB complete image; it includes Cloudflare's executable and agent downloads, with no compiler or Node runtime. The agent used about 3.7 MiB RSS while managing a real Docker workload; its roughly 218 MB image mostly contains Docker CLI, Buildx and Compose. That agent RSS excludes Docker Engine, router, connector and application processes.

## Migration order

See the [existing-fleet migration guide](../control/MIGRATING-RUST.md) for preserving foreground GitHub jobs, Mac supervisor settings, and temporary controller votes.

Validate binaries and actual Docker workloads before changing the live fleet. Install the native agent first while the original controller is serving. The agent preserves existing workload ownership, inventory, and connectors. It recognizes a presentation-only YAML change only after comparing all effective deployment inputs and the verified previous fingerprint, avoiding unnecessary workload rebuilds during the controller change.

Replace controllers one at a time, preserving the existing seven-member configuration, D1 credentials, signing keys, tunnel namespace, and at least four available voters. Keep at least four other committed voters available while replacing a process, and verify the replacement is a committed voter before starting the next replacement. When moving a logical slot between hosts, retire its temporary copy only after the returning copy is committed. Keep the old images and configuration available for rollback. Never bootstrap to bypass a missing quorum.

The native installer builds and verifies the new agent image before cutover and restores both release pointers and the previous container on failure. It does not delete application containers or volumes. Existing cached JavaScript releases remain available for rollback; native agent updates retain two recent native releases and the running executable. An explicit `AGENT_URL` mirror is honored for both installation and native updates.

The application test suites include deployed-controller specification and scheduling fixtures, 439 metrics events across all five chart ranges, API/authentication and live-log tests, D1 failure tests, and agent ownership and lifecycle checks. `control/test/ha-service.mjs` supports `RUST_CONTROL_BINARY` and optional `RUST_CONTROL_IDS` for full-native and mixed-version seven-member tests. `HA_MEMORY_RESULT` records the complete controller process trees for comparison. The read-only `check-config` example accepts a configuration JSON object on stdin and reports counts and memory without exposing credentials.

For native development, build the official Raft helper before testing the replicated controller:

```sh
CGO_ENABLED=0 go -C control/raft build -o /tmp/runner-etcd-raft .
CP_ETCD_RAFT_BINARY=/tmp/runner-etcd-raft cargo test --locked --workspace --manifest-path rust/Cargo.toml
```

`CP_ETCD_RAFT_BINARY` selects the helper for local tests. Packaged containers use `/usr/local/bin/runner-etcd-raft`. A helper failure stops its controller so the container supervisor can restart it with a fresh Raft identity.

The final controller image contains only static executables, certificates and agent assets. Build artifacts strip debug/symbol tables from the pinned Cloudflare connector. Agent installers and native download URLs retain identical bytes and checksums; the installer archive streams from the existing native executables plus small static tar headers, avoiding a second copy of both binaries while preserving direct file extraction by bootstrap clients. The private archive index is build metadata and is not served as a public artifact.
