# Moving an existing fleet to the native runtime

New machines use `rust/run-agent.sh`. Existing foreground GitHub jobs must keep
their original process and deadline: terminating an agent normally ends its job
and destroys the machine's application containers.

`migration/agent.mjs` is a temporary administrator-only helper for those jobs.
It opens no server. On Linux it identifies exactly one agent using its executable,
saved machine slot, control URL, and constant-time comparison of the fleet join
credential. It verifies the packaged native binary's manifest, size and SHA-256.
Only then can it enable the matched Node process's loopback inspector and arrange
a graceful shutdown followed by `execve` under the original PID. The saved run
and start time remain unchanged. A failed native warmup restores the original
Node command, revision, PID, and workload inventory.

The real-Docker test in `test/rust-agent-hot-migration.py` covers wrong credentials,
wrong slots, checksum failure, startup rollback, successful migration, container
preservation, the original deadline, and repeated application. The
`Rust agent migration` workflow publishes only after this test passes. Both
architectures are packaged; the live GitHub host migration was validated on amd64.

Use a digest-pinned helper image. Its required private environment is
`CONTROL_URL`, `MIGRATION_FLEET_TOKEN` (the existing fleet join token), and
`FLEET_MACHINE`. Inspection is the default; `MIGRATION_MODE=apply` enables the
switch. `MIGRATION_APP` excludes the temporary helper's own container from the
preservation check. The container needs host PID and network namespaces,
privileged access, and the Docker socket. Lock the temporary app and supply its
credentials through the private app environment, never public Compose source.

When deployed as a managed app, use the entrypoint
`["/bin/sh", "-c", "sleep 3; node /migration/agent.mjs && exec sleep infinity"]`.
The short delay lets Compose's `up --wait` finish before the helper checks the
agent's saved busy state. After success only BusyBox sleep remains, keeping the
temporary app healthy without a resident Node process. Delete the app after
collecting the verification results. The active agent binary is staged under
the agent's private code directory, outside that app's files.

Upgrade agents before controllers: native agents recognize semantically
equivalent plans across the JavaScript and Rust serializers and preserve their
running containers. Verify actual executable checksums and application health
before replacing controllers.

Replace controllers individually and verify committed membership after every
step. A seven-member cluster requires four votes; a normal four-replica app
source update would replace all four at once. For a deployment with three
controllers outside the fleet, temporarily replace fleet slots 7, 6 and 5 with
separate native voters while scaling the fleet app down one replica at a time.
Only then update its sole remaining replica, and restore replicas one at a time.
Temporary voters must use `restart: no` and be removed after their returning
fleet counterparts are committed.

Every temporary voter must have a reachable configured peer origin. If it
temporarily uses a removed replica's numbered hostname, wait until the old
container is gone, preserve the original DNS record, and change only that exact
alias. Return the alias to the new machine tunnel before retiring the temporary
voter. Never bootstrap or lower quorum during a rolling migration.

Update the Mac supervisor's actual Compose file one service at a time: the
supervisor reapplies it every 30 seconds. Saving a separate candidate file alone
would allow it to restore the previous image. Keep the old images and volumes
available for rollback. Leadership changes retain the normal inventory recovery
window; let it finish before expecting placement changes to apply.
