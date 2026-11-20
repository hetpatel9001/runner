# Provider adapters

Machine supply is independent of app scheduling. The controller accepts machines
that authenticate and report a required `SERVER_TAG`; tags describe origins and
do not declare desired machine counts. Keep account APIs, quotas and targets in
provider adapters. The same controller and agent work with other Linux providers.

## GitHub Actions

[Fleet Provider](replicated-provider.md) is the only automatic machine trigger.
Each participating repository contains its own `watchdog.yml` (**Machine schedule**)
and `machine.yml`. There are no cron, push, release, or completion supply triggers.
The provider primary claims a UTC refill slot and signs a request bound to that
repository, default branch and slot. The serialized watchdog verifies the
signature and freshness, rejects repeated slots, then rechecks Actions inventory.
An unsigned manual request cannot launch a machine. Workflow reruns cannot
repeat a launch; disabling Fleet Provider pauses replenishment.

Configure each repository independently:

| Repository setting | Purpose |
| --- | --- |
| Variable `CONTROL_URL` | Stable HTTPS controller origin. |
| Secret `JOIN_TOKEN` | Fleet join token. |
| Variable `SERVER_TAG` | Distinct pool origin tag. |
| Variable `MACHINE_COUNT` | Desired capacity owned by this provider adapter. |
| Variable `CAPACITY_TRIGGER` | Must be `provider`; other values disable supply. |
| Variable `CAPACITY_PHASE_MIN` | Phase `0`–`9`, scaled to the refill interval. |
| Variable `CAPACITY_INTERVAL_MIN` | Refill interval: `1`, `2`, `5` or `10` minutes. |
| Secret `FLEET_PROVIDER_DISPATCH_KEY` | Private 64-character lowercase hex key, also stored as this provider's `dispatchKey`. |
| Variable `REGION` | Optional explicit location label. |

`POOL` and `POOL_SIZE` remain compatibility aliases. Each watchdog uses its own
repository-scoped GitHub token and image. See [independent pools](independent-pools.md).
Build and migration workflows are explicit maintenance actions, with no push trigger.

## Staggered supply

Each account requests at most one host per claimed interval. Distinct phases
interleave requests: at two minutes, phases `2`, `5`, `8` open at seconds
`24`, `60`, `96`. Pending jobs reserve capacity. Failed launches count toward
spacing. A host running for at least 210 minutes permits one warm overlap above
the target. Actual running and pending jobs are capped at target plus one for
new launches, including during migration from overlapping legacy handovers.
The provider never cancels healthy hosts to enforce its target.

The machine job only accepts a fresh, same-branch provider workflow dispatched
by `github-actions[bot]`. It starts the generic agent directly on its Ubuntu host;
apps run in Docker. The job has a six-hour timeout and reports a 355-minute
lifetime. The control plane evacuates apps before departure. No GitHub token
or dispatch key is passed to the agent or workloads.

Verify supply by checking Fleet Provider status and signed `workflow_dispatch`
runs titled **Machine schedule (refill …)**. Account restrictions, API quotas
and job queues still apply. Run regression checks with Node.js 24:

```sh
node --test .github/scripts/capacity.test.mjs providers/github/scheduler.test.mjs providers/github/quota.test.mjs
```
