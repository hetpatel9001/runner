# Independent GitHub pools

Each pool repository contains the complete runtime source, its two capacity
workflows, the capacity scripts, and the native build workflow. Machine and
watchdog jobs check out their own repository. They do not use `AGENT_REPO`.
Keep the existing per-pool `SERVER_TAG`, target, phase, controller URL, and
encrypted `JOIN_TOKEN` secret.

Run **Rust runtime** in that repository. It tests and builds both Linux
architectures and publishes controller, agent, and Fleet Provider images under
`ghcr.io/<owner>/<repository>/`. Images are associated with that repository;
its workflow token can read its own packages without a cross-account token.

After the complete build succeeds, obtain the controller image digest from
the `pool-runtime-images` artifact's `control-image.json`
(`containerimage.digest`). Set `rust/runtime-image` to
`ghcr.io/<owner>/<repository>/control@sha256:<digest>`. Update the root
Dockerfile and the optional migration helper's native image pin at the same
time if those entrypoints are used. Publish the local-checkout machine and
watchdog workflows only after that runtime exists.

The machine workflow validates that the pin belongs to its own repository.
It pulls the pinned image with a temporary Docker authentication directory,
then removes that directory before starting the agent. The agent uses the
already cached image to extract its native executable; it never needs a
different pool's source repository or container package. No registry token
is passed to the running agent or workloads.

Updates are explicit per repository: build, validate, then advance that
repository's digest. Existing machines keep their running code and rotate
normally. The replicated controller and Fleet Provider still coordinate the
fleet, and hosts still need GitHub, GHCR, and the configured controller.
