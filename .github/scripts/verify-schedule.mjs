import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { githubClient } from "./capacity.mjs";

export async function verifyScheduledOrigin(env, api, now = Date.now) {
  if (!/^[1-9]\d*$/.test(env.SCHEDULE_RUN || "")) throw new Error("a Fleet Provider workflow run is required");
  if (env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_ACTOR !== "github-actions[bot]" ||
      (env.GITHUB_RUN_ATTEMPT && env.GITHUB_RUN_ATTEMPT !== "1"))
    throw new Error("only the Fleet Provider workflow can start a machine");
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY || "")) throw new Error("GITHUB_REPOSITORY is required");
  if (!api && !env.GH_TOKEN) throw new Error("GH_TOKEN is required");
  api ||= githubClient(env.GH_TOKEN, env.GITHUB_API_URL);
  const run = await api(`/repos/${env.GITHUB_REPOSITORY}/actions/runs/${env.SCHEDULE_RUN}`);
  const match = /^Machine schedule \(refill ([1-9]\d{12})\)$/.exec(run.display_title || "");
  const slot = Number(match?.[1]);
  if (run.event !== "workflow_dispatch" || !match || slot > now() || now() - slot >= 10 * 60_000 ||
      run.path !== ".github/workflows/watchdog.yml" || run.head_branch !== env.GITHUB_REF_NAME || run.run_attempt !== 1)
    throw new Error("host requests must come from this branch's fresh Fleet Provider workflow");
  return true;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  verifyScheduledOrigin(process.env).catch(error => { console.error(error.message); process.exitCode = 1; });
}
