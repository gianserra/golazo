import { spawnSync } from "node:child_process";
import process from "node:process";

const steps = [
  {
    name: "normal work, overlap reconciliation, failure replacement, and completion",
    command: process.execPath,
    args: ["scripts/run-cargo.mjs", "test", "--quiet", "coordination::rollout_scenarios"],
  },
  {
    name: "human escalation presentation, staleness, audit, and resumed work",
    command: process.execPath,
    args: [
      "scripts/run-cargo.mjs",
      "test",
      "--quiet",
      "human_escalation_journey_presents_blocks_rejects_stale_choice_and_resumes_work",
    ],
  },
  {
    name: "Supervisor outage safety, continued progress, and replay",
    command: process.execPath,
    args: [
      "scripts/run-cargo.mjs",
      "test",
      "--quiet",
      "supervisor_outage_does_not_gate_watchdog_safety_or_later_replay",
    ],
  },
  {
    name: "backup, restart, queue recovery, and dirty-worktree preservation",
    command: process.execPath,
    args: ["scripts/run-cargo.mjs", "test", "--quiet", "coordination::disaster_recovery"],
  },
  {
    name: "shadow mode, constrained pilot rollback, and evidence-gated expansion",
    command: process.execPath,
    args: ["scripts/run-cargo.mjs", "test", "--quiet", "coordination::rollout::tests"],
  },
  {
    name: "matched single-worker and pooled benchmark",
    command: process.execPath,
    args: [
      "scripts/run-cargo.mjs",
      "run",
      "--quiet",
      "--bin",
      "golazo-rollout-benchmark",
      "--",
      "rust-backend/coordination/fixtures/rollout-benchmark-reference.json",
    ],
  },
  {
    name: "operator dashboard tests",
    command: "pnpm",
    args: ["test:frontend"],
  },
  {
    name: "frontend type contract",
    command: "pnpm",
    args: ["typecheck"],
  },
  {
    name: "production frontend build",
    command: "pnpm",
    args: ["build:frontend"],
  },
  {
    name: "authoritative implementation tracker validation",
    command: process.execPath,
    args: [
      "scripts/run-cargo.mjs",
      "run",
      "--quiet",
      "--bin",
      "golazo-tracker",
      "--",
      "--root",
      ".goal-manager",
      "validate",
      "autonomous-worker-architecture",
    ],
  },
];

for (const [index, step] of steps.entries()) {
  process.stdout.write(`\n[${index + 1}/${steps.length}] ${step.name}\n`);
  const result = spawnSync(step.command, step.args, {
    cwd: process.cwd(),
    encoding: "utf8",
    stdio: "inherit",
  });
  if (result.error) {
    process.stderr.write(`Unable to start ${step.command}: ${result.error.message}\n`);
    process.exit(1);
  }
  if (result.status !== 0) {
    process.stderr.write(`Acceptance step failed: ${step.name}\n`);
    process.exit(result.status ?? 1);
  }
}

process.stdout.write("\nAutonomous worker acceptance demo passed.\n");
