// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AssistantMessageContent,
  AutonomousDetailsDisclosure,
  AutonomousExecutionDisclosure,
  GoalClaimPackageDetails,
  GoalEscalationInbox,
  GoalIntegrationView,
  GoalPoolControls,
  GoalPoolSummaryCard,
  GoalWorkerDock,
  GoalWorkers,
  api,
  latestWorkerRunActivity,
  loadGoalPoolSummary,
  parseWorkerCompletionResult,
  parseWorkerStructuredUpdate,
  preservedChatScrollTop,
  ScrollToLatestButton,
  SupervisorRuntimeCard,
  sameThreadTurns,
  unreadResponseDelta,
  workerNeedsCurrentAttention,
  WorkingUpdateContent,
} from "./App";
import type {
  Goal,
  GoalEscalationData,
  GoalIntegrationArtifactData,
  GoalPoolSummary,
  GoalWorkerCardData,
} from "./App";

const now = "2026-10-05T15:00:00.000Z";

function resource<T>(id: string, data: T, resourceVersion = "1") {
  return {
    apiVersion: "v1" as const,
    kind: "test",
    metadata: { id, resourceVersion, createdAt: now, updatedAt: now },
    data,
  };
}

function summary(overrides: Partial<GoalPoolSummary> = {}): GoalPoolSummary {
  return {
    refreshedAt: now,
    poolResourceVersion: "7",
    desiredConcurrency: 2,
    activeWorkers: 1,
    readyWork: 1,
    blockedWork: 0,
    queuedIntegrations: 0,
    runningIntegrations: 0,
    failedIntegrations: 0,
    mode: "running",
    health: "healthy",
    healthLabel: "Healthy",
    supervisor: null,
    readyUnits: [],
    workers: [],
    claims: [],
    packages: [],
    contracts: [],
    integrations: [],
    delivery: null,
    activity: [],
    escalations: [],
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("API responses", () => {
  it("accepts an empty 204 approval response without parsing JSON", async () => {
    const json = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, status: 204, json }) as unknown as Response));

    await expect(api<void>("/app-server/approvals/approval-1", { method: "POST" })).resolves.toBeUndefined();
    expect(json).not.toHaveBeenCalled();
  });
});

describe("worker completion presentation", () => {
  it("renders the structured worker result as a readable summary with technical detail collapsed", () => {
    const payload = JSON.stringify({
      completedStepIds: ["harden-finding-contract"],
      evidenceRefs: ["src/Domain/Finding.cs:3", "dotnet build: succeeded"],
      knownRisks: ["The trusted integration boundary still needs to run the full test suite."],
      summary: "Hardened the finding contract and added invariant coverage.",
      validationCommands: ["dotnet build tests/Domain.Tests/Domain.Tests.csproj", "git diff --check"],
    });
    expect(parseWorkerCompletionResult(payload)?.completedStepIds).toEqual(["harden-finding-contract"]);
    expect(parseWorkerCompletionResult('{"unrelated":true}')).toBeNull();

    render(<AssistantMessageContent text={payload} />);
    expect(screen.getByText("Worker completed")).toBeTruthy();
    expect(screen.getByText("Hardened the finding contract and added invariant coverage.")).toBeTruthy();
    expect(screen.getByText("Harden finding contract")).toBeTruthy();
    expect(screen.getByText("Verification gaps")).toBeTruthy();
    expect(screen.getByText("Tests not run")).toBeTruthy();
    expect(screen.getByText("Validation")).toBeTruthy();
    expect(screen.getByText("Evidence")).toBeTruthy();
    expect(screen.getByText("Raw structured result")).toBeTruthy();
  });

  it("renders a structured working update as progress instead of raw JSON", () => {
    const payload = JSON.stringify({
      completedStepIds: [],
      evidenceRefs: ["src/DocumentSourceAddress.cs", "tests/SourceAddressingTests.cs"],
      knownRisks: [],
      summary: "Source-path IDs are implemented and focused validation is starting.",
      validationCommands: ["dotnet test tests/AbaQa.Domain.Tests/AbaQa.Domain.Tests.csproj"],
    });

    expect(parseWorkerStructuredUpdate(payload)?.completedStepIds).toEqual([]);
    expect(parseWorkerCompletionResult(payload)?.completedStepIds).toEqual([]);
    render(<WorkingUpdateContent text={payload} />);
    expect(screen.getByText("Worker progress")).toBeTruthy();
    expect(screen.getByText("Source-path IDs are implemented and focused validation is starting.")).toBeTruthy();
    expect(screen.getByText("Planned validation")).toBeTruthy();
    expect(screen.getByText("Evidence so far")).toBeTruthy();
    expect(screen.getByText("Raw structured update")).toBeTruthy();
  });

  it("renders a final partial result without pretending a tracker step completed", () => {
    const payload = JSON.stringify({
      completedStepIds: [],
      evidenceRefs: ["src/Rules/ConsistencyRule.cs"],
      knownRisks: ["One acceptance case remains."],
      summary: "Integrated a verified portion of the consistency rules.",
      validationCommands: ["dotnet build"],
    });

    render(<AssistantMessageContent text={payload} />);
    expect(screen.getByText("Worker progress recorded")).toBeTruthy();
    expect(screen.getByText(/No tracker step was marked complete/)).toBeTruthy();
    expect(screen.queryByText("Worker completed")).toBeNull();
  });

  it("hides incomplete streaming JSON until the structured update is readable", () => {
    render(<WorkingUpdateContent text={'{"completedStepIds":[],"summary":"Still validating"'} />);
    expect(screen.getByText("Receiving structured worker update…")).toBeTruthy();
    expect(screen.queryByText(/completedStepIds/)).toBeNull();
  });

  it("uses the latest Codex commentary as live worker activity", () => {
    const run = {
      id: "run-live",
      status: "running",
      created_at: "2026-10-05T14:00:00.000Z",
      started_at: "2026-10-05T14:00:01.000Z",
      finished_at: null,
      final_message: null,
      events: [
        {
          method: "item/completed",
          params: {
            completedAtMs: 1_759_674_010_000,
            item: { id: "message-1", type: "agentMessage", phase: "commentary", text: "Inspecting the parser." },
          },
        },
        {
          method: "item/started",
          params: {
            startedAtMs: 1_759_674_020_000,
            item: { id: "message-2", type: "agentMessage", phase: "commentary", text: "" },
          },
        },
        { method: "item/agentMessage/delta", params: { itemId: "message-2", delta: "Focused tests " } },
        { method: "item/agentMessage/delta", params: { itemId: "message-2", delta: "are running now." } },
      ],
    };

    expect(latestWorkerRunActivity(run as never)).toEqual({
      summary: "Focused tests are running now.",
      at: "2025-10-05T14:20:20.000Z",
    });
  });

  it("uses a completed worker's structured final summary instead of a lifecycle label", () => {
    const run = {
      id: "run-complete",
      status: "completed",
      created_at: "2026-10-05T14:00:00.000Z",
      started_at: "2026-10-05T14:00:01.000Z",
      finished_at: "2026-10-05T14:05:00.000Z",
      final_message: JSON.stringify({
        completedStepIds: ["parser-coverage"],
        evidenceRefs: ["tests/parser.test.ts"],
        knownRisks: [],
        summary: "Parser coverage is implemented and verified.",
        validationCommands: ["pnpm test"],
      }),
      events: [],
    };

    expect(latestWorkerRunActivity(run as never)).toEqual({
      summary: "Parser coverage is implemented and verified.",
      at: "2026-10-05T14:05:00.000Z",
    });
  });

  it("keeps unchanged polling data stable and preserves a detached reader position", () => {
    const thread = {
      id: "thread-1", cwd: "/tmp", preview: "", name: null, createdAt: 1, updatedAt: 1,
      modelProvider: "openai", source: null, status: "completed",
      turns: [{ id: "turn-1", status: "completed", items: [{ id: "item-1", type: "agentMessage", text: "Done" }] }],
      goalId: "goal-1",
    };
    expect(sameThreadTurns(thread, { ...thread, updatedAt: 2 })).toBe(true);
    expect(sameThreadTurns(thread, { ...thread, turns: [...thread.turns, { id: "turn-2", status: "running", items: [] }] })).toBe(false);
    expect(preservedChatScrollTop(420, 1200, 600)).toBe(420);
    expect(preservedChatScrollTop(900, 1200, 600)).toBe(600);
    expect(unreadResponseDelta(2, 4)).toBe(2);
    expect(unreadResponseDelta(4, 3)).toBe(0);
  });
});

describe("conversation scrolling", () => {
  it("shows unread responses on the explicit latest-message control", () => {
    const onClick = vi.fn();
    render(<ScrollToLatestButton unreadCount={3} onClick={onClick} />);

    const button = screen.getByRole("button", { name: "Scroll to latest message, 3 new responses" });
    expect(screen.getByText("3")).toBeTruthy();
    fireEvent.click(button);
    expect(onClick).toHaveBeenCalledOnce();
  });
});

describe("worker dashboard", () => {
  it("does not keep the current pool unhealthy for historical replaced workers", () => {
    expect(workerNeedsCurrentAttention({ state: "failed", activeClaims: [] })).toBe(false);
    expect(workerNeedsCurrentAttention({ state: "failed", activeClaims: ["claim-still-owned"] })).toBe(true);
    expect(workerNeedsCurrentAttention({ state: "recovering", activeClaims: ["claim-recovering"] })).toBe(true);
  });

  it("prioritizes a live worker dock with expandable details and thread access", async () => {
    const onOpenThread = vi.fn();
    const worker: GoalWorkerCardData = {
      id: "worker-live-001",
      createdAt: now,
      updatedAt: now,
      state: "active",
      claimId: "claim-live-001",
      claimLabel: "Document understanding",
      packageLabel: null,
      runId: "run-live-001",
      threadId: "thread-live-001",
      branch: "golazo/worker-live-001",
      workspacePath: "/tmp/worktrees/worker-live-001",
      activity: "Reviewing parser boundaries",
      activityAt: now,
      validation: "unknown",
      lastHeartbeatAt: now,
      terminationReason: null,
      failureCode: null,
      failureMessage: null,
      recoveryGuidance: null,
    };

    render(<GoalWorkerDock summary={summary({ workers: [worker] })} loading={false} error={null} onOpenThread={onOpenThread} />);

    expect(screen.getByRole("region", { name: "Live worker pool" })).toBeTruthy();
    expect(screen.getByText("1 working · no action needed")).toBeTruthy();
    await waitFor(() => expect(screen.getAllByText("Reviewing parser boundaries")).toHaveLength(2));
    fireEvent.click(screen.getByRole("button", { name: "Open worker thread" }));
    expect(onOpenThread).toHaveBeenCalledWith("thread-live-001");
  });

  it("compresses completed worker activity when the pool is idle", () => {
    const worker: GoalWorkerCardData = {
      id: "worker-done-001",
      createdAt: now,
      updatedAt: now,
      state: "completed",
      claimId: null,
      claimLabel: "Job lifecycle",
      packageLabel: null,
      runId: "run-done-001",
      threadId: "thread-done-001",
      branch: "golazo/worker-done-001",
      workspacePath: "/tmp/worktrees/worker-done-001",
      activity: "Integrated and verified",
      activityAt: now,
      validation: "passed",
      lastHeartbeatAt: now,
      terminationReason: null,
      failureCode: null,
      failureMessage: null,
      recoveryGuidance: null,
    };

    const view = render(<GoalWorkerDock summary={summary({ mode: "stopped", activeWorkers: 0, readyWork: 0, workers: [worker] })} loading={false} error={null} />);
    const dock = view.container.querySelector("details.worker-pool-dock")! as HTMLDetailsElement;
    expect(dock.open).toBe(false);
    expect(screen.getByText("Last worker run")).toBeTruthy();
    expect(screen.getByText(/1 completed/)).toBeTruthy();
    fireEvent.click(dock.querySelector(":scope > summary")!);
    expect(screen.getAllByText("Integrated and verified")).toHaveLength(2);
  });

  it("labels interrupted workers as recovery work instead of live activity", async () => {
    const worker: GoalWorkerCardData = {
      id: "worker-recovering-001",
      createdAt: now,
      updatedAt: now,
      state: "recovering",
      claimId: "claim-recovering-001",
      claimLabel: "Semantic review safety",
      packageLabel: null,
      runId: "run-interrupted-001",
      threadId: "thread-interrupted-001",
      branch: "golazo/worker-recovering-001",
      workspacePath: "/tmp/worktrees/worker-recovering-001",
      activity: "Runtime interrupted; Golazo is preserving the workspace and reassigning this claim",
      activityAt: now,
      validation: "unknown",
      lastHeartbeatAt: now,
      terminationReason: null,
      failureCode: null,
      failureMessage: null,
      recoveryGuidance: null,
    };

    render(<GoalWorkerDock summary={summary({ activeWorkers: 1, workers: [worker] })} loading={false} error={null} />);

    expect(screen.getByRole("region", { name: "Worker recovery required" })).toBeTruthy();
    expect(screen.getByText("No worker process is currently running. Golazo is preserving any partial work and assigning replacement workers.")).toBeTruthy();
    await waitFor(() => expect(screen.getAllByText(/Runtime interrupted/)).toHaveLength(2));
    expect(screen.queryByRole("region", { name: "Live worker pool" })).toBeNull();
  });

  it("does not label a terminal failed pool as live", () => {
    const worker: GoalWorkerCardData = {
      id: "worker-failed-001",
      createdAt: now,
      updatedAt: now,
      state: "failed",
      claimId: null,
      claimLabel: "Document understanding",
      packageLabel: null,
      runId: "run-failed-001",
      threadId: "thread-failed-001",
      branch: "golazo/worker-failed-001",
      workspacePath: "/tmp/worktrees/worker-failed-001",
      activity: "Codex worker failed (usageLimitExceeded): Usage limit reached",
      activityAt: now,
      validation: "unknown",
      lastHeartbeatAt: now,
      terminationReason: "Usage limit reached",
      failureCode: "usageLimitExceeded",
      failureMessage: "Usage limit reached",
      recoveryGuidance: "Retry after the account limit resets.",
    };

    render(<GoalWorkerDock summary={summary({ activeWorkers: 0, workers: [worker] })} loading={false} error={null} />);

    expect(screen.getByRole("region", { name: "Worker pool needs attention" })).toBeTruthy();
    expect(screen.getByText("No worker process is currently running. Review the worker failures before retrying ready work.")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Live worker pool" })).toBeNull();
  });

  it("keeps autonomous controls collapsed behind a compact status summary", async () => {
    const onToggle = vi.fn();
    const view = render(
      <AutonomousExecutionDisclosure
        summary={summary({ blockedWork: 1, failedIntegrations: 1 })}
        loading={false}
        error={null}
        open={false}
        onToggle={onToggle}
      >
        <button type="button">Advanced worker action</button>
      </AutonomousExecutionDisclosure>,
    );

    const disclosure = view.container.querySelector("details")!;
    expect(disclosure.open).toBe(false);
    expect(screen.getByText("Optional multi-worker coordination")).toBeTruthy();
    expect(screen.getByText("2 need attention")).toBeTruthy();
    fireEvent.click(view.container.querySelector("summary")!);
    await waitFor(() => expect(onToggle).toHaveBeenCalledWith(true));
  });

  it("keeps claims, integrations, and coordination history behind one audit disclosure", () => {
    const view = render(
      <AutonomousDetailsDisclosure claims={4} integrations={11} events={96} attentionCount={0}>
        <p>Detailed audit records</p>
      </AutonomousDetailsDisclosure>,
    );

    const disclosure = view.container.querySelector("details.autonomous-details") as HTMLDetailsElement;
    expect(disclosure.open).toBe(false);
    expect(screen.getByText("Details & history")).toBeTruthy();
    expect(screen.getByText("4 claims · 11 integrations · 96 events")).toBeTruthy();
  });

  it("surfaces hidden history that needs attention without expanding it", () => {
    const view = render(
      <AutonomousDetailsDisclosure claims={0} integrations={3} events={12} attentionCount={2}>
        <p>Detailed audit records</p>
      </AutonomousDetailsDisclosure>,
    );

    const disclosure = view.container.querySelector("details.autonomous-details") as HTMLDetailsElement;
    expect(disclosure.open).toBe(false);
    expect(disclosure.classList.contains("has-attention")).toBe(true);
    expect(screen.getByText("2 need attention")).toBeTruthy();
  });

  it("shows normal and stalled worker state with operational context", () => {
    const workers: GoalWorkerCardData[] = [
      {
        id: "worker-active-001",
        state: "active",
        claimId: "claim-normal-001",
        claimLabel: "Normal delivery",
        packageLabel: "Delivery package",
        runId: "run-001",
        threadId: "thread-001",
        branch: "golazo/worker-active-001",
        workspacePath: "/tmp/worktrees/worker-active-001",
        activity: "Implementation is progressing",
        activityAt: now,
        validation: "passed",
        lastHeartbeatAt: now,
        terminationReason: null,
        failureCode: null,
        failureMessage: null,
        recoveryGuidance: null,
      },
      {
        id: "worker-stalled-002",
        state: "recovering",
        claimId: "claim-stalled-002",
        claimLabel: "Stalled delivery",
        packageLabel: null,
        runId: null,
        threadId: "thread-002",
        branch: "golazo/worker-stalled-002",
        workspacePath: "/tmp/worktrees/worker-stalled-002",
        activity: "Heartbeat lost; workspace quarantined",
        activityAt: now,
        validation: "failed",
        lastHeartbeatAt: "2026-10-05T14:30:00.000Z",
        terminationReason: "Worker heartbeat expired",
        failureCode: null,
        failureMessage: null,
        recoveryGuidance: null,
      },
    ];

    const { container } = render(<GoalWorkers workers={workers} loading={false} error={null} />);
    expect(screen.getByText("Normal delivery")).toBeTruthy();
    expect(screen.getByText("Stalled delivery")).toBeTruthy();
    expect(screen.getByText("Validation passed")).toBeTruthy();
    expect(screen.getByText("Validation failed")).toBeTruthy();

    const cards = container.querySelectorAll("summary");
    fireEvent.click(cards[1]);
    expect(screen.getByText("Heartbeat lost; workspace quarantined")).toBeTruthy();
    expect(screen.getByText("Worker heartbeat expired")).toBeTruthy();
  });

  it("shows the concrete Codex failure and preserved-work guidance", () => {
    const workers: GoalWorkerCardData[] = [{
      id: "worker-limited-003",
      state: "failed",
      claimId: null,
      claimLabel: "No active claim",
      packageLabel: null,
      runId: "run-limited-003",
      threadId: "thread-limited-003",
      branch: "golazo/worker-limited-003",
      workspacePath: "/tmp/worktrees/worker-limited-003",
      activity: "Run stopped",
      activityAt: now,
      validation: "unknown",
      lastHeartbeatAt: now,
      terminationReason: "Permanent: worker failed",
      failureCode: "usageLimitExceeded",
      failureMessage: "You have reached your Codex usage limit.",
      recoveryGuidance: "The isolated workspace and its uncommitted changes are preserved. Wait for Codex usage to reset before recovering this work.",
    }];

    const { container } = render(<GoalWorkers workers={workers} loading={false} error={null} />);
    fireEvent.click(container.querySelector("summary")!);

    expect(screen.getByText("Usage Limit Exceeded")).toBeTruthy();
    expect(screen.getByText("You have reached your Codex usage limit.")).toBeTruthy();
    expect(screen.getByText(/uncommitted changes are preserved/)).toBeTruthy();
  });

  it("collapses a superseded terminal worker into audit history", () => {
    const older: GoalWorkerCardData = {
      id: "worker-old",
      createdAt: "2026-10-05T10:00:00.000Z",
      updatedAt: "2026-10-05T10:30:00.000Z",
      state: "failed",
      claimId: "claim-old",
      claimLabel: "Document understanding",
      packageLabel: null,
      runId: "run-old",
      threadId: "thread-old",
      branch: "golazo/worker-old",
      workspacePath: "/tmp/worktrees/worker-old",
      activity: "Startup recovery found no active claim or run",
      activityAt: "2026-10-05T10:30:00.000Z",
      validation: "unknown",
      lastHeartbeatAt: "2026-10-05T10:20:00.000Z",
      terminationReason: "startup recovery found no active claim or Codex run to recover",
      failureCode: null,
      failureMessage: null,
      recoveryGuidance: null,
    };
    const newer: GoalWorkerCardData = {
      ...older,
      id: "worker-new",
      createdAt: "2026-10-05T11:00:00.000Z",
      updatedAt: "2026-10-05T11:05:00.000Z",
      state: "active",
      claimId: "claim-new",
      runId: "run-new",
      threadId: "thread-new",
      activity: "Parsing the next document fixture",
      terminationReason: null,
    };

    const { container } = render(<GoalWorkers workers={[newer, older]} loading={false} error={null} />);
    const history = container.querySelector("details.worker-history") as HTMLDetailsElement;
    expect(history).toBeTruthy();
    expect(history.open).toBe(false);
    expect(screen.getByText("1 superseded")).toBeTruthy();
    expect(container.querySelectorAll(".worker-list > .worker-card")).toHaveLength(1);
    expect(history.querySelectorAll(":scope > div > .worker-card")).toHaveLength(1);
  });

  it("renders managed overlap evidence inside a claimed feature", () => {
    const claim = resource("claim-overlap-001", {
      owner: "worker-active-001",
      state: "active",
      scope: { kind: "feature" as const, feature_id: "sync-engine" },
      baseRevision: "abc123",
      leaseGeneration: 3,
      heartbeatAt: now,
      leaseExpiresAt: "2026-10-05T15:10:00.000Z",
      overlaps: [{
        otherClaimId: "claim-overlap-002",
        kind: "reconcilable",
        paths: ["src/schema.ts", "src/client.ts"],
        rationale: "Both slices update the shared API shape",
        recordedAt: now,
      }],
      producesContracts: [],
      consumesContracts: [],
    });
    const goal: Goal = {
      goal_id: "dashboard",
      title: "Dashboard",
      progress: { completed_steps: 1, total_steps: 2, completion_rate: 50 },
      slices: [],
      features: [{
        id: "sync-engine",
        title: "Sync engine",
        description: "",
        status: "Partial",
        progress: { completed_steps: 1, total_steps: 2, completion_rate: 50 },
        slice_count: 1,
        steps: [
          { id: "one", title: "Capture changes", done: true },
          { id: "two", title: "Reconcile updates", done: false },
        ],
      }],
    };

    const { container } = render(
      <GoalClaimPackageDetails goal={goal} summary={summary({ claims: [claim] })} loading={false} />,
    );
    fireEvent.click(container.querySelector("summary")!);
    expect(screen.getByText("Both slices update the shared API shape")).toBeTruthy();
    expect(screen.getByText("2 affected paths")).toBeTruthy();
    expect(screen.getByText("Reconcile updates")).toBeTruthy();
  });

  it("shows the durable user decision that resolved an escalation", () => {
    const escalation: GoalEscalationData = {
      resource: resource("escalation-001", {
        kind: "decision_point" as const,
        severity: "warning",
        state: "resolved",
        scope: { kind: "claim" as const, claim_id: "claim-normal-001" },
        summary: "Choose the compatible contract revision",
        evidenceRefs: ["validation:contract-revision"],
        options: [{
          id: "use-v2",
          action: "resume",
          label: "Use contract v2",
          description: "Refresh the worker against revision two.",
          consequences: ["The worker must revalidate its generated client."],
          recommended: true,
        }],
        staleAfterRevisions: [],
        stalenessSnapshot: null,
        decision: {
          optionId: "use-v2",
          decidedBy: "Alex",
          acceptedRisk: "One client regeneration is required",
          decidedAt: now,
        },
      }),
      staleReasons: [],
    };

    const { container } = render(<GoalEscalationInbox escalations={[escalation]} loading={false} />);
    fireEvent.click(container.querySelector("summary")!);
    expect(screen.getByText("Recorded decision")).toBeTruthy();
    expect(screen.getAllByText("Use contract v2")).toHaveLength(2);
    expect(screen.getByText(/Decided by Alex/)).toBeTruthy();
    expect(screen.getByText(/One client regeneration is required/)).toBeTruthy();
  });

  it("surfaces integration failure, conflict, and preserved recovery evidence", () => {
    const integration: GoalIntegrationArtifactData = {
      resource: resource("artifact-001", {
        claimId: "claim-normal-001",
        claimGeneration: 2,
        workerId: "worker-active-001",
        repositoryId: "repo-main",
        baseRevision: "abc123",
        headRevision: "def456",
        commits: [{ revision: "def456", subject: "Update API" }],
        diffSummary: { filesChanged: 2, insertions: 12, deletions: 4, changedPaths: ["src/api.ts"], summary: "API update" },
        changedContracts: [],
        migrations: [],
        validations: [],
        evidenceRefs: ["artifact:diff"],
        knownRisks: ["Generated client is stale"],
        workspace: {
          branch: "golazo/worker-active-001",
          stagedPaths: [],
          unstagedPaths: [],
          untrackedPaths: [],
          conflictedPaths: ["src/api.ts"],
        },
      }),
      validations: [resource("validation-001", {
        passed: false,
        results: [{
          gateId: "tests",
          kind: "test",
          command: "pnpm test",
          required: true,
          succeeded: false,
          exitCode: 1,
          stdoutExcerpt: "",
          stderrExcerpt: "contract mismatch",
        }],
      })],
      reconciliations: [resource("reconciliation-001", {
        strategy: "manual",
        state: "manual_required",
        targetRevision: "fed789",
        previousHead: "def456",
        resultingHead: "def456",
        backupRef: "refs/golazo/backups/artifact-001",
        command: "git rebase fed789",
        manualInstructions: "Resolve the API contract conflict",
        workspacePreserved: true,
      })],
      maintenance: [resource("maintenance-001", {
        kind: "rollback",
        state: "completed",
        requestedBy: "Alex",
        reason: "Required validation failed",
        previousRepositoryHead: "fed789",
        resultingRepositoryHead: "abc123",
        backupRef: "refs/golazo/backups/artifact-001",
        retainedBranch: "golazo/worker-active-001",
        cleanupRecordPath: null,
        error: null,
        artifactPreserved: true,
      })],
      jobs: [resource("job-001", {
        artifactId: "artifact-001",
        state: "failed",
        repositoryId: "repo-main",
        priority: 10,
        attempt: 1,
        enqueuedAt: now,
        completedAt: now,
        outcome: { summary: "Validation gate failed", at: now },
      })],
      queuePosition: null,
    };

    const delivery = resource("dashboard", {
      goalId: "dashboard",
      integrationBranch: "codex/goal-dashboard",
      targetBranch: "develop",
      remote: "origin",
      mergePolicy: "manual" as const,
      status: "pull_request_open" as const,
      integrationWorktree: "/tmp/goal-dashboard",
      headRevision: "delivery-head",
      pushedRevision: "delivery-head",
      pullRequestNumber: 42,
      pullRequestUrl: "https://github.com/example/repo/pull/42",
      lastError: null,
      updatedAt: now,
    });
    const { container } = render(<GoalIntegrationView goalId="dashboard" integrations={[integration]} delivery={delivery} loading={false} />);
    expect(screen.getByText("codex/goal-dashboard")).toBeTruthy();
    expect(screen.getByText("PR #42")).toBeTruthy();
    fireEvent.click(container.querySelector("summary")!);
    expect(screen.getByText("Validation gate failed")).toBeTruthy();
    expect(screen.getByText("src/api.ts")).toBeTruthy();
    expect(screen.getByText("Generated client is stale")).toBeTruthy();
    expect(screen.getByText("Resolve the API contract conflict")).toBeTruthy();
    expect(screen.getByText(/Artifact preserved/)).toBeTruthy();
  });

  it("retains the last snapshot during an outage and clears the warning after restart", () => {
    const onRetry = vi.fn();
    const current = summary({ desiredConcurrency: 3, activeWorkers: 2 });
    const view = render(
      <GoalPoolSummaryCard summary={current} loading={false} error="Failed to fetch" onRetry={onRetry} />,
    );
    expect(screen.getByRole("alert").textContent).toContain("Showing the last successful snapshot");
    expect(screen.getByText("2 / 3")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledTimes(1);

    view.rerender(
      <GoalPoolSummaryCard summary={{ ...current, refreshedAt: new Date().toISOString() }} loading={false} error={null} onRetry={onRetry} />,
    );
    expect(screen.queryByText("Live updates interrupted")).toBeNull();
    expect(screen.getByText("Healthy")).toBeTruthy();
  });

  it("loads normal worker state through the coordination API projection", async () => {
    const worker = resource("worker-api-001", {
      state: "active",
      activeClaims: ["claim-api-001"],
      currentRunId: "run-api-001",
      currentThreadId: "thread-api-001",
      workspace: null,
      lastHeartbeatAt: now,
      termination: null,
    });
    const claim = resource("claim-api-001", {
      owner: "worker-api-001",
      state: "active",
      scope: { kind: "feature", feature_id: "api-feature" },
      baseRevision: "abc123",
      leaseGeneration: 1,
      heartbeatAt: now,
      leaseExpiresAt: "2026-10-05T15:10:00.000Z",
      overlaps: [],
      producesContracts: [],
      consumesContracts: [],
    });
    const pool = resource("dashboard", {
      goal: { goalId: "dashboard", desiredConcurrency: 1, mode: "running" },
      workers: [worker.data],
      activeWorkers: 1,
      availableGlobalSlots: 3,
    }, "11");
    const collections = new Map<string, unknown[]>([
      ["/ready-work", []],
      ["/claims", [claim]],
      ["/packages", []],
      ["/workers", [worker]],
      ["/integration/artifacts", []],
      ["/contracts", []],
      ["/events", []],
      ["/interventions", []],
      ["/notifications", []],
      ["/escalations", []],
      ["/activity", []],
    ]);
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      const body = url.includes("/health")
        ? resource("dashboard", { status: "healthy", operatingMode: "normal", components: [] })
        : url.includes("/supervisor")
        ? resource("dashboard", {
            goalId: "dashboard",
            state: "idle",
            poolMode: "running",
            activeRunId: null,
            activeThreadId: null,
            currentTriggerKey: null,
            acknowledgedEventSequence: 12,
            lastDeliveredEventSequence: 12,
            evaluationCount: 2,
            consumedTokens: 1450,
            circuitOpenUntil: null,
            pendingTriggerCount: 0,
            lastInterventionId: "intervention-1",
            lastInterventionLevel: "recommend",
            lastInterventionState: "applied",
            lastInterventionSummary: "Keep both workers on their current claims.",
            lastInterventionAt: now,
            lastFailure: null,
          })
        : url.includes("/pool")
        ? pool
        : { apiVersion: "v1", items: [...collections.entries()].find(([suffix]) => url.includes(suffix))?.[1] || [], nextCursor: null };
      return { ok: true, json: async () => body } as Response;
    }));

    const loaded = await loadGoalPoolSummary("dashboard");
    expect(loaded.poolResourceVersion).toBe("11");
    expect(loaded.healthLabel).toBe("Healthy");
    expect(loaded.supervisor?.data).toMatchObject({ state: "idle", evaluationCount: 2, consumedTokens: 1450 });
    expect(loaded.workers).toHaveLength(1);
    expect(loaded.workers[0]).toMatchObject({
      id: "worker-api-001",
      claimId: "claim-api-001",
      claimLabel: "api-feature",
      runId: "run-api-001",
      threadId: "thread-api-001",
    });
  });

  it("shows live Supervisor state without exposing the full control surface", () => {
    const supervisor = resource("dashboard", {
      goalId: "dashboard",
      state: "evaluating" as const,
      poolMode: "running",
      activeRunId: "run-supervisor-1",
      activeThreadId: "thread-supervisor-1",
      currentTriggerKey: "worker_stalled:worker-1",
      acknowledgedEventSequence: 18,
      lastDeliveredEventSequence: 20,
      evaluationCount: 3,
      consumedTokens: 2200,
      circuitOpenUntil: null,
      pendingTriggerCount: 2,
      lastInterventionId: "intervention-1",
      lastInterventionLevel: "coordinate",
      lastInterventionState: "applied",
      lastInterventionSummary: "Reassign the abandoned claim after preserving its workspace.",
      lastInterventionAt: now,
      lastFailure: null,
    });
    render(<SupervisorRuntimeCard resource={supervisor} loading={false} />);
    expect(screen.getByText("Supervisor")).toBeTruthy();
    expect(screen.getByText("Evaluating")).toBeTruthy();
    expect(screen.getByText("worker_stalled:worker-1")).toBeTruthy();
    expect(screen.getByText("Reassign the abandoned claim after preserving its workspace.")).toBeTruthy();
    expect(screen.getByText("2,200")).toBeTruthy();
  });

  it("presents only the normal worker count and valid primary action by default", () => {
    const { container } = render(
      <GoalPoolControls
        goalId="dashboard"
        summary={summary({ mode: "stopped", activeWorkers: 0 })}
        loading={false}
        actor="Alex"
        permissionMode="ask"
        onChanged={vi.fn()}
      />,
    );

    expect(screen.getByRole("heading", { name: "Run workers" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Start workers" })).toBeTruthy();
    expect(container.querySelectorAll(".worker-primary-actions button")).toHaveLength(1);
    expect((container.querySelector(".worker-settings-disclosure") as HTMLDetailsElement).open).toBe(false);
    expect((container.querySelector(".advanced-worker-controls") as HTMLDetailsElement).open).toBe(false);
    expect(screen.getByText("Workspace-only changes · local commands and Git · no network · approval when needed")).toBeTruthy();
  });

  it("sends explicit user and goal permission policy when starting workers", async () => {
    vi.spyOn(window, "confirm").mockReturnValue(true);
    const fetchMock = vi.fn(async (_input: string | URL | Request, _init?: RequestInit) => ({
      ok: true,
      json: async () => resource("dashboard", {}),
    }) as Response);
    vi.stubGlobal("fetch", fetchMock);
    const onChanged = vi.fn();
    render(
      <GoalPoolControls
        goalId="dashboard"
        summary={summary({ mode: "stopped" })}
        loading={false}
        actor="Alex"
        permissionMode="full-access"
        onChanged={onChanged}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Start workers" }));
    await waitFor(() => expect(onChanged).toHaveBeenCalledTimes(1));
    const request = fetchMock.mock.calls[0][1]!;
    const payload = JSON.parse(String(request.body));
    expect(payload.command.userPolicy).toMatchObject({
      sandbox: "danger-full-access",
      approvalPolicy: "never",
      networkAccess: true,
    });
    expect(payload.command.permissionPolicy).toMatchObject({
      sandbox: "workspace-write",
      approvalPolicy: "on-request",
      networkAccess: false,
      toolCapabilities: ["read_files", "write_files", "run_commands", "git"],
    });
    expect(payload.command.resourcePolicy).toMatchObject({
      maxWorkerTokens: 250_000,
      maxGoalTokens: 1_000_000,
      maxWorkerTurnSeconds: 1_800,
      maxGoalElapsedSeconds: 28_800,
      maxWorkerNetworkRequests: 0,
      maxGoalNetworkRequests: 0,
      maxGoalConcurrency: 4,
    });
    expect(payload.confirmation).toMatchObject({
      action: "pool.start_policy",
      target: "dashboard",
      confirmedBy: "Alex",
    });
  });
});
