// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AutonomousExecutionDisclosure,
  GoalClaimPackageDetails,
  GoalEscalationInbox,
  GoalIntegrationView,
  GoalPoolControls,
  GoalPoolSummaryCard,
  GoalWorkers,
  api,
  loadGoalPoolSummary,
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
    readyUnits: [],
    workers: [],
    claims: [],
    packages: [],
    contracts: [],
    integrations: [],
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

describe("worker dashboard", () => {
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

    const { container } = render(<GoalIntegrationView goalId="dashboard" integrations={[integration]} loading={false} />);
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
        : url.includes("/pool")
        ? pool
        : { apiVersion: "v1", items: [...collections.entries()].find(([suffix]) => url.includes(suffix))?.[1] || [], nextCursor: null };
      return { ok: true, json: async () => body } as Response;
    }));

    const loaded = await loadGoalPoolSummary("dashboard");
    expect(loaded.poolResourceVersion).toBe("11");
    expect(loaded.healthLabel).toBe("Healthy");
    expect(loaded.workers).toHaveLength(1);
    expect(loaded.workers[0]).toMatchObject({
      id: "worker-api-001",
      claimId: "claim-api-001",
      claimLabel: "api-feature",
      runId: "run-api-001",
      threadId: "thread-api-001",
    });
  });

  it("sends explicit user and goal permission policy when configuring workers", async () => {
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

    fireEvent.click(screen.getByRole("button", { name: "Configure" }));
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
      action: "pool.configure_policy",
      target: "dashboard",
      confirmedBy: "Alex",
    });
  });
});
