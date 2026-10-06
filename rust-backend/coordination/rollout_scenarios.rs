use super::claims::{ClaimPolicy, ClaimService};
use super::contracts::{ContractRegistration, ContractRegistry};
use super::detectors::{
    ContractChangeDetector, FileOverlapDetector, HealthDetectionPolicy, MergeConflictPredictor,
    MigrationOverlapDetector, SymbolOverlapDetector, WorkerHealthDetector,
};
use super::domain::{
    Claim, ClaimScope, ClaimState, ContractExpectation, ContractKind, CoordinationActor,
    CoordinationEventKind, IntegrationJobState, OverlapKind, ValidationGateKind, WorkPackageId,
    Worker, WorkerState,
};
use super::integration::{
    CaptureIntegrationArtifactRequest, IntegrationArtifactService, IntegrationFinalizationRequest,
    IntegrationFinalizationService, IntegrationQueueService, ValidationGateRunner,
    ValidationGateSpec,
};
use super::pool::{RecoveryEvidence, WorkerFailureKind, WorkerPoolPolicy, WorkerPoolService};
use super::store::{
    ClaimRepository, EventRepository, IntegrationJobRepository, SqliteCoordinationStore,
    WorkerRepository,
};
use super::supervisor_evals::supervisor_eval_cases;
use super::workspace::WorktreeManager;
use crate::models::{Feature, IntegrationScope, Status, Step};
use crate::tracker::Tracker;
use chrono::{Duration, Utc};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

#[test]
fn two_peer_workers_integrate_independent_packages_end_to_end() {
    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repository");
    fs::create_dir(&repository).unwrap();
    git(&repository, &["init", "-q"]);
    git(
        &repository,
        &["config", "user.email", "rollout@example.test"],
    );
    git(&repository, &["config", "user.name", "Rollout Test"]);
    fs::write(repository.join("alpha.txt"), "alpha: initial\n").unwrap();
    fs::write(repository.join("beta.txt"), "beta: initial\n").unwrap();
    git(&repository, &["add", "alpha.txt", "beta.txt"]);
    git(&repository, &["commit", "-q", "-m", "initial"]);
    let base_revision = git_output(&repository, &["rev-parse", "HEAD"]);

    let tracker = Tracker::new(directory.path().join("goals"));
    tracker
        .create_goal_with_features(
            "goal-two-worker",
            "Two worker rollout",
            "independent work packages",
            vec![
                feature("alpha", "Integrate alpha"),
                feature("beta", "Integrate beta"),
            ],
        )
        .unwrap();
    tracker
        .add_work_package(
            "goal-two-worker",
            "package-alpha",
            "Alpha package",
            "Owns alpha.txt",
            vec!["alpha".into()],
            vec![],
            10,
            IntegrationScope::WorkPackage,
        )
        .unwrap();
    tracker
        .add_work_package(
            "goal-two-worker",
            "package-beta",
            "Beta package",
            "Owns beta.txt",
            vec!["beta".into()],
            vec![],
            10,
            IntegrationScope::WorkPackage,
        )
        .unwrap();

    let store = Arc::new(
        SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
    );
    let manager = WorktreeManager::open(&repository).unwrap();
    let now = Utc::now();
    let mut alpha_worker = active_worker("goal-two-worker", now);
    alpha_worker.workspace = Some(
        manager
            .create_for_worker("goal-two-worker", &alpha_worker.id, &base_revision, now)
            .unwrap(),
    );
    let mut beta_worker = active_worker("goal-two-worker", now);
    beta_worker.workspace = Some(
        manager
            .create_for_worker("goal-two-worker", &beta_worker.id, &base_revision, now)
            .unwrap(),
    );
    store.upsert_worker(&alpha_worker).unwrap();
    store.upsert_worker(&beta_worker).unwrap();

    let claims = ClaimService::new(tracker.clone(), Arc::clone(&store), ClaimPolicy::default());
    let alpha_claim = claims
        .claim_ready_unit(
            "goal-two-worker",
            &alpha_worker.id,
            Some(ClaimScope::WorkPackage {
                work_package_id: WorkPackageId::parse("package-alpha").unwrap(),
            }),
            &base_revision,
            "two-worker-alpha-claim",
            now,
        )
        .unwrap();
    let beta_claim = claims
        .claim_ready_unit(
            "goal-two-worker",
            &beta_worker.id,
            Some(ClaimScope::WorkPackage {
                work_package_id: WorkPackageId::parse("package-beta").unwrap(),
            }),
            &base_revision,
            "two-worker-beta-claim",
            now,
        )
        .unwrap();

    let alpha_worktree = Path::new(&alpha_worker.workspace.as_ref().unwrap().worktree_path);
    fs::write(alpha_worktree.join("alpha.txt"), "alpha: implemented\n").unwrap();
    git(alpha_worktree, &["add", "alpha.txt"]);
    git(alpha_worktree, &["commit", "-q", "-m", "implement alpha"]);
    let beta_worktree = Path::new(&beta_worker.workspace.as_ref().unwrap().worktree_path);
    fs::write(beta_worktree.join("beta.txt"), "beta: implemented\n").unwrap();
    git(beta_worktree, &["add", "beta.txt"]);
    git(beta_worktree, &["commit", "-q", "-m", "implement beta"]);

    let artifacts = IntegrationArtifactService::new(Arc::clone(&store));
    let alpha_artifact = artifacts
        .capture(
            CaptureIntegrationArtifactRequest {
                claim_id: alpha_claim.id.clone(),
                worker_id: alpha_worker.id.clone(),
                changed_contracts: vec![],
                migrations: vec![],
                validations: vec![],
                evidence_refs: vec!["test:alpha-worker".into()],
                known_risks: vec![],
            },
            now + Duration::seconds(1),
        )
        .unwrap();
    let beta_artifact = artifacts
        .capture(
            CaptureIntegrationArtifactRequest {
                claim_id: beta_claim.id.clone(),
                worker_id: beta_worker.id.clone(),
                changed_contracts: vec![],
                migrations: vec![],
                validations: vec![],
                evidence_refs: vec!["test:beta-worker".into()],
                known_risks: vec![],
            },
            now + Duration::seconds(1),
        )
        .unwrap();
    assert_eq!(alpha_artifact.diff_summary.changed_paths, vec!["alpha.txt"]);
    assert_eq!(beta_artifact.diff_summary.changed_paths, vec!["beta.txt"]);

    let validation = ValidationGateRunner::new(Arc::clone(&store));
    let alpha_report = validation
        .run(
            &alpha_artifact.id,
            &[file_validation_gate(
                "alpha-gate",
                "alpha.txt",
                "alpha: implemented",
            )],
            now + Duration::seconds(2),
        )
        .unwrap();
    let beta_report = validation
        .run(
            &beta_artifact.id,
            &[file_validation_gate(
                "beta-gate",
                "beta.txt",
                "beta: implemented",
            )],
            now + Duration::seconds(2),
        )
        .unwrap();
    assert!(alpha_report.passed && beta_report.passed);

    let queue = IntegrationQueueService::new(Arc::clone(&store));
    let alpha_job = queue
        .enqueue(&alpha_artifact.id, 10, now + Duration::seconds(3))
        .unwrap();
    let beta_job = queue
        .enqueue(&beta_artifact.id, 0, now + Duration::seconds(3))
        .unwrap();
    let first = queue
        .acquire_next(&alpha_artifact.repository_id, now + Duration::seconds(4))
        .unwrap()
        .unwrap();
    assert_eq!(first.id, alpha_job.id);
    assert!(
        queue
            .acquire_next(&alpha_artifact.repository_id, now + Duration::seconds(4))
            .unwrap()
            .is_none(),
        "the shared repository integration lane must remain serialized"
    );

    git(&repository, &["cherry-pick", &alpha_artifact.head_revision]);
    let alpha_integration_revision = git_output(&repository, &["rev-parse", "HEAD"]);
    let finalizer = IntegrationFinalizationService::new(Arc::clone(&store), tracker.clone());
    finalizer
        .finalize(
            IntegrationFinalizationRequest {
                job_id: first.id,
                validation_report_id: alpha_report.id,
                tracker_feature_id: "alpha".into(),
                tracker_step_ids: vec!["integrate".into()],
                tracker_summary: "Alpha package validated and integrated".into(),
                tracker_evidence: vec!["validation:alpha-gate".into()],
                tracker_status: Status::Done,
                integration_revision: alpha_integration_revision,
            },
            now + Duration::seconds(5),
        )
        .unwrap();

    let second = queue
        .acquire_next(&beta_artifact.repository_id, now + Duration::seconds(6))
        .unwrap()
        .unwrap();
    assert_eq!(second.id, beta_job.id);
    git(&repository, &["cherry-pick", &beta_artifact.head_revision]);
    let beta_integration_revision = git_output(&repository, &["rev-parse", "HEAD"]);
    finalizer
        .finalize(
            IntegrationFinalizationRequest {
                job_id: second.id,
                validation_report_id: beta_report.id,
                tracker_feature_id: "beta".into(),
                tracker_step_ids: vec!["integrate".into()],
                tracker_summary: "Beta package validated and integrated".into(),
                tracker_evidence: vec!["validation:beta-gate".into()],
                tracker_status: Status::Done,
                integration_revision: beta_integration_revision,
            },
            now + Duration::seconds(7),
        )
        .unwrap();

    assert_eq!(
        fs::read_to_string(repository.join("alpha.txt")).unwrap(),
        "alpha: implemented\n"
    );
    assert_eq!(
        fs::read_to_string(repository.join("beta.txt")).unwrap(),
        "beta: implemented\n"
    );
    for claim in [&alpha_claim, &beta_claim] {
        assert_eq!(
            store.claim(&claim.id).unwrap().unwrap().state,
            ClaimState::Completed
        );
    }
    for worker in [&alpha_worker, &beta_worker] {
        assert_eq!(
            store.worker(&worker.id).unwrap().unwrap().state,
            WorkerState::Completed
        );
    }
    for job in [&alpha_job, &beta_job] {
        assert_eq!(
            store.integration_job(&job.id).unwrap().unwrap().state,
            IntegrationJobState::Succeeded
        );
    }
    let goal = tracker.get_goal("goal-two-worker").unwrap();
    assert!(goal["features"].as_array().unwrap().iter().all(
        |feature| feature["status"] == "Done" && feature["progress"]["completion_rate"] == 100
    ));
    assert_eq!(goal["slices"].as_array().unwrap().len(), 2);
    assert_eq!(
        store
            .events_for_goal("goal-two-worker", 0, None, 100)
            .unwrap()
            .iter()
            .filter(|event| event.kind == CoordinationEventKind::CompletionPublished)
            .count(),
        2
    );
}

#[test]
fn overlap_reconciliation_matrix_distinguishes_all_required_scenarios() {
    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repository");
    fs::create_dir_all(repository.join("src")).unwrap();
    git(&repository, &["init", "-q"]);
    git(
        &repository,
        &["config", "user.email", "overlap@example.test"],
    );
    git(&repository, &["config", "user.name", "Overlap Test"]);
    fs::write(
        repository.join("src/shared.rs"),
        "pub fn shared_value() -> &'static str { \"base\" }\n",
    )
    .unwrap();
    git(&repository, &["add", "."]);
    git(&repository, &["commit", "-q", "-m", "base"]);
    let base = git_output(&repository, &["rev-parse", "HEAD"]);

    let tracker = Tracker::new(directory.path().join("goals"));
    tracker
        .create_goal("goal-overlap", "Overlap reconciliation", "")
        .unwrap();
    for feature_id in ["left", "right"] {
        tracker
            .add_feature(
                "goal-overlap",
                feature_id,
                feature_id,
                "overlap fixture",
                Status::Planned,
            )
            .unwrap();
    }
    let store = Arc::new(
        SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
    );
    let manager = WorktreeManager::open(&repository).unwrap();
    let now = Utc::now();

    let mut left_worker = active_worker("goal-overlap", now);
    left_worker.workspace = Some(
        manager
            .create_for_worker("goal-overlap", &left_worker.id, &base, now)
            .unwrap(),
    );
    let mut right_worker = active_worker("goal-overlap", now);
    right_worker.workspace = Some(
        manager
            .create_for_worker("goal-overlap", &right_worker.id, &base, now)
            .unwrap(),
    );
    let mut left_claim = Claim::new(
        "goal-overlap",
        ClaimScope::Feature {
            feature_id: "left".into(),
        },
        left_worker.id.clone(),
        base.clone(),
        now,
        now + Duration::minutes(5),
    )
    .unwrap();
    let right_claim = Claim::new(
        "goal-overlap",
        ClaimScope::Feature {
            feature_id: "right".into(),
        },
        right_worker.id.clone(),
        base.clone(),
        now,
        now + Duration::minutes(5),
    )
    .unwrap();
    left_worker.active_claims.push(left_claim.id.clone());
    right_worker.active_claims.push(right_claim.id.clone());
    store.upsert_worker(&left_worker).unwrap();
    store.upsert_worker(&right_worker).unwrap();
    store.insert_claim(&left_claim).unwrap();
    store.insert_claim(&right_claim).unwrap();

    let left_worktree = Path::new(&left_worker.workspace.as_ref().unwrap().worktree_path);
    fs::write(
        left_worktree.join("src/shared.rs"),
        "pub fn shared_value() -> &'static str { \"left\" }\n",
    )
    .unwrap();
    fs::create_dir_all(left_worktree.join("migrations")).unwrap();
    fs::write(
        left_worktree.join("migrations/001_left.sql"),
        "create table left_items(id integer);\n",
    )
    .unwrap();
    git(left_worktree, &["add", "."]);
    git(left_worktree, &["commit", "-q", "-m", "left overlap"]);

    let right_worktree = Path::new(&right_worker.workspace.as_ref().unwrap().worktree_path);
    fs::write(
        right_worktree.join("src/shared.rs"),
        "pub fn shared_value() -> &'static str { \"right\" }\n",
    )
    .unwrap();
    fs::create_dir_all(right_worktree.join("migrations")).unwrap();
    fs::write(
        right_worktree.join("migrations/002_right.sql"),
        "create table right_items(id integer);\n",
    )
    .unwrap();
    git(right_worktree, &["add", "."]);
    git(right_worktree, &["commit", "-q", "-m", "right overlap"]);

    let claims = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
    for (index, kind) in [
        OverlapKind::Benign,
        OverlapKind::ReconciliationRequired,
        OverlapKind::SemanticConflict,
    ]
    .into_iter()
    .enumerate()
    {
        let (left, right) = claims
            .record_managed_overlap(
                &left_claim.id,
                &right_claim.id,
                kind,
                vec!["src/shared.rs".into()],
                "scenario matrix classification",
                now + Duration::seconds(index as i64 + 1),
            )
            .unwrap();
        assert_eq!(left.overlaps[0].kind, kind);
        assert_eq!(right.overlaps[0].kind, kind);
        assert_eq!(left.overlaps[0].other_claim_id, right.id);
        assert_eq!(right.overlaps[0].other_claim_id, left.id);
    }

    let file_overlap = FileOverlapDetector::new(Arc::clone(&store))
        .detect_and_publish(
            &left_worker.id,
            &left_claim.id,
            &right_worker.id,
            &right_claim.id,
            now + Duration::seconds(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        file_overlap.kind,
        CoordinationEventKind::FileOverlapDetected
    );
    let symbol_overlap = SymbolOverlapDetector::new(Arc::clone(&store))
        .detect_and_publish(
            &left_worker.id,
            &left_claim.id,
            &right_worker.id,
            &right_claim.id,
            now + Duration::seconds(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        symbol_overlap.kind,
        CoordinationEventKind::SymbolOverlapDetected
    );
    let migration_overlap = MigrationOverlapDetector::new(Arc::clone(&store))
        .detect_and_publish(
            &left_worker.id,
            &left_claim.id,
            &right_worker.id,
            &right_claim.id,
            now + Duration::seconds(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        migration_overlap.kind,
        CoordinationEventKind::MigrationOverlapDetected
    );
    let merge_conflict = MergeConflictPredictor::new(Arc::clone(&store))
        .detect_and_publish(
            &left_worker.id,
            &left_claim.id,
            &right_worker.id,
            &right_claim.id,
            now + Duration::seconds(4),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        merge_conflict.kind,
        CoordinationEventKind::MergeConflictPredicted
    );

    let registry = ContractRegistry::new(Arc::clone(&store));
    let contract = registry
        .register(
            ContractRegistration {
                goal_id: "goal-overlap".into(),
                stable_key: "api.shared.v1".into(),
                title: "Shared API".into(),
                kind: ContractKind::Api,
                producer: None,
                compatibility_notes: "Initial revision".into(),
            },
            CoordinationActor::System,
            None,
            now,
        )
        .unwrap();
    left_claim = store.claim(&left_claim.id).unwrap().unwrap();
    left_claim.consumes_contracts.push(ContractExpectation {
        contract_id: contract.id.as_str().into(),
        expected_revision: 1,
    });
    store.upsert_claim(&left_claim).unwrap();
    registry
        .revise(
            &contract.id,
            1,
            right_worker.id.clone(),
            "Required field changed".into(),
            CoordinationActor::Worker {
                worker_id: right_worker.id.clone(),
            },
            now + Duration::seconds(5),
        )
        .unwrap();
    let contract_events = ContractChangeDetector::new(Arc::clone(&store))
        .scan_goal("goal-overlap", now + Duration::seconds(6))
        .unwrap();
    assert_eq!(contract_events.len(), 1);
    assert_eq!(
        contract_events[0].kind,
        CoordinationEventKind::DependencyChanged
    );

    let evals = supervisor_eval_cases().unwrap();
    for (id, expected) in [
        (
            "benign-overlap-awareness",
            super::domain::InterventionLevel::Inform,
        ),
        (
            "semantic-conflict-coordinate",
            super::domain::InterventionLevel::Coordinate,
        ),
        (
            "stale-contract-recommendation",
            super::domain::InterventionLevel::Recommend,
        ),
    ] {
        assert_eq!(
            evals
                .iter()
                .find(|case| case.id == id)
                .unwrap()
                .expected_decision,
            expected
        );
    }
}

#[test]
fn stalled_worker_is_quarantined_replaced_and_recovered_without_duplicate_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let repository = directory.path().join("repository");
    fs::create_dir(&repository).unwrap();
    git(&repository, &["init", "-q"]);
    git(&repository, &["config", "user.email", "stall@example.test"]);
    git(&repository, &["config", "user.name", "Stall Test"]);
    fs::write(repository.join("README.md"), "base\n").unwrap();
    git(&repository, &["add", "README.md"]);
    git(&repository, &["commit", "-q", "-m", "base"]);
    let base = git_output(&repository, &["rev-parse", "HEAD"]);

    let tracker = Tracker::new(directory.path().join("goals"));
    tracker
        .create_goal("goal-stall", "Stall replacement", "")
        .unwrap();
    tracker
        .add_feature(
            "goal-stall",
            "feature-a",
            "Feature A",
            "recoverable work",
            Status::Planned,
        )
        .unwrap();
    let store = Arc::new(
        SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
    );
    let pool = WorkerPoolService::load(Arc::clone(&store), WorkerPoolPolicy::default()).unwrap();
    let started_at = Utc::now() - Duration::minutes(10);
    let original_worker = pool
        .start("goal-stall", Some(1), 1, started_at)
        .unwrap()
        .workers[0]
        .clone();
    let manager = WorktreeManager::open(&repository).unwrap();
    let original_binding = manager
        .create_for_worker("goal-stall", &original_worker.id, &base, started_at)
        .unwrap();
    pool.bind_workspace(&original_worker.id, original_binding.clone(), started_at)
        .unwrap();
    let claims = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
    let original_claim = pool
        .fill_ready_claims(&claims, "goal-stall", &base, started_at)
        .unwrap()
        .remove(0);
    fs::write(
        Path::new(&original_binding.worktree_path).join("partial.txt"),
        "preserve partial progress\n",
    )
    .unwrap();

    let now = Utc::now();
    let health_events = WorkerHealthDetector::new(
        Arc::clone(&store),
        HealthDetectionPolicy {
            heartbeat_stale_after_seconds: 60,
            activity_stale_after_seconds: 60,
            validation_failure_threshold: 3,
            validation_window_seconds: 600,
        },
    )
    .unwrap()
    .scan_goal("goal-stall", now)
    .unwrap();
    assert!(
        health_events
            .iter()
            .any(|event| event.kind == CoordinationEventKind::WorkerStalled)
    );

    let recovering = pool
        .report_worker_failure(
            &original_worker.id,
            WorkerFailureKind::WorkspaceUncertain,
            "heartbeat and activity stopped",
            now,
        )
        .unwrap();
    assert_eq!(recovering.state, WorkerState::Recovering);
    let orphan = manager
        .reconcile(&[])
        .unwrap()
        .orphaned_worktrees
        .into_iter()
        .find(|entry| entry.path == original_binding.worktree_path)
        .unwrap();
    let quarantine = manager
        .quarantine(&orphan, "stalled worker partial progress", now)
        .unwrap();
    assert!(Path::new(&quarantine.record_path).is_file());
    assert!(Path::new(&quarantine.worktree_path).is_dir());
    assert_eq!(
        fs::read_to_string(Path::new(&quarantine.worktree_path).join("partial.txt")).unwrap(),
        "preserve partial progress\n"
    );

    let expired = claims
        .expire_stale_claims("goal-stall", now, Duration::seconds(60))
        .unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].id, original_claim.id);
    let replacement = pool
        .replace_after_recovery(
            &claims,
            &original_worker.id,
            &base,
            RecoveryEvidence {
                claim_recovery_complete: true,
                workspace_recovery_complete: true,
                reason: "expired claim and preserved quarantine evidence".into(),
            },
            now + Duration::seconds(1),
        )
        .unwrap()
        .remove(0);
    assert_ne!(replacement.owner, original_worker.id);
    assert_eq!(
        replacement.lease_generation,
        original_claim.lease_generation + 1
    );
    let replacement_binding = manager
        .create_for_worker(
            "goal-stall",
            &replacement.owner,
            &base,
            now + Duration::seconds(1),
        )
        .unwrap();
    pool.bind_workspace(
        &replacement.owner,
        replacement_binding.clone(),
        now + Duration::seconds(1),
    )
    .unwrap();
    assert!(Path::new(&replacement_binding.worktree_path).is_dir());

    let active = store
        .claims_for_goal("goal-stall", Some(ClaimState::Active))
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, replacement.id);
    assert_eq!(
        store.claim(&original_claim.id).unwrap().unwrap().state,
        ClaimState::Expired
    );
    assert_eq!(
        store.worker(&original_worker.id).unwrap().unwrap().state,
        WorkerState::Failed
    );
    assert!(
        claims
            .heartbeat(
                &original_claim.id,
                &original_worker.id,
                original_claim.lease_generation,
                now + Duration::seconds(2),
            )
            .is_err()
    );
}

fn feature(id: &str, title: &str) -> Feature {
    Feature {
        id: id.into(),
        title: title.into(),
        description: format!("Independent {id} package"),
        status: Status::Partial,
        steps: vec![Step {
            id: "integrate".into(),
            title: "Validate and integrate".into(),
            done: false,
            next: false,
        }],
    }
}

fn active_worker(goal_id: &str, now: chrono::DateTime<Utc>) -> Worker {
    let mut worker = Worker::new(goal_id, now);
    worker.transition(WorkerState::Starting, now, None).unwrap();
    worker.transition(WorkerState::Active, now, None).unwrap();
    worker
}

fn file_validation_gate(id: &str, path: &str, expected: &str) -> ValidationGateSpec {
    ValidationGateSpec {
        id: id.into(),
        kind: ValidationGateKind::Integration,
        program: "sh".into(),
        args: vec![
            "-c".into(),
            format!("test \"$(cat {path})\" = '{expected}'"),
        ],
        required: true,
        max_output_bytes: 4_096,
    }
}

fn git(directory: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_output(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
