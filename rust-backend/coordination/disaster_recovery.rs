use super::claims::{ClaimPolicy, ClaimService};
use super::domain::{
    ClaimScope, ClaimState, IntegrationArtifact, IntegrationArtifactId, IntegrationDiffSummary,
    IntegrationJobState, IntegrationWorkspaceSnapshot, RecordMetadata, Worker, WorkerId,
    WorkerState,
};
use super::integration::IntegrationQueueService;
use super::store::{
    ClaimRepository, EventRepository, IntegrationArtifactRepository, IntegrationJobRepository,
    SqliteCoordinationStore, WorkerRepository,
};
use super::workspace::{CleanupAuthorization, WorkspaceError, WorktreeManager};
use crate::models::Status;
use crate::tracker::Tracker;
use chrono::{Duration, Utc};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

fn active_worker(goal_id: &str, now: chrono::DateTime<Utc>) -> Worker {
    let mut worker = Worker::new(goal_id, now);
    worker.transition(WorkerState::Starting, now, None).unwrap();
    worker.transition(WorkerState::Active, now, None).unwrap();
    worker
}

#[test]
fn backup_restore_and_restart_recovery_preserve_ownership_audit_and_queue_state() {
    let directory = tempfile::tempdir().unwrap();
    let tracker = Tracker::new(directory.path().join("goals"));
    tracker
        .create_goal("goal-a", "Goal A", "disaster recovery")
        .unwrap();
    tracker
        .add_feature(
            "goal-a",
            "feature-a",
            "Feature A",
            "recover safely",
            Status::Planned,
        )
        .unwrap();

    let source_path = directory.path().join("coordination.sqlite");
    let backup_path = directory.path().join("coordination.backup.sqlite");
    let restored_path = directory.path().join("coordination.restored.sqlite");
    let source = Arc::new(SqliteCoordinationStore::open(&source_path).unwrap());
    let stale_at = Utc::now() - Duration::minutes(10);
    let first_worker = active_worker("goal-a", stale_at);
    source.upsert_worker(&first_worker).unwrap();
    let first_service =
        ClaimService::new(tracker.clone(), Arc::clone(&source), ClaimPolicy::default());
    let original = first_service
        .claim_ready_unit(
            "goal-a",
            &first_worker.id,
            Some(ClaimScope::Feature {
                feature_id: "feature-a".into(),
            }),
            "base-a",
            "disaster-acquire-original",
            stale_at,
        )
        .unwrap();
    let original_audit = source.events_for_goal("goal-a", 0, None, 100).unwrap();
    assert!(!original_audit.is_empty());
    source.backup_to(&backup_path).unwrap();

    let restored = Arc::new(SqliteCoordinationStore::open(&restored_path).unwrap());
    restored.restore_from(&backup_path).unwrap();
    restored.integrity_check().unwrap();
    assert_eq!(
        restored.claim(&original.id).unwrap().unwrap().state,
        ClaimState::Active
    );
    let restored_audit = restored.events_for_goal("goal-a", 0, None, 100).unwrap();
    assert_eq!(restored_audit, original_audit);

    let now = Utc::now();
    let replacement_worker = active_worker("goal-a", now);
    restored.upsert_worker(&replacement_worker).unwrap();
    let service = ClaimService::new(
        tracker.clone(),
        Arc::clone(&restored),
        ClaimPolicy::default(),
    );
    let replacement = service
        .expire_and_reclaim(
            "goal-a",
            &replacement_worker.id,
            original.scope.clone(),
            "base-b",
            "disaster-reclaim",
            now,
            Duration::minutes(1),
        )
        .unwrap();
    assert_eq!(
        restored.claim(&original.id).unwrap().unwrap().state,
        ClaimState::Expired
    );
    assert_eq!(replacement.lease_generation, original.lease_generation + 1);
    let active = restored
        .claims_for_goal("goal-a", Some(ClaimState::Active))
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, replacement.id);
    assert!(
        service
            .heartbeat(
                &original.id,
                &first_worker.id,
                original.lease_generation,
                now
            )
            .is_err(),
        "an expired generation must not regain ownership"
    );

    let recovered_audit = restored.events_for_goal("goal-a", 0, None, 100).unwrap();
    let original_event_ids = original_audit
        .iter()
        .map(|event| event.id.as_str())
        .collect::<BTreeSet<_>>();
    let recovered_event_ids = recovered_audit
        .iter()
        .map(|event| event.id.as_str())
        .collect::<BTreeSet<_>>();
    assert!(original_event_ids.is_subset(&recovered_event_ids));
    assert!(recovered_audit.len() >= original_audit.len() + 2);
    assert_eq!(recovered_event_ids.len(), recovered_audit.len());
    assert!(
        recovered_audit
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );

    let artifact = IntegrationArtifact {
        metadata: RecordMetadata::new(now),
        id: IntegrationArtifactId::new(),
        goal_id: "goal-a".into(),
        claim_id: replacement.id.clone(),
        claim_generation: replacement.lease_generation,
        worker_id: replacement_worker.id.clone(),
        repository_id: "repo-a".into(),
        base_revision: "base-b".into(),
        head_revision: "head-b".into(),
        commits: vec![],
        diff_summary: IntegrationDiffSummary {
            files_changed: 0,
            insertions: 0,
            deletions: 0,
            changed_paths: vec![],
            summary: "recovery fixture".into(),
        },
        changed_contracts: vec![],
        migrations: vec![],
        validations: vec![],
        evidence_refs: vec!["event:recovered-audit".into()],
        known_risks: vec![],
        workspace: IntegrationWorkspaceSnapshot {
            branch: "codex/recovery".into(),
            staged_paths: vec![],
            unstaged_paths: vec![],
            untracked_paths: vec![],
            conflicted_paths: vec![],
        },
    };
    restored.insert_integration_artifact(&artifact).unwrap();
    let queue = IntegrationQueueService::new(Arc::clone(&restored));
    let job = queue.enqueue(&artifact.id, 0, now).unwrap();
    let running = queue.acquire_next("repo-a", now).unwrap().unwrap();
    assert_eq!(running.id, job.id);
    assert_eq!(running.state, IntegrationJobState::Running);

    let recovered_jobs = restored
        .recover_running_integration_jobs(now + Duration::seconds(1))
        .unwrap();
    assert_eq!(recovered_jobs.len(), 1);
    assert_eq!(recovered_jobs[0].id, job.id);
    assert_eq!(recovered_jobs[0].state, IntegrationJobState::Queued);
    let jobs = restored.integration_jobs_for_goal("goal-a", None).unwrap();
    assert!(
        jobs.iter()
            .all(|job| job.state != IntegrationJobState::Running)
    );
    assert!(jobs.iter().any(|candidate| {
        candidate.id == job.id && candidate.state == IntegrationJobState::Queued
    }));
}

#[test]
fn dirty_unintegrated_workspace_cannot_be_cleaned_during_recovery() {
    let repository = tempfile::tempdir().unwrap();
    git(repository.path(), &["init", "-q"]);
    git(
        repository.path(),
        &["config", "user.email", "recovery@example.test"],
    );
    git(repository.path(), &["config", "user.name", "Recovery Test"]);
    fs::write(repository.path().join("README.md"), "initial\n").unwrap();
    git(repository.path(), &["add", "README.md"]);
    git(repository.path(), &["commit", "-q", "-m", "initial"]);

    let manager = WorktreeManager::open(repository.path()).unwrap();
    let binding = manager
        .create_for_worker("goal-a", &WorkerId::new(), "HEAD", Utc::now())
        .unwrap();
    let worktree = Path::new(&binding.worktree_path);
    fs::write(worktree.join("unintegrated.txt"), "preserve me\n").unwrap();
    let head = manager.inspect(&binding).unwrap().head_revision;

    let cleanup = manager.cleanup(
        &binding,
        CleanupAuthorization::Integrated {
            integration_artifact_id: "artifact-recovery".into(),
            expected_head_revision: head,
        },
        Utc::now(),
    );
    assert!(matches!(
        cleanup,
        Err(WorkspaceError::CleanupNotAuthorized(_))
    ));
    assert!(worktree.is_dir());
    assert_eq!(
        fs::read_to_string(worktree.join("unintegrated.txt")).unwrap(),
        "preserve me\n"
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(repository.path())
            .args([
                "show-ref",
                "--verify",
                &format!("refs/heads/{}", binding.branch)
            ])
            .output()
            .unwrap()
            .status
            .success()
    );
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
