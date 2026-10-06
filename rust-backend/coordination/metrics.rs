use super::claims::{ClaimPolicy, ClaimService, ClaimServiceError};
use super::domain::{
    ClaimState, CoordinationEventKind, IntegrationJobState, NotificationState, WorkerState,
};
use super::store::{
    ClaimRepository, EventRepository, IntegrationJobRepository, InterventionRepository,
    NotificationRepository, PoolRepository, SqliteCoordinationStore, StoreError, WorkerRepository,
};
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Claims(#[from] ClaimServiceError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalMetricsSnapshot {
    pub goal_id: String,
    pub collected_at: DateTime<Utc>,
    pub utilization: UtilizationMetrics,
    pub ready_queue: ReadyQueueMetrics,
    pub claims: ClaimMetrics,
    pub throughput: ThroughputMetrics,
    pub failures: FailureMetrics,
    pub supervisor: SupervisorMetrics,
    pub notifications: NotificationMetrics,
    pub conflicts: ConflictMetrics,
    pub integration: IntegrationLatencyMetrics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UtilizationMetrics {
    pub desired_workers: usize,
    pub busy_workers: usize,
    pub utilization_percent: u64,
    pub workers_by_state: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadyQueueMetrics {
    pub ready_units: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimMetrics {
    pub active: usize,
    pub average_age_seconds: u64,
    pub maximum_age_seconds: u64,
    pub minimum_lease_remaining_seconds: Option<i64>,
    pub expiring_within_five_minutes: usize,
    pub past_due_leases: usize,
    pub claims_by_state: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThroughputMetrics {
    pub completed_claims: usize,
    pub completed_workers: usize,
    pub completed_claims_per_hour: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureMetrics {
    pub failed_workers: usize,
    pub blocked_or_expired_claims: usize,
    pub failed_integrations: usize,
    pub failure_events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorMetrics {
    pub actions: usize,
    pub actions_by_level: BTreeMap<String, usize>,
    pub actions_by_state: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationMetrics {
    pub total: usize,
    pub queued: usize,
    pub failed: usize,
    pub delivery_attempts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConflictMetrics {
    pub conflict_events: usize,
    pub conflicts_by_kind: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationLatencyMetrics {
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub samples: usize,
    pub average_seconds: u64,
    pub p95_seconds: u64,
    pub maximum_seconds: u64,
}

pub struct GoalMetricsService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
}

impl GoalMetricsService {
    pub fn new(tracker: Tracker, store: Arc<SqliteCoordinationStore>) -> Self {
        Self { tracker, store }
    }

    pub fn collect(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<GoalMetricsSnapshot, MetricsError> {
        self.tracker.get_goal(goal_id)?;
        let workers = self.store.workers_for_goal(goal_id, None)?;
        let claims = self.store.claims_for_goal(goal_id, None)?;
        let interventions = self.store.interventions_for_goal(goal_id, None)?;
        let notifications = self.store.notifications_for_goal(goal_id, None)?;
        let jobs = self.store.integration_jobs_for_goal(goal_id, None)?;
        let events = all_events_for_goal(self.store.as_ref(), goal_id)?;
        let desired_workers = self
            .store
            .goal_pool(goal_id)?
            .map(|pool| pool.desired_concurrency)
            .unwrap_or_default();

        let mut workers_by_state = BTreeMap::new();
        for worker in &workers {
            increment(&mut workers_by_state, enum_name(worker.state));
        }
        let busy_workers = workers
            .iter()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerState::Starting
                        | WorkerState::Active
                        | WorkerState::Waiting
                        | WorkerState::Recovering
                )
            })
            .count();
        let utilization_percent = if desired_workers == 0 {
            0
        } else {
            ((busy_workers as u128 * 100) / desired_workers as u128) as u64
        };

        let ready_units = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        )
        .ready_unclaimed_scopes(goal_id)?
        .len();

        let mut claims_by_state = BTreeMap::new();
        for claim in &claims {
            increment(&mut claims_by_state, enum_name(claim.state));
        }
        let active_claims = claims
            .iter()
            .filter(|claim| claim.state == ClaimState::Active)
            .collect::<Vec<_>>();
        let ages = active_claims
            .iter()
            .map(|claim| seconds_between(claim.metadata.created_at, now))
            .collect::<Vec<_>>();
        let lease_remaining = active_claims
            .iter()
            .map(|claim| (claim.lease_expires_at - now).num_seconds())
            .collect::<Vec<_>>();
        let completed_claims = claims
            .iter()
            .filter(|claim| claim.state == ClaimState::Completed)
            .count();
        let throughput_window_seconds = claims
            .iter()
            .map(|claim| claim.metadata.created_at)
            .min()
            .map(|earliest| seconds_between(earliest, now).max(1))
            .unwrap_or(1);

        let mut actions_by_level = BTreeMap::new();
        let mut actions_by_state = BTreeMap::new();
        for intervention in &interventions {
            increment(&mut actions_by_level, enum_name(intervention.level));
            increment(&mut actions_by_state, enum_name(intervention.state));
        }

        let mut conflicts_by_kind = BTreeMap::new();
        let mut failure_events = 0usize;
        for event in &events {
            if matches!(
                event.kind,
                CoordinationEventKind::FileOverlapDetected
                    | CoordinationEventKind::SymbolOverlapDetected
                    | CoordinationEventKind::MigrationOverlapDetected
                    | CoordinationEventKind::MergeConflictPredicted
            ) {
                increment(&mut conflicts_by_kind, enum_name(event.kind.clone()));
            }
            if matches!(
                event.kind,
                CoordinationEventKind::WorkerStalled
                    | CoordinationEventKind::RepeatedValidationFailure
                    | CoordinationEventKind::BlockerRaised
            ) {
                failure_events += 1;
            }
        }

        let mut latencies = jobs
            .iter()
            .filter_map(|job| Some(seconds_between(job.started_at?, job.completed_at?)))
            .collect::<Vec<_>>();
        latencies.sort_unstable();
        let latency_sum = latencies.iter().copied().sum::<u64>();
        let latency_average = latency_sum
            .checked_div(latencies.len() as u64)
            .unwrap_or_default();
        let latency_p95 = percentile(&latencies, 95);

        Ok(GoalMetricsSnapshot {
            goal_id: goal_id.into(),
            collected_at: now,
            utilization: UtilizationMetrics {
                desired_workers,
                busy_workers,
                utilization_percent,
                workers_by_state,
            },
            ready_queue: ReadyQueueMetrics { ready_units },
            claims: ClaimMetrics {
                active: active_claims.len(),
                average_age_seconds: ages
                    .iter()
                    .copied()
                    .sum::<u64>()
                    .checked_div(ages.len() as u64)
                    .unwrap_or_default(),
                maximum_age_seconds: ages.iter().copied().max().unwrap_or_default(),
                minimum_lease_remaining_seconds: lease_remaining.iter().copied().min(),
                expiring_within_five_minutes: lease_remaining
                    .iter()
                    .filter(|remaining| **remaining >= 0 && **remaining <= 300)
                    .count(),
                past_due_leases: lease_remaining
                    .iter()
                    .filter(|remaining| **remaining < 0)
                    .count(),
                claims_by_state,
            },
            throughput: ThroughputMetrics {
                completed_claims,
                completed_workers: workers
                    .iter()
                    .filter(|worker| worker.state == WorkerState::Completed)
                    .count(),
                completed_claims_per_hour: completed_claims as f64 * 3_600.0
                    / throughput_window_seconds as f64,
            },
            failures: FailureMetrics {
                failed_workers: workers
                    .iter()
                    .filter(|worker| worker.state == WorkerState::Failed)
                    .count(),
                blocked_or_expired_claims: claims
                    .iter()
                    .filter(|claim| {
                        matches!(
                            claim.state,
                            ClaimState::Blocked | ClaimState::Expired | ClaimState::Revoked
                        )
                    })
                    .count(),
                failed_integrations: jobs
                    .iter()
                    .filter(|job| job.state == IntegrationJobState::Failed)
                    .count(),
                failure_events,
            },
            supervisor: SupervisorMetrics {
                actions: interventions.len(),
                actions_by_level,
                actions_by_state,
            },
            notifications: NotificationMetrics {
                total: notifications.len(),
                queued: notifications
                    .iter()
                    .filter(|notification| notification.state == NotificationState::Queued)
                    .count(),
                failed: notifications
                    .iter()
                    .filter(|notification| notification.state == NotificationState::Failed)
                    .count(),
                delivery_attempts: notifications
                    .iter()
                    .map(|notification| notification.delivery_attempts as u64)
                    .sum(),
            },
            conflicts: ConflictMetrics {
                conflict_events: conflicts_by_kind.values().sum(),
                conflicts_by_kind,
            },
            integration: IntegrationLatencyMetrics {
                queued: jobs
                    .iter()
                    .filter(|job| job.state == IntegrationJobState::Queued)
                    .count(),
                running: jobs
                    .iter()
                    .filter(|job| job.state == IntegrationJobState::Running)
                    .count(),
                completed: jobs
                    .iter()
                    .filter(|job| job.state == IntegrationJobState::Succeeded)
                    .count(),
                failed: jobs
                    .iter()
                    .filter(|job| job.state == IntegrationJobState::Failed)
                    .count(),
                samples: latencies.len(),
                average_seconds: latency_average,
                p95_seconds: latency_p95,
                maximum_seconds: latencies.iter().copied().max().unwrap_or_default(),
            },
        })
    }
}

fn all_events_for_goal(
    store: &SqliteCoordinationStore,
    goal_id: &str,
) -> Result<Vec<super::domain::CoordinationEvent>, StoreError> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let page = store.events_for_goal(goal_id, after, None, 1_000)?;
        if page.is_empty() {
            break;
        }
        after = page
            .last()
            .and_then(|event| event.sequence)
            .unwrap_or(after);
        let complete = page.len() < 1_000;
        events.extend(page);
        if complete {
            break;
        }
    }
    Ok(events)
}

fn enum_name(value: impl std::fmt::Debug) -> String {
    format!("{value:?}").to_ascii_lowercase()
}

fn increment(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_default() += 1;
}

fn seconds_between(start: DateTime<Utc>, end: DateTime<Utc>) -> u64 {
    (end - start).num_seconds().max(0) as u64
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        Claim, ClaimScope, CoordinationActor, CoordinationEvent, EventSeverity,
        SupervisorIntervention, Worker,
    };
    use crate::coordination::pool::{GoalPoolState, PoolMode};
    use crate::models::Status;
    use chrono::Duration;

    #[test]
    fn collects_every_required_operational_metric_from_durable_state() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "metrics").unwrap();
        for feature_id in ["active", "ready"] {
            tracker
                .add_feature("goal-a", feature_id, feature_id, "metrics", Status::Planned)
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        store
            .upsert_goal_pool(
                &GoalPoolState {
                    goal_id: "goal-a".into(),
                    desired_concurrency: 2,
                    mode: PoolMode::Running,
                    worker_permissions: Default::default(),
                    resource_quotas: Default::default(),
                    started_at: Some(now - Duration::hours(1)),
                },
                now,
            )
            .unwrap();
        let mut worker = Worker::new("goal-a", now - Duration::minutes(10));
        worker
            .transition(WorkerState::Starting, now - Duration::minutes(9), None)
            .unwrap();
        worker
            .transition(WorkerState::Active, now - Duration::minutes(8), None)
            .unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "active".into(),
            },
            worker.id.clone(),
            "base-a",
            now - Duration::minutes(8),
            now + Duration::minutes(2),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let intervention = SupervisorIntervention::new(
            "goal-a",
            super::super::domain::InterventionLevel::Observe,
            vec![],
            vec![worker.id.clone()],
            "observe",
            now,
        );
        store.upsert_intervention(&intervention).unwrap();
        let conflict = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::FileOverlapDetected,
            EventSeverity::Warning,
            CoordinationActor::System,
            claim.id.as_str(),
            serde_json::json!({"claimId": claim.id.as_str()}),
            now,
        );
        store.append_event(&conflict).unwrap();

        let snapshot = GoalMetricsService::new(tracker, store)
            .collect("goal-a", now)
            .unwrap();
        assert_eq!(snapshot.utilization.desired_workers, 2);
        assert_eq!(snapshot.utilization.busy_workers, 1);
        assert_eq!(snapshot.utilization.utilization_percent, 50);
        assert_eq!(snapshot.ready_queue.ready_units, 1);
        assert_eq!(snapshot.claims.active, 1);
        assert_eq!(snapshot.claims.expiring_within_five_minutes, 1);
        assert_eq!(snapshot.supervisor.actions, 1);
        assert_eq!(snapshot.conflicts.conflict_events, 1);
        assert_eq!(snapshot.notifications.total, 0);
        assert_eq!(snapshot.integration.samples, 0);
    }
}
