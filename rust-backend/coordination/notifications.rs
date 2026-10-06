use super::domain::{
    Claim, ClaimId, ClaimScope, ClaimState, ContractId, CoordinationActor, CoordinationEvent,
    CoordinationEventKind, CoordinationEventPayload, CoordinationExchange,
    CoordinationExchangeState, DomainError, EscalationScope, EventId, EventSeverity, ExchangeId,
    HumanEscalation, InterventionId, NotificationId, NotificationState, SharedContract,
    SupervisorIntervention, WorkPackage, WorkPackageId, Worker, WorkerId, WorkerNotification,
};
use super::store::{
    ClaimRepository, ContractRepository, EscalationRepository, EventRepository, ExchangeRepository,
    ExpectedRecordUpdate, NotificationRepository, SqliteCoordinationStore, StoreError,
    WorkPackageRepository, WorkerRepository,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecipientReason {
    DirectClaim,
    WorkPackageOwner,
    DependentPackage,
    ContractConsumer,
    OverlapParticipant,
    WorkerHealth,
    EscalationScope,
    CoordinationTarget,
    SupervisorTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationRecipient {
    pub worker_id: WorkerId,
    pub reasons: Vec<RecipientReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecipientResolution {
    pub goal_id: String,
    pub source_event_id: Option<EventId>,
    pub source_intervention_id: Option<InterventionId>,
    pub recipients: Vec<NotificationRecipient>,
}

#[derive(Debug, Error)]
pub enum RecipientResolutionError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] super::domain::DomainError),
    #[error("Supervisor intervention targets an unknown, terminal, or foreign worker: {0}")]
    InvalidSupervisorTarget(String),
}

#[derive(Debug, Clone)]
pub struct NotificationRecipientResolver {
    store: Arc<SqliteCoordinationStore>,
}

impl NotificationRecipientResolver {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn resolve_event(
        &self,
        event: &CoordinationEvent,
    ) -> Result<RecipientResolution, RecipientResolutionError> {
        let mut context = ResolutionContext::load(self.store.as_ref(), &event.goal_id)?;
        let Some(payload) = event.typed_payload()? else {
            return Ok(context.finish(Some(event.id.clone()), None));
        };
        match payload {
            CoordinationEventPayload::Activity(payload) => {
                context.add_claim(&payload.claim_id, RecipientReason::DirectClaim, true);
            }
            CoordinationEventPayload::Claim(payload) => {
                context.add_claim(&payload.claim_id, RecipientReason::DirectClaim, true);
            }
            CoordinationEventPayload::Contract(payload) => {
                context
                    .add_contract_consumers(Some(&payload.contract_id), Some(&payload.stable_key));
            }
            CoordinationEventPayload::Dependency(payload) => {
                for worker_id in payload.affected_worker_ids {
                    context.add_worker(&worker_id, RecipientReason::DependentPackage);
                }
                context.add_package_with_dependents(&payload.work_package_id);
            }
            CoordinationEventPayload::ContractMismatch(payload) => {
                context.add_worker(&payload.worker_id, RecipientReason::ContractConsumer);
                context
                    .add_contract_consumers(Some(&payload.contract_id), Some(&payload.stable_key));
            }
            CoordinationEventPayload::FileOverlap(payload) => {
                context.add_worker(&payload.left_worker_id, RecipientReason::OverlapParticipant);
                context.add_worker(
                    &payload.right_worker_id,
                    RecipientReason::OverlapParticipant,
                );
            }
            CoordinationEventPayload::SymbolOverlap(payload) => {
                context.add_worker(&payload.left_worker_id, RecipientReason::OverlapParticipant);
                context.add_worker(
                    &payload.right_worker_id,
                    RecipientReason::OverlapParticipant,
                );
            }
            CoordinationEventPayload::MigrationOverlap(payload) => {
                context.add_worker(&payload.left_worker_id, RecipientReason::OverlapParticipant);
                context.add_worker(
                    &payload.right_worker_id,
                    RecipientReason::OverlapParticipant,
                );
            }
            CoordinationEventPayload::MergeConflict(payload) => {
                context.add_worker(&payload.left_worker_id, RecipientReason::OverlapParticipant);
                context.add_worker(
                    &payload.right_worker_id,
                    RecipientReason::OverlapParticipant,
                );
            }
            CoordinationEventPayload::WorkerStalled(payload) => {
                context.add_worker(&payload.worker_id, RecipientReason::WorkerHealth);
            }
            CoordinationEventPayload::RepeatedValidationFailure(payload) => {
                context.add_worker(&payload.worker_id, RecipientReason::WorkerHealth);
            }
            CoordinationEventPayload::WatchdogAction(payload) => {
                context.add_worker(&payload.worker_id, RecipientReason::WorkerHealth);
            }
            CoordinationEventPayload::Blocker(payload) => {
                context.add_worker(&payload.worker_id, RecipientReason::DirectClaim);
                if let Some(escalation_id) = payload.escalation_id
                    && let Some(escalation) = self.store.escalation(&escalation_id)?
                {
                    context.add_escalation_scope(&escalation);
                }
            }
            CoordinationEventPayload::CoordinationRequest(payload) => {
                context.add_coordination_target(&payload.target_scope);
            }
            CoordinationEventPayload::HumanDecision(payload) => {
                if let Some(escalation) = self.store.escalation(&payload.escalation_id)? {
                    context.add_escalation_scope(&escalation);
                }
            }
            CoordinationEventPayload::PrivilegedAction(_) => {}
            CoordinationEventPayload::Completion(payload) => {
                context.add_claim(&payload.claim_id, RecipientReason::DirectClaim, true);
            }
        }
        if let CoordinationActor::Worker { worker_id } = &event.producer {
            context.remove_worker(worker_id);
        }
        Ok(context.finish(Some(event.id.clone()), None))
    }

    pub fn resolve_intervention(
        &self,
        intervention: &SupervisorIntervention,
    ) -> Result<RecipientResolution, RecipientResolutionError> {
        let mut context = ResolutionContext::load(self.store.as_ref(), &intervention.goal_id)?;
        for worker_id in &intervention.target_workers {
            if !context.is_eligible(worker_id) {
                return Err(RecipientResolutionError::InvalidSupervisorTarget(
                    worker_id.as_str().into(),
                ));
            }
            context.add_worker(worker_id, RecipientReason::SupervisorTarget);
        }
        Ok(context.finish(None, Some(intervention.id.clone())))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationNoisePolicy {
    pub minimum_severity: EventSeverity,
    pub max_pending_per_worker: usize,
    pub coalesce_window: Duration,
    pub unchanged_cooldown: Duration,
}

impl Default for NotificationNoisePolicy {
    fn default() -> Self {
        Self {
            minimum_severity: EventSeverity::Info,
            max_pending_per_worker: 32,
            coalesce_window: Duration::minutes(5),
            unchanged_cooldown: Duration::minutes(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationCandidate {
    pub goal_id: String,
    pub target_worker: WorkerId,
    pub source_event_id: EventId,
    pub purpose: String,
    pub summary: String,
    pub severity: EventSeverity,
    pub evidence_refs: Vec<String>,
    pub recommended_action: Option<String>,
    pub required_acknowledgement: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationSuppressionReason {
    BelowRelevanceThreshold,
    UnchangedContext,
    PendingCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationEnqueueOutcome {
    Queued(WorkerNotification),
    Coalesced {
        notification: WorkerNotification,
        superseded_ids: Vec<NotificationId>,
    },
    Suppressed {
        reason: NotificationSuppressionReason,
        existing_notification_id: Option<NotificationId>,
    },
}

#[derive(Debug, Error)]
pub enum NotificationQueueError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("invalid notification noise policy")]
    InvalidPolicy,
    #[error("notification target is unknown, terminal, or belongs to another goal")]
    InvalidTarget,
    #[error("notification source event is unknown or belongs to another goal")]
    InvalidSourceEvent,
    #[error("notification enqueue time precedes existing durable state")]
    InvalidTimestamp,
}

#[derive(Debug, Clone)]
pub struct NotificationQueueService {
    store: Arc<SqliteCoordinationStore>,
    policy: NotificationNoisePolicy,
}

impl NotificationQueueService {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        policy: NotificationNoisePolicy,
    ) -> Result<Self, NotificationQueueError> {
        if policy.max_pending_per_worker == 0
            || policy.max_pending_per_worker > 256
            || policy.coalesce_window <= Duration::zero()
            || policy.unchanged_cooldown < policy.coalesce_window
        {
            return Err(NotificationQueueError::InvalidPolicy);
        }
        Ok(Self { store, policy })
    }

    pub fn enqueue(
        &self,
        candidate: NotificationCandidate,
        now: DateTime<Utc>,
    ) -> Result<NotificationEnqueueOutcome, NotificationQueueError> {
        for attempt in 0..32 {
            match self.enqueue_once(candidate.clone(), now) {
                Err(NotificationQueueError::Store(StoreError::StaleNotificationRevision(_)))
                    if attempt < 31 =>
                {
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("bounded notification retry loop always returns")
    }

    fn enqueue_once(
        &self,
        candidate: NotificationCandidate,
        now: DateTime<Utc>,
    ) -> Result<NotificationEnqueueOutcome, NotificationQueueError> {
        let worker = self
            .store
            .worker(&candidate.target_worker)?
            .filter(|worker| worker.goal_id == candidate.goal_id && !worker.state.is_terminal())
            .ok_or(NotificationQueueError::InvalidTarget)?;
        let source_event = self
            .store
            .event(&candidate.source_event_id)?
            .filter(|event| event.goal_id == candidate.goal_id)
            .ok_or(NotificationQueueError::InvalidSourceEvent)?;
        let severity = if source_event.severity.rank() > candidate.severity.rank() {
            source_event.severity
        } else {
            candidate.severity
        };
        if severity.rank() < self.policy.minimum_severity.rank()
            && !candidate.required_acknowledgement
        {
            return Ok(NotificationEnqueueOutcome::Suppressed {
                reason: NotificationSuppressionReason::BelowRelevanceThreshold,
                existing_notification_id: None,
            });
        }
        let mut notification = WorkerNotification::new(
            candidate.goal_id,
            worker.id.clone(),
            candidate.source_event_id,
            candidate.purpose,
            candidate.summary,
            severity,
            candidate.evidence_refs,
            candidate.recommended_action,
            candidate.required_acknowledgement,
            candidate.expires_at,
            now,
        )?;
        let fingerprint = notification_content_fingerprint(&notification);
        notification.content_fingerprint = Some(fingerprint.clone());

        let pending = self
            .store
            .notifications_for_worker(&worker.id, None)?
            .into_iter()
            .filter(|existing| {
                existing.goal_id == notification.goal_id
                    && matches!(
                        existing.state,
                        NotificationState::Queued
                            | NotificationState::Delivered
                            | NotificationState::Acknowledged
                    )
                    && existing
                        .expires_at
                        .is_none_or(|expires_at| expires_at > now)
            })
            .collect::<Vec<_>>();
        if pending
            .iter()
            .any(|existing| existing.metadata.updated_at > now)
        {
            return Err(NotificationQueueError::InvalidTimestamp);
        }
        if let Some(existing) = pending.iter().find(|existing| {
            existing.content_fingerprint.as_deref() == Some(fingerprint.as_str())
                && now - existing.metadata.updated_at <= self.policy.unchanged_cooldown
        }) {
            return Ok(NotificationEnqueueOutcome::Suppressed {
                reason: NotificationSuppressionReason::UnchangedContext,
                existing_notification_id: Some(existing.id.clone()),
            });
        }

        let mut related = pending
            .iter()
            .filter(|existing| {
                existing.state == NotificationState::Queued
                    && existing.purpose == notification.purpose
                    && now - existing.metadata.updated_at <= self.policy.coalesce_window
            })
            .cloned()
            .collect::<Vec<_>>();
        related.sort_by(|left, right| {
            left.metadata
                .updated_at
                .cmp(&right.metadata.updated_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        related.truncate(32);
        if related.is_empty() {
            if pending.len() >= self.policy.max_pending_per_worker {
                return Ok(NotificationEnqueueOutcome::Suppressed {
                    reason: NotificationSuppressionReason::PendingCapacity,
                    existing_notification_id: None,
                });
            }
            self.store.upsert_notification(&notification)?;
            return Ok(NotificationEnqueueOutcome::Queued(notification));
        }

        let mut evidence = related
            .iter()
            .flat_map(|existing| existing.evidence_refs.iter().cloned())
            .chain(notification.evidence_refs.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .take(32)
            .collect::<Vec<_>>();
        evidence.sort();
        notification.evidence_refs = evidence;
        notification.required_acknowledgement |= related
            .iter()
            .any(|existing| existing.required_acknowledgement);
        if let Some(maximum) = related
            .iter()
            .map(|existing| existing.severity)
            .max_by_key(|severity| severity.rank())
            && maximum.rank() > notification.severity.rank()
        {
            notification.severity = maximum;
        }
        let latest_summary = notification.summary.clone();
        notification.summary = bounded_notification_text(
            &format!(
                "{} related updates. Latest: {latest_summary}",
                related.len() + 1
            ),
            2_000,
        );
        notification.coalesced_source_event_ids = related
            .iter()
            .flat_map(|existing| {
                std::iter::once(existing.source_event_id.clone())
                    .chain(existing.coalesced_source_event_ids.iter().cloned())
            })
            .fold(Vec::new(), |mut event_ids, event_id| {
                if !event_ids.contains(&event_id) && event_ids.len() < 32 {
                    event_ids.push(event_id);
                }
                event_ids
            });
        notification.validate()?;
        let mut superseded = Vec::new();
        let mut superseded_ids = Vec::new();
        for mut existing in related {
            let expected_updated_at = existing.metadata.updated_at;
            existing.supersede_attributed(
                notification.id.clone(),
                CoordinationActor::System,
                Some("coalesced into a newer related notification".into()),
                next_notification_time(now, expected_updated_at),
            )?;
            superseded_ids.push(existing.id.clone());
            superseded.push(ExpectedRecordUpdate {
                record: existing,
                expected_updated_at,
            });
        }
        self.store
            .replace_notifications_atomically(&notification, &superseded)?;
        Ok(NotificationEnqueueOutcome::Coalesced {
            notification,
            superseded_ids,
        })
    }
}

fn notification_content_fingerprint(notification: &WorkerNotification) -> String {
    let material = serde_json::json!({
        "goalId": notification.goal_id,
        "targetWorker": notification.target_worker,
        "purpose": notification.purpose,
        "summary": notification.summary,
        "severity": notification.severity,
        "recommendedAction": notification.recommended_action,
        "requiredAcknowledgement": notification.required_acknowledgement,
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&material).unwrap_or_default());
    format!("sha256:{:x}", hasher.finalize())
}

fn bounded_notification_text(value: &str, maximum: usize) -> String {
    let mut characters = value.chars();
    let bounded = characters.by_ref().take(maximum).collect::<String>();
    if characters.next().is_some() {
        let mut shortened = bounded
            .chars()
            .take(maximum.saturating_sub(1))
            .collect::<String>();
        shortened.push('…');
        shortened
    } else {
        bounded
    }
}

fn next_notification_time(requested: DateTime<Utc>, current: DateTime<Utc>) -> DateTime<Utc> {
    if requested <= current {
        current + Duration::microseconds(1)
    } else {
        requested
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationNotificationDelivery {
    pub worker_id: WorkerId,
    pub notification_id: NotificationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationRequestDispatch {
    pub event_id: EventId,
    pub event_sequence: u64,
    pub deliveries: Vec<CoordinationNotificationDelivery>,
}

#[derive(Debug, Error)]
pub enum CoordinationRequestError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Recipients(#[from] RecipientResolutionError),
    #[error(transparent)]
    Queue(#[from] NotificationQueueError),
    #[error("coordination request event or requester attribution is invalid")]
    InvalidRequest,
    #[error("coordination request did not resolve to another active worker")]
    NoRecipients,
    #[error("stored coordination request could not be replayed")]
    StoredEventMissing,
    #[error("coordination request could not be delivered to worker {0}")]
    Undeliverable(String),
}

#[derive(Debug, Clone)]
pub struct CoordinationRequestService {
    store: Arc<SqliteCoordinationStore>,
    recipients: NotificationRecipientResolver,
    queue: NotificationQueueService,
}

impl CoordinationRequestService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            recipients: NotificationRecipientResolver::new(Arc::clone(&store)),
            queue: NotificationQueueService::new(
                Arc::clone(&store),
                NotificationNoisePolicy::default(),
            )
            .expect("default notification noise policy must be valid"),
            store,
        }
    }

    pub fn dispatch(
        &self,
        event: &CoordinationEvent,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<CoordinationRequestDispatch, CoordinationRequestError> {
        let Some(CoordinationEventPayload::CoordinationRequest(request)) = event.typed_payload()?
        else {
            return Err(CoordinationRequestError::InvalidRequest);
        };
        if !matches!(
            &event.producer,
            CoordinationActor::Worker { worker_id }
                if worker_id == &request.requester_worker_id
        ) || idempotency_key.trim().is_empty()
            || idempotency_key.chars().count() > 200
        {
            return Err(CoordinationRequestError::InvalidRequest);
        }
        let preview = self.recipients.resolve_event(event)?;
        if preview.recipients.is_empty() {
            return Err(CoordinationRequestError::NoRecipients);
        }
        let event_sequence = self.store.append_event_once(
            &format!(
                "worker.coordination:{}:{idempotency_key}",
                request.claim_id.as_str()
            ),
            event,
        )?;
        let stored = self
            .store
            .events_for_goal(&event.goal_id, event_sequence.saturating_sub(1), None, 1)?
            .into_iter()
            .find(|stored| stored.sequence == Some(event_sequence))
            .ok_or(CoordinationRequestError::StoredEventMissing)?;
        let Some(CoordinationEventPayload::CoordinationRequest(stored_request)) =
            stored.typed_payload()?
        else {
            return Err(CoordinationRequestError::StoredEventMissing);
        };
        let resolution = self.recipients.resolve_event(&stored)?;
        if resolution.recipients.is_empty() {
            return Err(CoordinationRequestError::NoRecipients);
        }
        let mut deliveries = Vec::new();
        for recipient in resolution.recipients {
            let mut evidence_refs = stored_request.evidence_refs.clone();
            let event_reference = format!("event:{}", stored.id.as_str());
            if !evidence_refs.contains(&event_reference) {
                evidence_refs.insert(0, event_reference);
            }
            evidence_refs.truncate(32);
            let outcome = self.queue.enqueue(
                NotificationCandidate {
                    goal_id: stored.goal_id.clone(),
                    target_worker: recipient.worker_id.clone(),
                    source_event_id: stored.id.clone(),
                    purpose: "coordination_request".into(),
                    summary: bounded_notification_text(
                        &format!(
                            "Worker {} requests coordination for {}: {}",
                            stored_request.requester_worker_id.as_str(),
                            stored_request.target_scope,
                            stored_request.desired_outcome
                        ),
                        2_000,
                    ),
                    severity: stored.severity,
                    evidence_refs,
                    recommended_action: Some(
                        "Review the scoped evidence, acknowledge the request, and record the coordination outcome."
                            .into(),
                    ),
                    required_acknowledgement: true,
                    expires_at: Some(now + Duration::minutes(30)),
                },
                now,
            )?;
            let notification_id = match outcome {
                NotificationEnqueueOutcome::Queued(notification)
                | NotificationEnqueueOutcome::Coalesced { notification, .. } => notification.id,
                NotificationEnqueueOutcome::Suppressed {
                    reason: NotificationSuppressionReason::UnchangedContext,
                    existing_notification_id: Some(notification_id),
                } => notification_id,
                NotificationEnqueueOutcome::Suppressed { .. } => {
                    return Err(CoordinationRequestError::Undeliverable(
                        recipient.worker_id.as_str().into(),
                    ));
                }
            };
            deliveries.push(CoordinationNotificationDelivery {
                worker_id: recipient.worker_id,
                notification_id,
            });
        }
        deliveries.sort_by(|left, right| left.worker_id.as_str().cmp(right.worker_id.as_str()));
        Ok(CoordinationRequestDispatch {
            event_id: stored.id,
            event_sequence,
            deliveries,
        })
    }
}

#[derive(Debug, Error)]
pub enum CoordinationExchangeError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Recipients(#[from] RecipientResolutionError),
    #[error("coordination request event was not found: {0}")]
    RequestNotFound(String),
    #[error("coordination exchange was not found: {0}")]
    ExchangeNotFound(String),
    #[error("coordination exchange belongs to a different goal")]
    GoalMismatch,
    #[error("worker is not authorized for this coordination exchange")]
    Unauthorized,
    #[error("request did not target the selected active worker")]
    TargetMismatch,
    #[error("targeted notification must be delivered or acknowledged before direct exchange")]
    NotificationNotAttempted,
    #[error("coordination exchange changed concurrently")]
    StaleRevision,
    #[error("coordination exchange TTL must be between one and fifteen minutes")]
    InvalidTtl,
}

#[derive(Debug, Clone)]
pub struct CoordinationExchangeService {
    store: Arc<SqliteCoordinationStore>,
    recipients: NotificationRecipientResolver,
}

impl CoordinationExchangeService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            recipients: NotificationRecipientResolver::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn open(
        &self,
        goal_id: &str,
        request_event_id: &EventId,
        requester_worker_id: &WorkerId,
        target_worker_id: &WorkerId,
        insufficiency_reason: impl Into<String>,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<CoordinationExchange, CoordinationExchangeError> {
        if ttl < Duration::minutes(1) || ttl > Duration::minutes(15) {
            return Err(CoordinationExchangeError::InvalidTtl);
        }
        let request_event = self.store.event(request_event_id)?.ok_or_else(|| {
            CoordinationExchangeError::RequestNotFound(request_event_id.as_str().into())
        })?;
        if request_event.goal_id != goal_id {
            return Err(CoordinationExchangeError::GoalMismatch);
        }
        let Some(CoordinationEventPayload::CoordinationRequest(request)) =
            request_event.typed_payload()?
        else {
            return Err(CoordinationExchangeError::TargetMismatch);
        };
        if request.requester_worker_id != *requester_worker_id
            || !matches!(
                &request_event.producer,
                CoordinationActor::Worker { worker_id } if worker_id == requester_worker_id
            )
        {
            return Err(CoordinationExchangeError::Unauthorized);
        }
        let resolution = self.recipients.resolve_event(&request_event)?;
        if !resolution
            .recipients
            .iter()
            .any(|recipient| recipient.worker_id == *target_worker_id)
        {
            return Err(CoordinationExchangeError::TargetMismatch);
        }
        let attempted = self
            .store
            .notifications_for_worker(target_worker_id, None)?
            .into_iter()
            .any(|notification| {
                (notification.source_event_id == request_event.id
                    || notification
                        .coalesced_source_event_ids
                        .contains(&request_event.id))
                    && matches!(
                        notification.state,
                        NotificationState::Delivered | NotificationState::Acknowledged
                    )
            });
        if !attempted {
            return Err(CoordinationExchangeError::NotificationNotAttempted);
        }
        if let Some(existing) = self
            .store
            .exchanges_for_goal(goal_id, Some(CoordinationExchangeState::Open))?
            .into_iter()
            .find(|exchange| {
                exchange.request_event_id == request_event.id
                    && exchange.target_worker_id == *target_worker_id
            })
        {
            return Ok(existing);
        }
        let exchange = CoordinationExchange::new(
            goal_id,
            request_event.id,
            request.target_scope,
            request.desired_outcome,
            requester_worker_id.clone(),
            target_worker_id.clone(),
            insufficiency_reason,
            now + ttl,
            4,
            now,
        )?;
        self.store.insert_exchange(&exchange)?;
        self.publish_exchange_event(
            &exchange,
            CoordinationActor::Worker {
                worker_id: requester_worker_id.clone(),
            },
            "exchange_opened",
            now,
        )?;
        Ok(exchange)
    }

    pub fn post_message(
        &self,
        goal_id: &str,
        exchange_id: &ExchangeId,
        author_worker_id: &WorkerId,
        body: impl Into<String>,
        evidence_refs: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<CoordinationExchange, CoordinationExchangeError> {
        let mut exchange = self.load(goal_id, exchange_id)?;
        if !exchange.is_participant(author_worker_id) {
            return Err(CoordinationExchangeError::Unauthorized);
        }
        let expected_updated_at = exchange.metadata.updated_at;
        exchange.add_message(
            author_worker_id.clone(),
            body,
            evidence_refs,
            next_notification_time(now, expected_updated_at),
        )?;
        self.persist(&exchange, expected_updated_at)?;
        self.publish_exchange_event(
            &exchange,
            CoordinationActor::Worker {
                worker_id: author_worker_id.clone(),
            },
            "exchange_message_added",
            exchange.metadata.updated_at,
        )?;
        Ok(exchange)
    }

    pub fn complete(
        &self,
        goal_id: &str,
        exchange_id: &ExchangeId,
        actor_worker_id: &WorkerId,
        shared_note: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<CoordinationExchange, CoordinationExchangeError> {
        let mut exchange = self.load(goal_id, exchange_id)?;
        if !exchange.is_participant(actor_worker_id) {
            return Err(CoordinationExchangeError::Unauthorized);
        }
        let expected_updated_at = exchange.metadata.updated_at;
        exchange.complete(
            actor_worker_id.clone(),
            shared_note,
            next_notification_time(now, expected_updated_at),
        )?;
        self.persist(&exchange, expected_updated_at)?;
        self.publish_exchange_event(
            &exchange,
            CoordinationActor::Worker {
                worker_id: actor_worker_id.clone(),
            },
            "exchange_completed",
            exchange.metadata.updated_at,
        )?;
        Ok(exchange)
    }

    pub fn expire_due(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<CoordinationExchange>, CoordinationExchangeError> {
        let mut expired = Vec::new();
        for mut exchange in self
            .store
            .exchanges_for_goal(goal_id, Some(CoordinationExchangeState::Open))?
        {
            if exchange.expires_at > now {
                continue;
            }
            let expected_updated_at = exchange.metadata.updated_at;
            exchange.expire(next_notification_time(now, expected_updated_at))?;
            self.persist(&exchange, expected_updated_at)?;
            self.publish_exchange_event(
                &exchange,
                CoordinationActor::System,
                "exchange_expired",
                exchange.metadata.updated_at,
            )?;
            expired.push(exchange);
        }
        Ok(expired)
    }

    fn load(
        &self,
        goal_id: &str,
        exchange_id: &ExchangeId,
    ) -> Result<CoordinationExchange, CoordinationExchangeError> {
        let exchange = self.store.exchange(exchange_id)?.ok_or_else(|| {
            CoordinationExchangeError::ExchangeNotFound(exchange_id.as_str().into())
        })?;
        if exchange.goal_id != goal_id {
            return Err(CoordinationExchangeError::GoalMismatch);
        }
        Ok(exchange)
    }

    fn persist(
        &self,
        exchange: &CoordinationExchange,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<(), CoordinationExchangeError> {
        if !self
            .store
            .update_exchange_if_revision(exchange, expected_updated_at)?
        {
            return Err(CoordinationExchangeError::StaleRevision);
        }
        Ok(())
    }

    fn publish_exchange_event(
        &self,
        exchange: &CoordinationExchange,
        producer: CoordinationActor,
        category: &str,
        now: DateTime<Utc>,
    ) -> Result<(), CoordinationExchangeError> {
        let event = CoordinationEvent::new(
            &exchange.goal_id,
            CoordinationEventKind::CoordinationExchangeChanged,
            EventSeverity::Info,
            producer,
            exchange.id.as_str(),
            serde_json::json!({
                "category": category,
                "exchangeId": exchange.id.as_str(),
                "requestEventId": exchange.request_event_id.as_str(),
                "state": exchange.state,
                "messageCount": exchange.messages.len(),
                "expiresAt": exchange.expires_at,
                "sharedNoteRecorded": exchange.shared_note.is_some(),
            }),
            now,
        );
        self.store.append_event(&event)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NotificationLifecycleError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("notification was not found: {0}")]
    NotFound(String),
    #[error("notification belongs to a different goal")]
    GoalMismatch,
    #[error("notification lifecycle update used a stale revision")]
    StaleRevision,
    #[error("notification transition time must advance its durable revision")]
    NonMonotonicTimestamp,
    #[error("only the target worker can acknowledge or act on a notification")]
    WrongWorker,
    #[error("notification requires acknowledgement before it can be acted on")]
    AcknowledgementRequired,
    #[error("notification actor or transition detail is invalid: {0}")]
    InvalidAttribution(String),
    #[error("notification has not reached its expiry deadline")]
    NotDueForExpiry,
    #[error("replacement notification must be queued for the same goal and target worker")]
    InvalidReplacement,
    #[error("notification expired before delivery")]
    ExpiredBeforeDelivery,
}

#[derive(Debug, Clone)]
pub struct NotificationLifecycleService {
    store: Arc<SqliteCoordinationStore>,
}

impl NotificationLifecycleService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn mark_delivered(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        actor: CoordinationActor,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        validate_actor(&actor)?;
        if notification
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(NotificationLifecycleError::ExpiredBeforeDelivery);
        }
        notification.transition_attributed(
            NotificationState::Delivered,
            actor,
            Some("notification delivered at a safe turn boundary".into()),
            now,
        )?;
        self.persist(notification, expected_updated_at)
    }

    pub fn acknowledge(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        worker_id: &WorkerId,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        ensure_target_worker(&notification, worker_id)?;
        notification.transition_attributed(
            NotificationState::Acknowledged,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            Some("target worker acknowledged notification".into()),
            now,
        )?;
        self.persist(notification, expected_updated_at)
    }

    pub fn mark_acted_on(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        worker_id: &WorkerId,
        outcome: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        ensure_target_worker(&notification, worker_id)?;
        if notification.required_acknowledgement
            && notification.state != NotificationState::Acknowledged
        {
            return Err(NotificationLifecycleError::AcknowledgementRequired);
        }
        let outcome = outcome.into();
        validate_detail(&outcome)?;
        notification.transition_attributed(
            NotificationState::ActedOn,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            Some(outcome),
            now,
        )?;
        self.persist(notification, expected_updated_at)
    }

    pub fn mark_failed(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        actor: CoordinationActor,
        reason: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        validate_actor(&actor)?;
        let reason = reason.into();
        validate_detail(&reason)?;
        notification.transition_attributed(NotificationState::Failed, actor, Some(reason), now)?;
        self.persist(notification, expected_updated_at)
    }

    pub fn mark_expired(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        actor: CoordinationActor,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        validate_actor(&actor)?;
        if !notification
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(NotificationLifecycleError::NotDueForExpiry);
        }
        notification.transition_attributed(
            NotificationState::Expired,
            actor,
            Some("notification expiry deadline elapsed".into()),
            now,
        )?;
        self.persist(notification, expected_updated_at)
    }

    pub fn supersede(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        replacement_id: &NotificationId,
        actor: CoordinationActor,
        reason: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        validate_actor(&actor)?;
        let reason = reason.into();
        validate_detail(&reason)?;
        let replacement = self
            .store
            .notification(replacement_id)?
            .ok_or_else(|| NotificationLifecycleError::NotFound(replacement_id.as_str().into()))?;
        if replacement.id == notification.id
            || replacement.goal_id != notification.goal_id
            || replacement.target_worker != notification.target_worker
            || replacement.state != NotificationState::Queued
        {
            return Err(NotificationLifecycleError::InvalidReplacement);
        }
        notification.supersede_attributed(replacement.id, actor, Some(reason), now)?;
        self.persist(notification, expected_updated_at)
    }

    pub fn record_replayed(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        actor: CoordinationActor,
        recovery_kind: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let mut notification =
            self.load_current(goal_id, notification_id, expected_updated_at, now)?;
        validate_actor(&actor)?;
        let recovery_kind = recovery_kind.into();
        if recovery_kind.trim().is_empty() || recovery_kind.chars().count() > 1_900 {
            return Err(NotificationLifecycleError::InvalidAttribution(
                "recovery kind must be non-empty and at most 1,900 characters".into(),
            ));
        }
        notification.record_replay_attributed(
            actor,
            Some(format!("notification replayed after {recovery_kind}")),
            now,
        )?;
        self.persist(notification, expected_updated_at)
    }

    fn load_current(
        &self,
        goal_id: &str,
        notification_id: &NotificationId,
        expected_updated_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        let notification = self
            .store
            .notification(notification_id)?
            .ok_or_else(|| NotificationLifecycleError::NotFound(notification_id.as_str().into()))?;
        if notification.goal_id != goal_id {
            return Err(NotificationLifecycleError::GoalMismatch);
        }
        if notification.metadata.updated_at != expected_updated_at {
            return Err(NotificationLifecycleError::StaleRevision);
        }
        if now <= notification.metadata.updated_at {
            return Err(NotificationLifecycleError::NonMonotonicTimestamp);
        }
        Ok(notification)
    }

    fn persist(
        &self,
        notification: WorkerNotification,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<WorkerNotification, NotificationLifecycleError> {
        if !self
            .store
            .update_notification_if_revision(&notification, expected_updated_at)?
        {
            return Err(NotificationLifecycleError::StaleRevision);
        }
        Ok(notification)
    }
}

fn ensure_target_worker(
    notification: &WorkerNotification,
    worker_id: &WorkerId,
) -> Result<(), NotificationLifecycleError> {
    if notification.target_worker != *worker_id {
        return Err(NotificationLifecycleError::WrongWorker);
    }
    Ok(())
}

fn validate_actor(actor: &CoordinationActor) -> Result<(), NotificationLifecycleError> {
    match actor {
        CoordinationActor::User { user_id } if user_id.trim().is_empty() || user_id.len() > 200 => {
            Err(NotificationLifecycleError::InvalidAttribution(
                "user ID must be non-empty and at most 200 bytes".into(),
            ))
        }
        _ => Ok(()),
    }
}

fn validate_detail(detail: &str) -> Result<(), NotificationLifecycleError> {
    if detail.trim().is_empty() || detail.chars().count() > 2_000 {
        return Err(NotificationLifecycleError::InvalidAttribution(
            "detail must be non-empty and at most 2,000 characters".into(),
        ));
    }
    Ok(())
}

struct ResolutionContext {
    goal_id: String,
    workers: BTreeMap<String, Worker>,
    claims: Vec<Claim>,
    packages: Vec<WorkPackage>,
    contracts: Vec<SharedContract>,
    reasons: BTreeMap<String, BTreeSet<RecipientReason>>,
}

impl ResolutionContext {
    fn load(
        store: &SqliteCoordinationStore,
        goal_id: &str,
    ) -> Result<Self, RecipientResolutionError> {
        let workers = store
            .workers_for_goal(goal_id, None)?
            .into_iter()
            .filter(|worker| !worker.state.is_terminal())
            .map(|worker| (worker.id.as_str().to_string(), worker))
            .collect();
        Ok(Self {
            goal_id: goal_id.into(),
            workers,
            claims: store.claims_for_goal(goal_id, None)?,
            packages: store.work_packages_for_goal(goal_id, None)?,
            contracts: store.contracts_for_goal(goal_id)?,
            reasons: BTreeMap::new(),
        })
    }

    fn is_eligible(&self, worker_id: &WorkerId) -> bool {
        self.workers.contains_key(worker_id.as_str())
    }

    fn add_worker(&mut self, worker_id: &WorkerId, reason: RecipientReason) {
        if self.is_eligible(worker_id) {
            self.reasons
                .entry(worker_id.as_str().to_string())
                .or_default()
                .insert(reason);
        }
    }

    fn remove_worker(&mut self, worker_id: &WorkerId) {
        self.reasons.remove(worker_id.as_str());
    }

    fn add_claim(&mut self, claim_id: &ClaimId, reason: RecipientReason, dependents: bool) {
        let Some(claim) = self
            .claims
            .iter()
            .find(|claim| claim.id == *claim_id)
            .cloned()
        else {
            return;
        };
        self.add_worker(&claim.owner, reason);
        if dependents && let ClaimScope::WorkPackage { work_package_id } = claim.scope {
            self.add_package_with_dependents(&work_package_id);
        }
    }

    fn add_package_owners(&mut self, package_id: &WorkPackageId, reason: RecipientReason) {
        let owners = self
            .claims
            .iter()
            .filter(|claim| {
                matches!(claim.state, ClaimState::Active | ClaimState::Blocked)
                    && matches!(
                        &claim.scope,
                        ClaimScope::WorkPackage { work_package_id }
                            if work_package_id == package_id
                    )
            })
            .map(|claim| claim.owner.clone())
            .collect::<Vec<_>>();
        for owner in owners {
            self.add_worker(&owner, reason);
        }
    }

    fn add_package_with_dependents(&mut self, package_id: &WorkPackageId) {
        self.add_package_owners(package_id, RecipientReason::WorkPackageOwner);
        self.add_package_dependents(package_id);
    }

    fn add_package_dependents(&mut self, package_id: &WorkPackageId) {
        let mut queue = VecDeque::from([package_id.clone()]);
        let mut visited = BTreeSet::from([package_id.as_str().to_string()]);
        while let Some(current) = queue.pop_front() {
            let dependents = self
                .packages
                .iter()
                .filter(|package| package.depends_on.contains(&current))
                .map(|package| package.id.clone())
                .collect::<Vec<_>>();
            for dependent in dependents {
                if visited.insert(dependent.as_str().to_string()) {
                    self.add_package_owners(&dependent, RecipientReason::DependentPackage);
                    queue.push_back(dependent);
                }
            }
        }
    }

    fn add_contract_consumers(
        &mut self,
        contract_id: Option<&ContractId>,
        stable_key: Option<&str>,
    ) {
        let mut package_ids = BTreeSet::new();
        for contract in &self.contracts {
            if contract_id.is_some_and(|id| id == &contract.id)
                || stable_key.is_some_and(|key| key == contract.stable_key)
            {
                package_ids.extend(
                    contract
                        .dependents
                        .iter()
                        .map(|consumer| consumer.work_package_id.as_str().to_string()),
                );
            }
        }
        let contract_keys = [
            contract_id.map(|id| id.as_str().to_string()),
            stable_key.map(str::to_string),
        ]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
        package_ids.extend(self.packages.iter().filter_map(|package| {
            package
                .consumes_contracts
                .iter()
                .any(|expectation| contract_keys.contains(&expectation.contract_id))
                .then(|| package.id.as_str().to_string())
        }));
        let direct_consumers = self
            .claims
            .iter()
            .filter(|claim| {
                matches!(claim.state, ClaimState::Active | ClaimState::Blocked)
                    && claim
                        .consumes_contracts
                        .iter()
                        .any(|expectation| contract_keys.contains(&expectation.contract_id))
            })
            .map(|claim| claim.owner.clone())
            .collect::<Vec<_>>();
        for worker_id in direct_consumers {
            self.add_worker(&worker_id, RecipientReason::ContractConsumer);
        }
        for package_id in package_ids {
            if let Ok(package_id) = WorkPackageId::parse(package_id) {
                self.add_package_owners(&package_id, RecipientReason::ContractConsumer);
                self.add_package_dependents(&package_id);
            }
        }
    }

    fn add_escalation_scope(&mut self, escalation: &HumanEscalation) {
        if escalation.goal_id != self.goal_id {
            return;
        }
        match &escalation.scope {
            EscalationScope::Claim { claim_id } => {
                self.add_claim(claim_id, RecipientReason::EscalationScope, false);
            }
            EscalationScope::WorkPackage { work_package_id } => {
                self.add_package_owners(work_package_id, RecipientReason::EscalationScope);
            }
            EscalationScope::Contract { contract_id } => {
                self.add_contract_consumers(Some(contract_id), None);
            }
            EscalationScope::IntegrationLane { repository_id } => {
                let worker_ids = self
                    .workers
                    .values()
                    .filter(|worker| {
                        worker
                            .workspace
                            .as_ref()
                            .is_some_and(|workspace| workspace.repository_id == *repository_id)
                    })
                    .map(|worker| worker.id.clone())
                    .collect::<Vec<_>>();
                for worker_id in worker_ids {
                    self.add_worker(&worker_id, RecipientReason::EscalationScope);
                }
            }
            EscalationScope::Goal { goal_id } if goal_id == &self.goal_id => {
                let worker_ids = self
                    .workers
                    .values()
                    .map(|worker| worker.id.clone())
                    .collect::<Vec<_>>();
                for worker_id in worker_ids {
                    self.add_worker(&worker_id, RecipientReason::EscalationScope);
                }
            }
            EscalationScope::Goal { .. } => {}
        }
    }

    fn add_coordination_target(&mut self, target_scope: &str) {
        if let Some(value) = target_scope.strip_prefix("worker:") {
            if let Ok(worker_id) = WorkerId::parse(value) {
                self.add_worker(&worker_id, RecipientReason::CoordinationTarget);
            }
        } else if let Some(value) = target_scope.strip_prefix("claim:") {
            if let Ok(claim_id) = ClaimId::parse(value) {
                self.add_claim(&claim_id, RecipientReason::CoordinationTarget, false);
            }
        } else if let Some(value) = target_scope.strip_prefix("package:") {
            if let Ok(package_id) = WorkPackageId::parse(value) {
                self.add_package_owners(&package_id, RecipientReason::CoordinationTarget);
            }
        } else if let Some(value) = target_scope.strip_prefix("contract:") {
            self.add_contract_consumers(None, Some(value));
        }
    }

    fn finish(
        self,
        source_event_id: Option<EventId>,
        source_intervention_id: Option<InterventionId>,
    ) -> RecipientResolution {
        let recipients = self
            .reasons
            .into_iter()
            .filter_map(|(worker_key, reasons)| {
                self.workers
                    .get(&worker_key)
                    .map(|worker| NotificationRecipient {
                        worker_id: worker.id.clone(),
                        reasons: reasons.into_iter().collect(),
                    })
            })
            .collect();
        RecipientResolution {
            goal_id: self.goal_id,
            source_event_id,
            source_intervention_id,
            recipients,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        ClaimEventPayload, ContractConsumer, ContractEventPayload, ContractExpectation,
        ContractKind, EventSeverity, FileOverlapEventPayload, FileOverlapInput,
        HumanDecisionEventPayload, InterventionLevel, RemediationAction, WorkPackageState,
        WorkerState,
    };
    use chrono::{Duration, Utc};

    fn active_worker(goal_id: &str, now: chrono::DateTime<Utc>) -> Worker {
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker
    }

    fn claimed_package(
        goal_id: &str,
        title: &str,
        feature: &str,
        now: chrono::DateTime<Utc>,
    ) -> WorkPackage {
        let mut package = WorkPackage::new(goal_id, title, vec![feature.into()], now).unwrap();
        package.transition(WorkPackageState::Planned, now).unwrap();
        package.transition(WorkPackageState::Ready, now).unwrap();
        package.transition(WorkPackageState::Claimed, now).unwrap();
        package
    }

    fn package_claim(
        package: &WorkPackage,
        worker: &mut Worker,
        now: chrono::DateTime<Utc>,
    ) -> Claim {
        let claim = Claim::new(
            &package.goal_id,
            ClaimScope::WorkPackage {
                work_package_id: package.id.clone(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        claim
    }

    fn recipient_ids(resolution: &RecipientResolution) -> Vec<String> {
        resolution
            .recipients
            .iter()
            .map(|recipient| recipient.worker_id.as_str().to_string())
            .collect()
    }

    fn sorted_worker_ids(workers: &[&Worker]) -> Vec<String> {
        let mut ids = workers
            .iter()
            .map(|worker| worker.id.as_str().to_string())
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    #[test]
    fn resolves_package_and_contract_dependencies_without_broadcasting() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let resolver = NotificationRecipientResolver::new(store.clone());
        let now = Utc::now();
        let mut producer_worker = active_worker("goal-a", now);
        let mut consumer_worker = active_worker("goal-a", now);
        let mut downstream_worker = active_worker("goal-a", now);
        let mut unrelated_worker = active_worker("goal-a", now);
        let producer = claimed_package("goal-a", "Producer", "producer", now);
        let mut consumer = claimed_package("goal-a", "Consumer", "consumer", now);
        consumer.depends_on = vec![producer.id.clone()];
        let mut downstream = claimed_package("goal-a", "Downstream", "downstream", now);
        downstream.depends_on = vec![consumer.id.clone()];
        let unrelated = claimed_package("goal-a", "Unrelated", "unrelated", now);
        let producer_claim = package_claim(&producer, &mut producer_worker, now);
        let mut consumer_claim = package_claim(&consumer, &mut consumer_worker, now);
        let downstream_claim = package_claim(&downstream, &mut downstream_worker, now);
        let unrelated_claim = package_claim(&unrelated, &mut unrelated_worker, now);
        let mut contract =
            SharedContract::new("goal-a", "api.shared", "Shared API", ContractKind::Api, now);
        contract.producer = Some(producer.id.clone());
        contract.dependents = vec![ContractConsumer {
            work_package_id: consumer.id.clone(),
            expected_revision: 1,
        }];
        consumer.consumes_contracts = vec![ContractExpectation {
            contract_id: contract.stable_key.clone(),
            expected_revision: 1,
        }];
        consumer_claim.consumes_contracts = consumer.consumes_contracts.clone();
        for worker in [
            &producer_worker,
            &consumer_worker,
            &downstream_worker,
            &unrelated_worker,
        ] {
            store.upsert_worker(worker).unwrap();
        }
        for package in [&producer, &consumer, &downstream, &unrelated] {
            store.upsert_work_package(package).unwrap();
        }
        for claim in [
            &producer_claim,
            &consumer_claim,
            &downstream_claim,
            &unrelated_claim,
        ] {
            store.insert_claim(claim).unwrap();
        }
        store.upsert_contract(&contract).unwrap();

        let contract_event = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Warning,
            CoordinationActor::Worker {
                worker_id: producer_worker.id.clone(),
            },
            contract.id.as_str(),
            CoordinationEventPayload::Contract(ContractEventPayload {
                contract_id: contract.id.clone(),
                stable_key: contract.stable_key.clone(),
                previous_revision: 1,
                revision: 2,
                producer_worker_id: Some(producer_worker.id.clone()),
                compatibility_notes: "Response field changed".into(),
            }),
            now,
        )
        .unwrap();
        let contract_recipients = resolver.resolve_event(&contract_event).unwrap();
        assert_eq!(
            recipient_ids(&contract_recipients),
            sorted_worker_ids(&[&consumer_worker, &downstream_worker])
        );
        assert!(
            contract_recipients
                .recipients
                .iter()
                .find(|recipient| recipient.worker_id == consumer_worker.id)
                .unwrap()
                .reasons
                .contains(&RecipientReason::ContractConsumer)
        );
        assert!(
            contract_recipients
                .recipients
                .iter()
                .find(|recipient| recipient.worker_id == downstream_worker.id)
                .unwrap()
                .reasons
                .contains(&RecipientReason::DependentPackage)
        );

        let dependency_event = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::System,
            producer.id.as_str(),
            CoordinationEventPayload::Dependency(super::super::domain::DependencyEventPayload {
                work_package_id: producer.id.clone(),
                dependency_key: "producer-output".into(),
                previous_revision: "base-a".into(),
                revision: "head-a".into(),
                affected_worker_ids: vec![],
            }),
            now,
        )
        .unwrap();
        assert_eq!(
            recipient_ids(&resolver.resolve_event(&dependency_event).unwrap()),
            sorted_worker_ids(&[&consumer_worker, &downstream_worker, &producer_worker])
        );

        let completion_event = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: producer_worker.id.clone(),
            },
            producer_claim.id.as_str(),
            CoordinationEventPayload::Claim(ClaimEventPayload {
                claim_id: producer_claim.id.clone(),
                worker_id: producer_worker.id.clone(),
                from_state: Some(ClaimState::Active),
                to_state: ClaimState::Completed,
                reason: "Producer completed".into(),
                artifact_id: Some("artifact-a".into()),
                evidence_refs: vec!["test:producer".into()],
            }),
            now,
        )
        .unwrap();
        assert_eq!(
            recipient_ids(&resolver.resolve_event(&completion_event).unwrap()),
            sorted_worker_ids(&[&consumer_worker, &downstream_worker])
        );
    }

    #[test]
    fn resolves_overlap_escalation_and_explicit_supervisor_targets() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let resolver = NotificationRecipientResolver::new(store.clone());
        let now = Utc::now();
        let mut left_worker = active_worker("goal-a", now);
        let mut right_worker = active_worker("goal-a", now);
        let left_package = claimed_package("goal-a", "Left", "left", now);
        let right_package = claimed_package("goal-a", "Right", "right", now);
        let left_claim = package_claim(&left_package, &mut left_worker, now);
        let right_claim = package_claim(&right_package, &mut right_worker, now);
        for worker in [&left_worker, &right_worker] {
            store.upsert_worker(worker).unwrap();
        }
        for package in [&left_package, &right_package] {
            store.upsert_work_package(package).unwrap();
        }
        for claim in [&left_claim, &right_claim] {
            store.insert_claim(claim).unwrap();
        }

        let overlap = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Warning,
            CoordinationActor::System,
            "overlap-a",
            CoordinationEventPayload::FileOverlap(FileOverlapEventPayload {
                left_worker_id: left_worker.id.clone(),
                left_claim_id: left_claim.id.clone(),
                left_base_revision: "base-a".into(),
                left_head_revision: "left-head".into(),
                right_worker_id: right_worker.id.clone(),
                right_claim_id: right_claim.id.clone(),
                right_base_revision: "base-a".into(),
                right_head_revision: "right-head".into(),
                paths: vec![FileOverlapInput {
                    path: "src/shared.rs".into(),
                    left_status: "modified".into(),
                    right_status: "modified".into(),
                    migration: false,
                    dependency_file: false,
                    generated_artifact: false,
                }],
            }),
            now,
        )
        .unwrap();
        assert_eq!(
            recipient_ids(&resolver.resolve_event(&overlap).unwrap()),
            sorted_worker_ids(&[&left_worker, &right_worker])
        );

        let escalation = HumanEscalation::new(
            "goal-a",
            super::super::domain::EscalationKind::DecisionPoint,
            super::super::domain::EscalationSeverity::High,
            EscalationScope::WorkPackage {
                work_package_id: right_package.id.clone(),
            },
            "Choose the right package direction",
            now,
        );
        store.upsert_escalation(&escalation).unwrap();
        let decision_event = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::User {
                user_id: "user-a".into(),
            },
            escalation.id.as_str(),
            CoordinationEventPayload::HumanDecision(HumanDecisionEventPayload {
                escalation_id: escalation.id.clone(),
                decided_by: "user-a".into(),
                option_id: "choose-right".into(),
                action: RemediationAction::ChooseDirection,
                accepted_risk: None,
            }),
            now,
        )
        .unwrap();
        assert_eq!(
            recipient_ids(&resolver.resolve_event(&decision_event).unwrap()),
            vec![right_worker.id.as_str().to_string()]
        );

        let intervention = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Recommend,
            vec![overlap.id.clone()],
            vec![left_worker.id.clone()],
            "Coordinate the shared file",
            now,
        );
        let resolved = resolver.resolve_intervention(&intervention).unwrap();
        assert_eq!(
            recipient_ids(&resolved),
            vec![left_worker.id.as_str().to_string()]
        );
        assert_eq!(
            resolved.recipients[0].reasons,
            vec![RecipientReason::SupervisorTarget]
        );

        let invalid = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Recommend,
            vec![overlap.id],
            vec![WorkerId::new()],
            "Invented target",
            now,
        );
        assert!(matches!(
            resolver.resolve_intervention(&invalid),
            Err(RecipientResolutionError::InvalidSupervisorTarget(_))
        ));
    }

    fn queued_notification(
        worker_id: WorkerId,
        now: chrono::DateTime<Utc>,
        expires_at: Option<chrono::DateTime<Utc>>,
    ) -> WorkerNotification {
        WorkerNotification::new(
            "goal-a",
            worker_id,
            EventId::new(),
            "dependency_changed",
            "A required dependency changed.",
            EventSeverity::Warning,
            vec!["package:producer".into()],
            Some("Refresh durable context before continuing.".into()),
            true,
            expires_at,
            now,
        )
        .unwrap()
    }

    #[test]
    fn persists_delivery_acknowledgement_and_action_audit_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let service = NotificationLifecycleService::new(store.clone());
        let worker_id = WorkerId::new();
        let now = Utc::now();
        let queued = queued_notification(worker_id.clone(), now, None);
        store.upsert_notification(&queued).unwrap();

        let delivered = service
            .mark_delivered(
                "goal-a",
                &queued.id,
                queued.metadata.updated_at,
                CoordinationActor::System,
                now + Duration::seconds(1),
            )
            .unwrap();
        let acknowledged = service
            .acknowledge(
                "goal-a",
                &queued.id,
                delivered.metadata.updated_at,
                &worker_id,
                now + Duration::seconds(2),
            )
            .unwrap();
        let acted_on = service
            .mark_acted_on(
                "goal-a",
                &queued.id,
                acknowledged.metadata.updated_at,
                &worker_id,
                "Refreshed dependency context and revalidated assumptions.",
                now + Duration::seconds(3),
            )
            .unwrap();

        assert_eq!(acted_on.state, NotificationState::ActedOn);
        assert_eq!(acted_on.delivery_attempts, 1);
        assert_eq!(acted_on.audit_history.len(), 4);
        assert_eq!(
            acted_on
                .audit_history
                .iter()
                .map(|entry| entry.state)
                .collect::<Vec<_>>(),
            vec![
                NotificationState::Queued,
                NotificationState::Delivered,
                NotificationState::Acknowledged,
                NotificationState::ActedOn,
            ]
        );
        assert!(matches!(
            &acted_on.audit_history[2].actor,
            CoordinationActor::Worker { worker_id: actor } if actor == &worker_id
        ));

        drop(service);
        drop(store);
        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        let restored = reopened.notification(&queued.id).unwrap().unwrap();
        assert_eq!(restored, acted_on);
        assert_eq!(
            reopened
                .notifications_for_worker(&worker_id, Some(NotificationState::ActedOn))
                .unwrap(),
            vec![acted_on]
        );
    }

    #[test]
    fn persists_expired_superseded_and_failed_terminal_delivery_states() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let service = NotificationLifecycleService::new(store.clone());
        let worker_id = WorkerId::new();
        let now = Utc::now();

        let expired_source =
            queued_notification(worker_id.clone(), now, Some(now + Duration::seconds(1)));
        let failed_source = queued_notification(worker_id.clone(), now, None);
        let superseded_source = queued_notification(worker_id.clone(), now, None);
        let replacement =
            queued_notification(worker_id.clone(), now + Duration::milliseconds(1), None);
        for notification in [
            &expired_source,
            &failed_source,
            &superseded_source,
            &replacement,
        ] {
            store.upsert_notification(notification).unwrap();
        }

        let expired = service
            .mark_expired(
                "goal-a",
                &expired_source.id,
                expired_source.metadata.updated_at,
                CoordinationActor::System,
                now + Duration::seconds(2),
            )
            .unwrap();
        let failed = service
            .mark_failed(
                "goal-a",
                &failed_source.id,
                failed_source.metadata.updated_at,
                CoordinationActor::System,
                "Worker thread was unavailable at delivery.",
                now + Duration::seconds(1),
            )
            .unwrap();
        let superseded = service
            .supersede(
                "goal-a",
                &superseded_source.id,
                superseded_source.metadata.updated_at,
                &replacement.id,
                CoordinationActor::Supervisor,
                "A newer dependency revision replaced this context.",
                now + Duration::seconds(1),
            )
            .unwrap();

        assert_eq!(expired.state, NotificationState::Expired);
        assert_eq!(failed.state, NotificationState::Failed);
        assert_eq!(failed.delivery_attempts, 1);
        assert_eq!(superseded.state, NotificationState::Superseded);
        assert_eq!(superseded.superseded_by, Some(replacement.id.clone()));
        assert_eq!(
            store.notification(&replacement.id).unwrap().unwrap().state,
            NotificationState::Queued
        );

        drop(service);
        drop(store);
        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        for expected in [&expired, &failed, &superseded, &replacement] {
            assert_eq!(
                reopened.notification(&expected.id).unwrap().as_ref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn rejects_stale_delivery_updates_and_wrong_worker_acknowledgement() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = NotificationLifecycleService::new(store.clone());
        let worker_id = WorkerId::new();
        let now = Utc::now();
        let queued = queued_notification(worker_id.clone(), now, None);
        store.upsert_notification(&queued).unwrap();
        let delivered = service
            .mark_delivered(
                "goal-a",
                &queued.id,
                queued.metadata.updated_at,
                CoordinationActor::System,
                now + Duration::seconds(1),
            )
            .unwrap();

        assert!(matches!(
            service.mark_failed(
                "goal-a",
                &queued.id,
                queued.metadata.updated_at,
                CoordinationActor::System,
                "stale failure",
                now + Duration::seconds(2),
            ),
            Err(NotificationLifecycleError::StaleRevision)
        ));
        assert!(matches!(
            service.acknowledge(
                "goal-a",
                &queued.id,
                delivered.metadata.updated_at,
                &WorkerId::new(),
                now + Duration::seconds(2),
            ),
            Err(NotificationLifecycleError::WrongWorker)
        ));
        assert_eq!(
            store.notification(&queued.id).unwrap().unwrap().state,
            NotificationState::Delivered
        );
    }

    fn notification_event(
        store: &SqliteCoordinationStore,
        severity: EventSeverity,
        summary: &str,
        now: chrono::DateTime<Utc>,
    ) -> CoordinationEvent {
        let event = CoordinationEvent::new(
            "goal-a",
            super::super::domain::CoordinationEventKind::DependencyChanged,
            severity,
            CoordinationActor::System,
            "package:producer",
            serde_json::json!({"summary": summary}),
            now,
        );
        store.append_event(&event).unwrap();
        event
    }

    fn notification_candidate(
        worker_id: WorkerId,
        event: &CoordinationEvent,
        purpose: &str,
        summary: &str,
    ) -> NotificationCandidate {
        NotificationCandidate {
            goal_id: "goal-a".into(),
            target_worker: worker_id,
            source_event_id: event.id.clone(),
            purpose: purpose.into(),
            summary: summary.into(),
            severity: event.severity,
            evidence_refs: vec![format!("event:{}", event.id.as_str())],
            recommended_action: Some("Refresh dependency assumptions.".into()),
            required_acknowledgement: false,
            expires_at: None,
        }
    }

    #[test]
    fn suppresses_irrelevant_unchanged_and_over_capacity_noise_and_coalesces_related_updates() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let now = Utc::now();
        let worker = active_worker("goal-a", now);
        store.upsert_worker(&worker).unwrap();
        let policy = NotificationNoisePolicy {
            max_pending_per_worker: 1,
            ..NotificationNoisePolicy::default()
        };
        let queue = NotificationQueueService::new(store.clone(), policy).unwrap();

        let debug_event = notification_event(&store, EventSeverity::Debug, "debug", now);
        let irrelevant = queue
            .enqueue(
                notification_candidate(
                    worker.id.clone(),
                    &debug_event,
                    "dependency_changed",
                    "Debug-only dependency observation.",
                ),
                now,
            )
            .unwrap();
        assert_eq!(
            irrelevant,
            NotificationEnqueueOutcome::Suppressed {
                reason: NotificationSuppressionReason::BelowRelevanceThreshold,
                existing_notification_id: None,
            }
        );

        let first_event = notification_event(
            &store,
            EventSeverity::Warning,
            "revision one",
            now + Duration::seconds(1),
        );
        let first = match queue
            .enqueue(
                notification_candidate(
                    worker.id.clone(),
                    &first_event,
                    "dependency_changed",
                    "Producer moved to revision one.",
                ),
                now + Duration::seconds(1),
            )
            .unwrap()
        {
            NotificationEnqueueOutcome::Queued(notification) => notification,
            other => panic!("unexpected first enqueue outcome: {other:?}"),
        };

        let duplicate_event = notification_event(
            &store,
            EventSeverity::Warning,
            "same revision",
            now + Duration::seconds(2),
        );
        let duplicate = queue
            .enqueue(
                notification_candidate(
                    worker.id.clone(),
                    &duplicate_event,
                    "dependency_changed",
                    "Producer moved to revision one.",
                ),
                now + Duration::seconds(2),
            )
            .unwrap();
        assert_eq!(
            duplicate,
            NotificationEnqueueOutcome::Suppressed {
                reason: NotificationSuppressionReason::UnchangedContext,
                existing_notification_id: Some(first.id.clone()),
            }
        );

        let second_event = notification_event(
            &store,
            EventSeverity::Critical,
            "revision two",
            now + Duration::seconds(3),
        );
        let coalesced = match queue
            .enqueue(
                notification_candidate(
                    worker.id.clone(),
                    &second_event,
                    "dependency_changed",
                    "Producer moved again to revision two.",
                ),
                now + Duration::seconds(3),
            )
            .unwrap()
        {
            NotificationEnqueueOutcome::Coalesced {
                notification,
                superseded_ids,
            } => {
                assert_eq!(superseded_ids, vec![first.id.clone()]);
                notification
            }
            other => panic!("unexpected coalesced outcome: {other:?}"),
        };
        assert_eq!(coalesced.severity, EventSeverity::Critical);
        assert!(coalesced.summary.starts_with("2 related updates."));
        assert_eq!(
            coalesced.coalesced_source_event_ids,
            vec![first_event.id.clone()]
        );
        let superseded = store.notification(&first.id).unwrap().unwrap();
        assert_eq!(superseded.state, NotificationState::Superseded);
        assert_eq!(superseded.superseded_by, Some(coalesced.id.clone()));

        let capacity_event = notification_event(
            &store,
            EventSeverity::Warning,
            "unrelated",
            now + Duration::seconds(4),
        );
        let at_capacity = queue
            .enqueue(
                notification_candidate(
                    worker.id.clone(),
                    &capacity_event,
                    "worker_health",
                    "A separate notification would exceed the pending cap.",
                ),
                now + Duration::seconds(4),
            )
            .unwrap();
        assert_eq!(
            at_capacity,
            NotificationEnqueueOutcome::Suppressed {
                reason: NotificationSuppressionReason::PendingCapacity,
                existing_notification_id: None,
            }
        );

        let oversized_event = notification_event(
            &store,
            EventSeverity::Warning,
            "oversized",
            now + Duration::seconds(5),
        );
        let mut oversized = notification_candidate(
            worker.id.clone(),
            &oversized_event,
            "oversized",
            "Payload bounds are enforced before persistence.",
        );
        oversized.evidence_refs = (0..33).map(|index| format!("evidence:{index}")).collect();
        assert!(matches!(
            queue.enqueue(oversized, now + Duration::seconds(5)),
            Err(NotificationQueueError::Domain(_))
        ));

        drop(queue);
        drop(store);
        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        assert_eq!(
            reopened
                .notifications_for_worker(&worker.id, Some(NotificationState::Queued))
                .unwrap(),
            vec![coalesced]
        );
    }

    #[test]
    fn stale_coalescing_rolls_back_both_supersession_and_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap();
        let now = Utc::now();
        let worker_id = WorkerId::new();
        let source = queued_notification(worker_id.clone(), now, None);
        store.upsert_notification(&source).unwrap();
        let replacement = queued_notification(worker_id, now + Duration::seconds(1), None);
        let mut superseded = source.clone();
        superseded
            .supersede_attributed(
                replacement.id.clone(),
                CoordinationActor::System,
                Some("coalesced".into()),
                now + Duration::seconds(1),
            )
            .unwrap();

        let error = store
            .replace_notifications_atomically(
                &replacement,
                &[ExpectedRecordUpdate {
                    record: superseded,
                    expected_updated_at: now - Duration::seconds(1),
                }],
            )
            .unwrap_err();
        assert!(matches!(error, StoreError::StaleNotificationRevision(_)));
        assert_eq!(store.notification(&source.id).unwrap(), Some(source));
        assert!(store.notification(&replacement.id).unwrap().is_none());
    }

    #[test]
    fn open_coordination_exchange_expires_durably_at_its_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let now = Utc::now();
        let exchange = CoordinationExchange::new(
            "goal-a",
            EventId::new(),
            "contract:shared-api",
            "Confirm the shared response boundary",
            WorkerId::new(),
            WorkerId::new(),
            "The targeted notification did not settle the contract detail",
            now + Duration::minutes(1),
            4,
            now,
        )
        .unwrap();
        store.insert_exchange(&exchange).unwrap();
        let expired = CoordinationExchangeService::new(store.clone())
            .expire_due("goal-a", now + Duration::minutes(1))
            .unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].state, CoordinationExchangeState::Expired);

        drop(store);
        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        assert_eq!(
            reopened.exchange(&exchange.id).unwrap().unwrap().state,
            CoordinationExchangeState::Expired
        );
        let events = reopened
            .events_for_goal("goal-a", 0, Some(exchange.id.as_str()), 10)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].kind,
            CoordinationEventKind::CoordinationExchangeChanged
        );
    }
}
