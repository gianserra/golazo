use super::domain::{EventSeverity, InterventionLevel};
use serde::{Deserialize, Serialize};

pub const SUPERVISOR_EVALS_JSON: &str = include_str!("fixtures/supervisor-evals.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorEvalEvidenceMode {
    None,
    Event,
    Reference,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorEvalCase {
    pub id: String,
    pub scenario: String,
    pub expected_decision: InterventionLevel,
    pub confidence_percent: u8,
    pub affected_worker_count: usize,
    pub evidence_mode: SupervisorEvalEvidenceMode,
    pub summary: String,
    pub recommendation: String,
    pub requested_action: Option<String>,
    pub requires_human_decision: bool,
}

pub fn supervisor_eval_cases() -> Result<Vec<SupervisorEvalCase>, serde_json::Error> {
    serde_json::from_str(SUPERVISOR_EVALS_JSON)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        EventId, InterventionId, InterventionState, SignalId, WorkerId, WorkerState,
    };
    use crate::coordination::supervisor::{
        SupervisorContextPacket, SupervisorEventContext, SupervisorInterventionContext,
        SupervisorTrigger, SupervisorTriggerKind, SupervisorWorkerContext,
    };
    use chrono::Utc;
    use std::collections::BTreeSet;

    #[test]
    fn corpus_covers_required_scenarios_and_validates_reference_decisions() {
        let cases = supervisor_eval_cases().unwrap();
        assert_eq!(cases.len(), 6);
        let case_ids = cases
            .iter()
            .map(|case| case.id.as_str())
            .collect::<BTreeSet<_>>();
        for required in [
            "benign-overlap-awareness",
            "semantic-conflict-coordinate",
            "stale-contract-recommendation",
            "stalled-worker-block",
            "false-alarm-observe",
            "product-direction-human-decision",
        ] {
            assert!(case_ids.contains(required));
        }
        for required in [
            InterventionLevel::Observe,
            InterventionLevel::Inform,
            InterventionLevel::Recommend,
            InterventionLevel::Coordinate,
            InterventionLevel::Block,
            InterventionLevel::Escalate,
        ] {
            assert!(cases.iter().any(|case| case.expected_decision == required));
        }

        for case in cases {
            let now = Utc::now();
            let event_id = EventId::new();
            let evidence_ref = "contract:shared-api:v3".to_string();
            let workers = (0..case.affected_worker_count)
                .map(|_| WorkerId::new())
                .collect::<Vec<_>>();
            let includes_event = matches!(
                case.evidence_mode,
                SupervisorEvalEvidenceMode::Event | SupervisorEvalEvidenceMode::Both
            );
            let includes_reference = matches!(
                case.evidence_mode,
                SupervisorEvalEvidenceMode::Reference | SupervisorEvalEvidenceMode::Both
            );
            let trigger_key = format!("eval:{}", case.id);
            let context = SupervisorContextPacket {
                schema_version: 1,
                goal_id: "eval-goal".into(),
                generated_at: now,
                trigger: SupervisorTrigger {
                    key: trigger_key.clone(),
                    goal_id: "eval-goal".into(),
                    kind: SupervisorTriggerKind::Signal,
                    severity: EventSeverity::Error,
                    confidence_percent: case.confidence_percent,
                    occurred_at: now,
                    signal_id: Some(SignalId::new()),
                    request_event_id: None,
                    escalation_id: None,
                    evidence_event_ids: includes_event
                        .then(|| event_id.clone())
                        .into_iter()
                        .collect(),
                    affected_worker_ids: workers.clone(),
                },
                workers: workers
                    .iter()
                    .map(|worker_id| SupervisorWorkerContext {
                        worker_id: worker_id.clone(),
                        state: WorkerState::Active,
                        updated_at: now,
                        current_run_id: Some("eval-run".into()),
                        current_thread_id: Some("eval-thread".into()),
                        active_claim_ids: vec![],
                        repository_id: Some("eval-repository".into()),
                        worktree_path: Some("/eval/worktree".into()),
                        base_revision: Some("eval-base".into()),
                        last_heartbeat_at: Some(now),
                    })
                    .collect(),
                claims: vec![],
                repository_changes: vec![],
                contracts: vec![],
                failure_events: includes_event
                    .then(|| SupervisorEventContext {
                        event_id: event_id.clone(),
                        kind: crate::coordination::domain::CoordinationEventKind::BlockerRaised,
                        severity: EventSeverity::Error,
                        occurred_at: now,
                        summary: case.scenario.clone(),
                    })
                    .into_iter()
                    .collect(),
                prior_interventions: includes_reference
                    .then(|| SupervisorInterventionContext {
                        intervention_id: InterventionId::new(),
                        level: InterventionLevel::Inform,
                        state: InterventionState::Delivered,
                        updated_at: now,
                        target_worker_ids: workers.clone(),
                        evidence_refs: vec![evidence_ref.clone()],
                        recommendation: "Prior durable observation".into(),
                        requested_action: None,
                        outcome: None,
                    })
                    .into_iter()
                    .collect(),
                open_escalations: vec![],
                truncated: false,
            };
            let escalation = (case.expected_decision == InterventionLevel::Escalate).then(|| {
                serde_json::json!({
                    "kind": "decision_point",
                    "severity": "high",
                    "scope": "contract:shared-api",
                    "question": "Which public API direction should be accepted?",
                    "options": [
                        {
                            "id": "preserve-v2",
                            "label": "Preserve v2",
                            "description": "Retain backward compatibility.",
                            "consequences": ["New behavior is deferred."],
                            "recommended": true
                        },
                        {
                            "id": "adopt-v3",
                            "label": "Adopt v3",
                            "description": "Adopt the new public contract.",
                            "consequences": ["Dependents must migrate."],
                            "recommended": false
                        }
                    ]
                })
            });
            let response = serde_json::json!({
                "schema_version": 1,
                "trigger_key": trigger_key,
                "decision": case.expected_decision,
                "confidence_percent": case.confidence_percent,
                "summary": case.summary,
                "evidence_event_ids": includes_event.then(|| event_id).into_iter().collect::<Vec<_>>(),
                "evidence_refs": includes_reference.then(|| evidence_ref).into_iter().collect::<Vec<_>>(),
                "target_worker_ids": workers,
                "recommendation": case.recommendation,
                "requested_action": case.requested_action,
                "requires_human_decision": case.requires_human_decision,
                "escalation": escalation
            });
            let parsed = context.parse_decision(&response.to_string()).unwrap();
            assert_eq!(parsed.decision, case.expected_decision, "{}", case.id);
        }
    }
}
