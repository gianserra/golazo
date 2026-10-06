use crate::coordination::domain::CoordinationEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const LOG_SCHEMA: &str = "golazo.operation.v1";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CorrelationIds {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intervention_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub escalation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

impl CorrelationIds {
    pub fn for_goal(goal_id: impl Into<String>) -> Self {
        Self {
            goal_id: Some(goal_id.into()),
            ..Self::default()
        }
    }

    pub fn with_worker(mut self, value: impl Into<String>) -> Self {
        self.worker_id = Some(value.into());
        self
    }

    pub fn with_claim(mut self, value: impl Into<String>) -> Self {
        self.claim_id = Some(value.into());
        self
    }

    pub fn with_run(mut self, value: impl Into<String>) -> Self {
        self.run_id = Some(value.into());
        self
    }

    pub fn with_thread(mut self, value: impl Into<String>) -> Self {
        self.thread_id = Some(value.into());
        self
    }

    pub fn with_workspace(mut self, value: impl Into<String>) -> Self {
        self.workspace_id = Some(value.into());
        self
    }

    pub fn with_event(mut self, value: impl Into<String>) -> Self {
        self.event_id = Some(value.into());
        self
    }

    pub fn with_intervention(mut self, value: impl Into<String>) -> Self {
        self.intervention_id = Some(value.into());
        self
    }

    pub fn with_escalation(mut self, value: impl Into<String>) -> Self {
        self.escalation_id = Some(value.into());
        self
    }

    pub fn with_integration(mut self, value: impl Into<String>) -> Self {
        self.integration_id = Some(value.into());
        self
    }

    pub fn with_correlation(mut self, value: impl Into<String>) -> Self {
        self.correlation_id = Some(value.into());
        self
    }

    pub fn from_event(event: &CoordinationEvent) -> Self {
        let mut ids = Self::for_goal(&event.goal_id)
            .with_event(event.id.as_str())
            .with_correlation(&event.correlation_id);
        ids.worker_id = find_string(&event.payload, &["workerId", "worker_id"]);
        ids.claim_id = find_string(&event.payload, &["claimId", "claim_id"]);
        ids.intervention_id = find_string(&event.payload, &["interventionId", "intervention_id"]);
        ids.escalation_id = find_string(&event.payload, &["escalationId", "escalation_id"]);
        ids.integration_id = find_string(
            &event.payload,
            &[
                "integrationId",
                "integration_id",
                "artifactId",
                "artifact_id",
                "jobId",
                "job_id",
                "finalizationId",
                "finalization_id",
            ],
        );
        ids
    }

    pub fn emit_info(&self, operation: &str, outcome: &str) {
        tracing::info!(
            target: "golazo_backend::operation",
            log_schema = LOG_SCHEMA,
            operation,
            outcome,
            goal_id = self.goal_id.as_deref().unwrap_or_default(),
            worker_id = self.worker_id.as_deref().unwrap_or_default(),
            claim_id = self.claim_id.as_deref().unwrap_or_default(),
            run_id = self.run_id.as_deref().unwrap_or_default(),
            thread_id = self.thread_id.as_deref().unwrap_or_default(),
            workspace_id = self.workspace_id.as_deref().unwrap_or_default(),
            event_id = self.event_id.as_deref().unwrap_or_default(),
            intervention_id = self.intervention_id.as_deref().unwrap_or_default(),
            escalation_id = self.escalation_id.as_deref().unwrap_or_default(),
            integration_id = self.integration_id.as_deref().unwrap_or_default(),
            correlation_id = self.correlation_id.as_deref().unwrap_or_default(),
            "Golazo operation"
        );
    }

    pub fn emit_warn(&self, operation: &str, outcome: &str) {
        tracing::warn!(
            target: "golazo_backend::operation",
            log_schema = LOG_SCHEMA,
            operation,
            outcome,
            goal_id = self.goal_id.as_deref().unwrap_or_default(),
            worker_id = self.worker_id.as_deref().unwrap_or_default(),
            claim_id = self.claim_id.as_deref().unwrap_or_default(),
            run_id = self.run_id.as_deref().unwrap_or_default(),
            thread_id = self.thread_id.as_deref().unwrap_or_default(),
            workspace_id = self.workspace_id.as_deref().unwrap_or_default(),
            event_id = self.event_id.as_deref().unwrap_or_default(),
            intervention_id = self.intervention_id.as_deref().unwrap_or_default(),
            escalation_id = self.escalation_id.as_deref().unwrap_or_default(),
            integration_id = self.integration_id.as_deref().unwrap_or_default(),
            correlation_id = self.correlation_id.as_deref().unwrap_or_default(),
            "Golazo operation warning"
        );
    }
}

fn find_string(value: &Value, keys: &[&str]) -> Option<String> {
    match value {
        Value::Object(values) => {
            for key in keys {
                if let Some(value) = values.get(*key).and_then(Value::as_str) {
                    return Some(value.into());
                }
            }
            values.values().find_map(|value| find_string(value, keys))
        }
        Value::Array(values) => values.iter().find_map(|value| find_string(value, keys)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        ActivityCategory, ActivityEventPayload, ClaimId, CoordinationActor,
        CoordinationEventPayload, EventSeverity, WorkerId,
    };
    use chrono::Utc;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex, MutexGuard};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    struct SharedBufferGuard<'a>(MutexGuard<'a, Vec<u8>>);

    impl Write for SharedBufferGuard<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedBufferGuard<'a>;

        fn make_writer(&'a self) -> Self::Writer {
            SharedBufferGuard(self.0.lock().unwrap())
        }
    }

    #[test]
    fn structured_context_serializes_every_supported_correlation_dimension() {
        let ids = CorrelationIds::for_goal("goal-1")
            .with_worker("worker-1")
            .with_claim("claim-1")
            .with_run("run-1")
            .with_thread("thread-1")
            .with_workspace("workspace-1")
            .with_event("event-1")
            .with_intervention("intervention-1")
            .with_escalation("escalation-1")
            .with_integration("integration-1")
            .with_correlation("correlation-1");
        let value = serde_json::to_value(ids).unwrap();
        for field in [
            "goal_id",
            "worker_id",
            "claim_id",
            "run_id",
            "thread_id",
            "workspace_id",
            "event_id",
            "intervention_id",
            "escalation_id",
            "integration_id",
            "correlation_id",
        ] {
            assert!(value.get(field).is_some(), "missing {field}");
        }
    }

    #[test]
    fn event_context_extracts_typed_worker_and_claim_ids() {
        let worker_id = WorkerId::new();
        let claim_id = ClaimId::new();
        let event = CoordinationEvent::from_typed_payload(
            "goal-1",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            "claim-1",
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker_id.clone(),
                claim_id: claim_id.clone(),
                category: ActivityCategory::Progress,
                summary: "progress".into(),
                progress_percent: Some(10),
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec![],
                validation_succeeded: None,
            }),
            Utc::now(),
        )
        .unwrap();
        let ids = CorrelationIds::from_event(&event);
        assert_eq!(ids.goal_id.as_deref(), Some("goal-1"));
        assert_eq!(ids.worker_id.as_deref(), Some(worker_id.as_str()));
        assert_eq!(ids.claim_id.as_deref(), Some(claim_id.as_str()));
        assert_eq!(ids.event_id.as_deref(), Some(event.id.as_str()));
        assert_eq!(ids.correlation_id.as_deref(), Some("claim-1"));
    }

    #[test]
    fn operation_emission_is_flat_json_with_stable_schema_and_ids() {
        let output = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(output.clone())
            .finish();
        let ids = CorrelationIds::for_goal("goal-1")
            .with_worker("worker-1")
            .with_claim("claim-1")
            .with_run("run-1")
            .with_thread("thread-1")
            .with_workspace("workspace-1")
            .with_event("event-1")
            .with_intervention("intervention-1")
            .with_escalation("escalation-1")
            .with_integration("integration-1")
            .with_correlation("correlation-1");
        tracing::subscriber::with_default(subscriber, || {
            ids.emit_info("test.operation", "succeeded");
        });
        let bytes = output.0.lock().unwrap().clone();
        let line = String::from_utf8(bytes).unwrap();
        let value: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(value["log_schema"], LOG_SCHEMA);
        assert_eq!(value["operation"], "test.operation");
        assert_eq!(value["outcome"], "succeeded");
        assert_eq!(value["goal_id"], "goal-1");
        assert_eq!(value["worker_id"], "worker-1");
        assert_eq!(value["claim_id"], "claim-1");
        assert_eq!(value["run_id"], "run-1");
        assert_eq!(value["thread_id"], "thread-1");
        assert_eq!(value["workspace_id"], "workspace-1");
        assert_eq!(value["event_id"], "event-1");
        assert_eq!(value["intervention_id"], "intervention-1");
        assert_eq!(value["escalation_id"], "escalation-1");
        assert_eq!(value["integration_id"], "integration-1");
        assert_eq!(value["correlation_id"], "correlation-1");
    }
}
