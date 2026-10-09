use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const COORDINATION_API_VERSION: &str = "v1";
pub const DEFAULT_PAGE_LIMIT: usize = 50;
pub const MAX_PAGE_LIMIT: usize = 200;

pub fn resource_version(value: &impl Serialize) -> String {
    let encoded = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(encoded);
    digest
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetadata {
    pub id: String,
    pub resource_version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDocument<T> {
    pub api_version: String,
    pub kind: String,
    pub metadata: ResourceMetadata,
    pub data: T,
}

impl<T> ResourceDocument<T> {
    pub fn new(kind: impl Into<String>, metadata: ResourceMetadata, data: T) -> Self {
        Self {
            api_version: COORDINATION_API_VERSION.into(),
            kind: kind.into(),
            metadata,
            data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionPage<T> {
    pub api_version: String,
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

impl<T> CollectionPage<T> {
    pub fn new(items: Vec<T>, next_cursor: Option<String>) -> Self {
        Self {
            api_version: COORDINATION_API_VERSION.into(),
            items,
            next_cursor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PageQuery {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl PageQuery {
    pub fn validated_limit(&self) -> Result<usize, ContractError> {
        let limit = self.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
        if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
            return Err(ContractError::InvalidPageLimit(limit));
        }
        Ok(limit)
    }

    pub fn validate_cursor(&self) -> Result<(), ContractError> {
        if self.cursor.as_ref().is_some_and(|cursor| {
            cursor.trim().is_empty()
                || cursor.len() > 500
                || cursor.chars().any(|character| character.is_control())
        }) {
            return Err(ContractError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandEnvelope<T> {
    pub idempotency_key: String,
    #[serde(default)]
    pub expected_resource_version: Option<String>,
    #[serde(default)]
    pub confirmation: Option<ActionConfirmation>,
    pub command: T,
}

impl<T> CommandEnvelope<T> {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.idempotency_key.trim().is_empty()
            || self.idempotency_key.len() > 200
            || self
                .expected_resource_version
                .as_ref()
                .is_some_and(|revision| revision.trim().is_empty() || revision.len() > 200)
        {
            return Err(ContractError::InvalidCommandEnvelope);
        }
        Ok(())
    }

    pub fn require_confirmation(
        &self,
        expected_action: &str,
        expected_target: &str,
    ) -> Result<&ActionConfirmation, ContractError> {
        let confirmation =
            self.confirmation
                .as_ref()
                .ok_or_else(|| ContractError::ConfirmationRequired {
                    action: expected_action.into(),
                    target: expected_target.into(),
                })?;
        confirmation.validate(expected_action, expected_target)?;
        Ok(confirmation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionConfirmation {
    pub action: String,
    pub target: String,
    pub confirmed_by: String,
    pub reason: String,
}

impl ActionConfirmation {
    pub fn validate(
        &self,
        expected_action: &str,
        expected_target: &str,
    ) -> Result<(), ContractError> {
        let bounded = |value: &str, maximum: usize| {
            !value.trim().is_empty()
                && value.chars().count() <= maximum
                && !value.chars().any(char::is_control)
        };
        if self.action != expected_action
            || self.target != expected_target
            || !bounded(&self.confirmed_by, 500)
            || !bounded(&self.reason, 2_000)
        {
            return Err(ContractError::InvalidConfirmation {
                action: expected_action.into(),
                target: expected_target.into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProblemCode {
    InvalidRequest,
    NotFound,
    Conflict,
    StaleRevision,
    DuplicateCommand,
    Forbidden,
    ConfirmationRequired,
    LimitExceeded,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiProblem {
    pub api_version: String,
    pub code: ProblemCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ApiProblem {
    pub fn new(code: ProblemCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            api_version: COORDINATION_API_VERSION.into(),
            code,
            message: message.into(),
            retryable,
            details: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationApiManifest {
    pub api_version: String,
    pub stability: String,
    pub resource_versioning: String,
    pub pagination: String,
    pub idempotency: String,
    pub optimistic_concurrency: String,
    pub resources: Vec<String>,
    pub command_groups: Vec<String>,
}

impl Default for CoordinationApiManifest {
    fn default() -> Self {
        Self {
            api_version: COORDINATION_API_VERSION.into(),
            stability: "additive changes remain within v1; breaking changes require a new path version"
                .into(),
            resource_versioning:
                "resourceVersion changes on every durable mutation and is returned with each resource"
                    .into(),
            pagination:
                "collections use opaque cursor and bounded limit parameters with deterministic ordering"
                    .into(),
            idempotency:
                "mutating commands require an idempotencyKey whose first committed outcome is replayed"
                    .into(),
            optimistic_concurrency:
                "commands that mutate existing resources accept expectedResourceVersion and reject stale revisions"
                    .into(),
            resources: vec![
                "pools".into(),
                "metrics".into(),
                "health".into(),
                "operational-alerts".into(),
                "supervisor-runtime".into(),
                "workers".into(),
                "work-packages".into(),
                "claims".into(),
                "claim-traces".into(),
                "partial-work-artifacts".into(),
                "goal-delivery".into(),
                "contracts".into(),
                "events".into(),
                "event-streams".into(),
                "signals".into(),
                "interventions".into(),
                "notifications".into(),
                "escalations".into(),
                "integration-artifacts".into(),
                "integration-validation-reports".into(),
                "integration-reconciliations".into(),
                "integration-jobs".into(),
                "integration-finalizations".into(),
                "integration-maintenance".into(),
            ],
            command_groups: vec![
                "pool-lifecycle".into(),
                "claim-lifecycle".into(),
                "notification-delivery".into(),
                "escalation-resolution".into(),
                "integration-lifecycle".into(),
            ],
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContractError {
    #[error("page limit must be between 1 and {MAX_PAGE_LIMIT}, received {0}")]
    InvalidPageLimit(usize),
    #[error("cursor must be non-empty, bounded, and contain no control characters")]
    InvalidCursor,
    #[error("command requires a bounded idempotency key and optional bounded expected revision")]
    InvalidCommandEnvelope,
    #[error("action {action} on {target} requires explicit confirmation")]
    ConfirmationRequired { action: String, target: String },
    #[error("confirmation must be bounded and match action {action} and target {target}")]
    InvalidConfirmation { action: String, target: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_is_versioned_bounded_and_explicit_about_mutation_safety() {
        let manifest = CoordinationApiManifest::default();
        assert_eq!(manifest.api_version, "v1");
        assert!(manifest.resources.contains(&"claims".into()));
        assert!(
            manifest
                .command_groups
                .contains(&"integration-lifecycle".into())
        );

        assert_eq!(PageQuery::default().validated_limit().unwrap(), 50);
        assert_eq!(
            PageQuery {
                cursor: None,
                limit: Some(201)
            }
            .validated_limit(),
            Err(ContractError::InvalidPageLimit(201))
        );
        assert!(
            PageQuery {
                cursor: Some("claim-123".into()),
                limit: Some(10)
            }
            .validate_cursor()
            .is_ok()
        );

        let confirmed = CommandEnvelope {
            idempotency_key: "stop-once".into(),
            expected_resource_version: Some("revision-1".into()),
            confirmation: Some(ActionConfirmation {
                action: "pool.stop".into(),
                target: "goal-a".into(),
                confirmed_by: "Alex".into(),
                reason: "Stop autonomous execution".into(),
            }),
            command: Value::Null,
        };
        assert!(
            confirmed
                .require_confirmation("pool.stop", "goal-a")
                .is_ok()
        );
        assert!(matches!(
            confirmed.require_confirmation("pool.stop", "goal-b"),
            Err(ContractError::InvalidConfirmation { .. })
        ));
        let unconfirmed = CommandEnvelope {
            idempotency_key: "stop-two".into(),
            expected_resource_version: None,
            confirmation: None,
            command: Value::Null,
        };
        assert!(matches!(
            unconfirmed.require_confirmation("pool.stop", "goal-a"),
            Err(ContractError::ConfirmationRequired { .. })
        ));

        let command = CommandEnvelope {
            idempotency_key: "pool:start:goal-a:1".into(),
            expected_resource_version: Some("2026-10-04T00:00:00Z".into()),
            confirmation: None,
            command: serde_json::json!({"action":"start"}),
        };
        command.validate().unwrap();
        let encoded = serde_json::to_value(command).unwrap();
        assert_eq!(encoded["idempotencyKey"], "pool:start:goal-a:1");
        assert!(encoded.get("expectedResourceVersion").is_some());

        let problem = ApiProblem::new(ProblemCode::StaleRevision, "revision changed", true);
        let encoded = serde_json::to_value(problem).unwrap();
        assert_eq!(encoded["apiVersion"], "v1");
        assert_eq!(encoded["code"], "stale_revision");
        assert_eq!(encoded["retryable"], true);
        assert_eq!(resource_version(&manifest).len(), 24);
    }
}
