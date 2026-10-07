pub use crate::redaction::{
    is_sensitive_field_name as is_sensitive_environment_name,
    redact_sensitive_text as redact_environment_credentials_text,
    redact_sensitive_value as redact_environment_credentials,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceQuotaPolicy {
    pub max_worker_tokens: u64,
    pub max_goal_tokens: u64,
    pub max_worker_turn_seconds: u64,
    pub max_goal_elapsed_seconds: u64,
    pub max_worker_processes: usize,
    pub max_goal_processes: usize,
    pub max_worker_disk_bytes: u64,
    pub max_goal_disk_bytes: u64,
    pub max_worker_network_requests: u64,
    pub max_goal_network_requests: u64,
    pub max_worker_retries: usize,
    pub max_goal_retries: usize,
    pub max_goal_concurrency: usize,
}

impl Default for ResourceQuotaPolicy {
    fn default() -> Self {
        Self {
            max_worker_tokens: 250_000,
            max_goal_tokens: 1_000_000,
            max_worker_turn_seconds: 30 * 60,
            max_goal_elapsed_seconds: 8 * 60 * 60,
            max_worker_processes: 8,
            max_goal_processes: 32,
            max_worker_disk_bytes: 2 * 1024 * 1024 * 1024,
            max_goal_disk_bytes: 8 * 1024 * 1024 * 1024,
            max_worker_network_requests: 0,
            max_goal_network_requests: 0,
            max_worker_retries: 3,
            max_goal_retries: 12,
            max_goal_concurrency: 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceUsage {
    pub tokens: u64,
    pub elapsed_seconds: u64,
    pub processes: usize,
    pub disk_bytes: u64,
    pub network_requests: u64,
    pub retries: usize,
    pub concurrency: usize,
}

pub fn validate_resource_quota_policy(policy: &ResourceQuotaPolicy) -> Result<(), String> {
    if policy.max_worker_tokens == 0
        || policy.max_goal_tokens < policy.max_worker_tokens
        || policy.max_worker_turn_seconds == 0
        || policy.max_goal_elapsed_seconds < policy.max_worker_turn_seconds
        || policy.max_worker_processes == 0
        || policy.max_goal_processes < policy.max_worker_processes
        || policy.max_worker_disk_bytes == 0
        || policy.max_goal_disk_bytes < policy.max_worker_disk_bytes
        || policy.max_goal_network_requests < policy.max_worker_network_requests
        || policy.max_worker_retries == 0
        || policy.max_goal_retries < policy.max_worker_retries
        || policy.max_goal_concurrency == 0
        || policy.max_goal_concurrency > 64
    {
        return Err(
            "resource quotas must be positive, goal limits must cover one worker, and concurrency must be between 1 and 64"
                .into(),
        );
    }
    Ok(())
}

pub fn worker_quota_violation(
    policy: &ResourceQuotaPolicy,
    usage: ResourceUsage,
) -> Option<String> {
    let mut exceeded = Vec::new();
    if usage.tokens >= policy.max_worker_tokens {
        exceeded.push(format!(
            "tokens {} >= {}",
            usage.tokens, policy.max_worker_tokens
        ));
    }
    if usage.elapsed_seconds >= policy.max_worker_turn_seconds {
        exceeded.push(format!(
            "turn seconds {} >= {}",
            usage.elapsed_seconds, policy.max_worker_turn_seconds
        ));
    }
    if usage.processes > policy.max_worker_processes {
        exceeded.push(format!(
            "processes {} > {}",
            usage.processes, policy.max_worker_processes
        ));
    }
    if usage.disk_bytes > policy.max_worker_disk_bytes {
        exceeded.push(format!(
            "disk bytes {} > {}",
            usage.disk_bytes, policy.max_worker_disk_bytes
        ));
    }
    if usage.network_requests > policy.max_worker_network_requests {
        exceeded.push(format!(
            "network requests {} > {}",
            usage.network_requests, policy.max_worker_network_requests
        ));
    }
    if usage.retries >= policy.max_worker_retries {
        exceeded.push(format!(
            "retries {} >= {}",
            usage.retries, policy.max_worker_retries
        ));
    }
    (!exceeded.is_empty())
        .then(|| format!("worker resource quota exceeded: {}", exceeded.join(", ")))
}

pub fn goal_quota_violation(policy: &ResourceQuotaPolicy, usage: ResourceUsage) -> Option<String> {
    let mut exceeded = Vec::new();
    if usage.tokens >= policy.max_goal_tokens {
        exceeded.push(format!(
            "tokens {} >= {}",
            usage.tokens, policy.max_goal_tokens
        ));
    }
    if usage.elapsed_seconds >= policy.max_goal_elapsed_seconds {
        exceeded.push(format!(
            "elapsed seconds {} >= {}",
            usage.elapsed_seconds, policy.max_goal_elapsed_seconds
        ));
    }
    if usage.processes > policy.max_goal_processes {
        exceeded.push(format!(
            "processes {} > {}",
            usage.processes, policy.max_goal_processes
        ));
    }
    if usage.disk_bytes > policy.max_goal_disk_bytes {
        exceeded.push(format!(
            "disk bytes {} > {}",
            usage.disk_bytes, policy.max_goal_disk_bytes
        ));
    }
    if usage.network_requests > policy.max_goal_network_requests {
        exceeded.push(format!(
            "network requests {} > {}",
            usage.network_requests, policy.max_goal_network_requests
        ));
    }
    if usage.retries >= policy.max_goal_retries {
        exceeded.push(format!(
            "retries {} >= {}",
            usage.retries, policy.max_goal_retries
        ));
    }
    if usage.concurrency > policy.max_goal_concurrency {
        exceeded.push(format!(
            "concurrency {} > {}",
            usage.concurrency, policy.max_goal_concurrency
        ));
    }
    (!exceeded.is_empty()).then(|| format!("goal resource quota exceeded: {}", exceeded.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redaction::REDACTED_CREDENTIAL;
    use serde_json::json;

    #[test]
    fn redacts_environment_credentials_in_structured_and_text_payloads() {
        let mut value = json!({
            "OPENAI_API_KEY": "sk-sensitive",
            "message": "AWS_SECRET_ACCESS_KEY=secret-value safe=value",
            "nested": [{"authorization_token": "bearer-sensitive"}],
            "safe": "visible"
        });

        redact_environment_credentials(&mut value);

        assert_eq!(value["OPENAI_API_KEY"], REDACTED_CREDENTIAL);
        assert_eq!(
            value["message"],
            format!("AWS_SECRET_ACCESS_KEY={REDACTED_CREDENTIAL} safe=value")
        );
        assert_eq!(
            value["nested"][0]["authorization_token"],
            REDACTED_CREDENTIAL
        );
        assert_eq!(value["safe"], "visible");
        assert!(!value.to_string().contains("sensitive"));
        assert!(!value.to_string().contains("secret-value"));
        assert!(!is_sensitive_environment_name("input_tokens"));
        assert!(!is_sensitive_environment_name("tokenUsage"));
        assert!(is_sensitive_environment_name("GITHUB_TOKEN"));
    }

    #[test]
    fn validates_and_evaluates_worker_and_goal_resource_quotas() {
        let policy = ResourceQuotaPolicy::default();
        validate_resource_quota_policy(&policy).unwrap();
        assert!(
            validate_resource_quota_policy(&ResourceQuotaPolicy {
                max_goal_tokens: 1,
                ..policy.clone()
            })
            .is_err()
        );

        let worker = worker_quota_violation(
            &policy,
            ResourceUsage {
                tokens: policy.max_worker_tokens,
                processes: policy.max_worker_processes + 1,
                network_requests: 1,
                retries: policy.max_worker_retries,
                ..ResourceUsage::default()
            },
        )
        .unwrap();
        assert!(worker.contains("tokens"));
        assert!(worker.contains("processes"));
        assert!(worker.contains("network requests"));
        assert!(worker.contains("retries"));

        let goal = goal_quota_violation(
            &policy,
            ResourceUsage {
                disk_bytes: policy.max_goal_disk_bytes + 1,
                concurrency: policy.max_goal_concurrency + 1,
                ..ResourceUsage::default()
            },
        )
        .unwrap();
        assert!(goal.contains("disk bytes"));
        assert!(goal.contains("concurrency"));
    }
}
