use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::sync::LazyLock;

pub const REDACTED_CREDENTIAL: &str = "[REDACTED_CREDENTIAL]";

static ENVIRONMENT_ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b([A-Z][A-Z0-9_]*(?:API_KEY|ACCESS_KEY|PRIVATE_KEY|PASSWORD|PASSWD|TOKEN|SECRET|CREDENTIAL|AUTH)[A-Z0-9_]*)\s*([=:])\s*([^\s,;]+)",
    )
    .expect("credential assignment redaction regex")
});

static SENSITIVE_ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)([\"']?(?:api[_-]?key|access[_-]?key|private[_-]?key|password|passwd|secret|credential|authorization|auth[_-]?token|access[_-]?token|token)[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;}]+)"#,
    )
    .expect("sensitive assignment redaction regex")
});

static AUTHORIZATION_VALUE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(authorization\s*[:=]\s*(?:bearer|basic)\s+)([A-Za-z0-9._~+/=-]+)")
        .expect("authorization redaction regex")
});

static URL_SECRET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)([?&](?:api[_-]?key|access[_-]?token|auth[_-]?token|token|secret|password|passwd)=)([^&\s#]+)",
    )
    .expect("URL secret redaction regex")
});

static URL_USERINFO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(https?://)([^/\s:@]+):([^@\s/]+)@")
        .expect("URL user information redaction regex")
});

static KNOWN_TOKEN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:sk-(?:proj-)?[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]{8,}|xox[baprs]-[A-Za-z0-9-]{8,}|AKIA[A-Z0-9]{12,})\b",
    )
    .expect("known token redaction regex")
});

static PRIVATE_KEY_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----")
        .expect("private key redaction regex")
});

pub fn is_sensitive_field_name(name: &str) -> bool {
    let normalized = name
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    if normalized == "tokens"
        || normalized.ends_with("tokens")
        || normalized.contains("tokenusage")
        || normalized.contains("tokencount")
        || normalized.contains("tokenbudget")
        || normalized.contains("tokenlimit")
    {
        return false;
    }
    if matches!(
        normalized.as_str(),
        "credentialisolation" | "credentialpolicy" | "credentialmetadata"
    ) {
        return false;
    }
    normalized.contains("apikey")
        || normalized.contains("accesskey")
        || normalized.contains("privatekey")
        || normalized.contains("password")
        || normalized.contains("passwd")
        || normalized.contains("secret")
        || normalized.contains("credential")
        || normalized == "token"
        || normalized.ends_with("token")
        || matches!(normalized.as_str(), "cookie" | "setcookie")
}

pub fn redact_sensitive_text(input: &str) -> String {
    let value = PRIVATE_KEY_BLOCK.replace_all(input, REDACTED_CREDENTIAL);
    let value = AUTHORIZATION_VALUE.replace_all(&value, |captures: &regex::Captures<'_>| {
        format!("{}{REDACTED_CREDENTIAL}", &captures[1])
    });
    let value = URL_SECRET.replace_all(&value, |captures: &regex::Captures<'_>| {
        format!("{}{REDACTED_CREDENTIAL}", &captures[1])
    });
    let value = URL_USERINFO.replace_all(&value, |captures: &regex::Captures<'_>| {
        format!("{}{REDACTED_CREDENTIAL}@", &captures[1])
    });
    let value = ENVIRONMENT_ASSIGNMENT.replace_all(&value, |captures: &regex::Captures<'_>| {
        format!("{}{}{REDACTED_CREDENTIAL}", &captures[1], &captures[2])
    });
    let value = SENSITIVE_ASSIGNMENT.replace_all(&value, |captures: &regex::Captures<'_>| {
        format!("{}{REDACTED_CREDENTIAL}", &captures[1])
    });
    KNOWN_TOKEN
        .replace_all(&value, REDACTED_CREDENTIAL)
        .into_owned()
}

pub fn redact_sensitive_value(value: &mut Value) {
    match value {
        Value::Object(entries) => {
            for (key, value) in entries {
                let normalized = key
                    .chars()
                    .filter(|character| character.is_ascii_alphanumeric())
                    .flat_map(char::to_lowercase)
                    .collect::<String>();
                if matches!(normalized.as_str(), "authorization" | "proxyauthorization")
                    && let Value::String(text) = value
                    && ["bearer ", "basic "]
                        .iter()
                        .any(|prefix| text.to_ascii_lowercase().starts_with(prefix))
                {
                    let scheme = text.split_whitespace().next().unwrap_or("Bearer");
                    *text = format!("{scheme} {REDACTED_CREDENTIAL}");
                } else if is_sensitive_field_name(key) {
                    match value {
                        Value::String(_) => {
                            *value = Value::String(REDACTED_CREDENTIAL.into());
                        }
                        Value::Array(values) => {
                            for value in values {
                                match value {
                                    Value::String(_) => {
                                        *value = Value::String(REDACTED_CREDENTIAL.into());
                                    }
                                    _ => redact_sensitive_value(value),
                                }
                            }
                        }
                        Value::Object(_) => redact_sensitive_value(value),
                        Value::Null | Value::Bool(_) | Value::Number(_) => {}
                    }
                } else {
                    redact_sensitive_value(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_sensitive_value(value);
            }
        }
        Value::String(value) => *value = redact_sensitive_text(value),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub fn redacted_copy<T>(value: &T) -> Result<T, serde_json::Error>
where
    T: Serialize + DeserializeOwned,
{
    let mut value = serde_json::to_value(value)?;
    redact_sensitive_value(&mut value);
    serde_json::from_value(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_secret_shapes_without_hiding_usage_or_safe_content() {
        let mut value = json!({
            "OPENAI_API_KEY": "sk-structured-secret",
            "inputTokens": 42,
            "prompt": "Use OPENAI_API_KEY=sk-promptsecret and Authorization: Bearer abc.def.secret",
            "diff": "+ password: hunter2\n+ url=https://user:pass@example.test/path?token=query-secret",
            "log": "github token ghp_1234567890abcdef",
            "event": {"cookie": "session-sensitive", "message": "safe event detail"},
            "escalationEvidence": ["-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----"]
        });

        redact_sensitive_value(&mut value);

        let rendered = value.to_string();
        for secret in [
            "structured-secret",
            "promptsecret",
            "abc.def.secret",
            "hunter2",
            "query-secret",
            "ghp_1234567890abcdef",
            "session-sensitive",
            "BEGIN PRIVATE KEY",
        ] {
            assert!(!rendered.contains(secret), "secret remained: {secret}");
        }
        assert_eq!(value["inputTokens"], 42);
        assert_eq!(value["event"]["message"], "safe event detail");
        assert!(rendered.contains(REDACTED_CREDENTIAL));
        assert!(!is_sensitive_field_name("maxWorkerTokens"));
        assert!(!is_sensitive_field_name("tokenUsage"));
        assert!(is_sensitive_field_name("githubToken"));
    }
}
