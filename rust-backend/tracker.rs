use crate::models::{
    Feature, GoalRecord, IntegrationScope, Progress, ReadinessPolicy, SliceFile, SliceRecord,
    Status, Step, TrackerWorkPackage,
};
use crate::redaction::{redact_sensitive_text, redact_sensitive_value, redacted_copy};
use chrono::{SecondsFormat, Utc};
use fs2::FileExt;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

const MARKER: &str = "codex-goal-manager";
const MAX_SLICES_PER_FILE: usize = 100;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct TrackerError {
    pub message: String,
    pub status: u16,
}

impl TrackerError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
    fn io(error: impl std::fmt::Display) -> Self {
        Self::new(500, error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct Tracker {
    pub root: PathBuf,
}

#[derive(Debug, Default)]
pub struct WorkPackageUpdate {
    pub title: Option<String>,
    pub description: Option<String>,
    pub feature_ids: Option<Vec<String>>,
    pub depends_on: Option<Vec<String>>,
    pub priority: Option<i32>,
    pub status: Option<Status>,
    pub readiness_policy: Option<ReadinessPolicy>,
    pub integration_scope: Option<IntegrationScope>,
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn valid_id(value: &str, label: &str) -> Result<(), TrackerError> {
    let pattern = Regex::new(r"^[a-z0-9][a-z0-9-]{0,62}$").unwrap();
    if pattern.is_match(value) {
        Ok(())
    } else {
        Err(TrackerError::new(
            422,
            format!("invalid {label}: use 1-63 lowercase letters, digits, or hyphens"),
        ))
    }
}

pub fn generate_goal_id(title: &str) -> String {
    let ascii: String = title
        .nfkd()
        .filter(|c| c.is_ascii())
        .collect::<String>()
        .to_lowercase();
    let slug_re = Regex::new(r"[^a-z0-9]+").unwrap();
    let mut slug = slug_re
        .replace_all(&ascii, "-")
        .trim_matches('-')
        .to_string();
    if slug.is_empty() {
        slug = "goal".into();
    }
    slug.truncate(slug.len().min(54));
    slug = slug.trim_end_matches('-').to_string();
    let mut hasher = Sha256::new();
    hasher.update(title.as_bytes());
    hasher.update([0]);
    hasher.update(Uuid::new_v4().as_bytes());
    format!("{}-{:x}", slug, hasher.finalize())[..slug.len() + 9].to_string()
}

fn progress(features: &[Feature]) -> Progress {
    let total = features
        .iter()
        .map(|feature| feature.steps.len())
        .sum::<usize>();
    let completed = features
        .iter()
        .flat_map(|feature| &feature.steps)
        .filter(|step| step.done)
        .count();
    let rate = if total == 0 {
        0
    } else {
        ((completed as f64 * 100.0) / total as f64).round() as i64
    };
    Progress {
        completed_steps: completed,
        total_steps: total,
        completion_rate: rate,
    }
}

fn feature_progress(feature: &Feature) -> Progress {
    progress(std::slice::from_ref(feature))
}

fn next_progress(features: &[Feature]) -> Progress {
    let selected = features
        .iter()
        .flat_map(|feature| &feature.steps)
        .filter(|step| step.next)
        .collect::<Vec<_>>();
    let total = selected.len();
    let completed = selected.iter().filter(|step| step.done).count();
    let rate = if total == 0 {
        0
    } else {
        ((completed as f64 * 100.0) / total as f64).round() as i64
    };
    Progress {
        completed_steps: completed,
        total_steps: total,
        completion_rate: rate,
    }
}

fn package_progress(goal: &GoalRecord, package: &TrackerWorkPackage) -> Progress {
    let features = goal
        .features
        .iter()
        .filter(|feature| package.feature_ids.contains(&feature.id))
        .cloned()
        .collect::<Vec<_>>();
    progress(&features)
}

fn package_readiness(goal: &GoalRecord, package: &TrackerWorkPackage) -> &'static str {
    if package.status == Status::Done {
        return "complete";
    }
    if package.status == Status::Blocked {
        return "blocked";
    }
    let dependencies_done = package.depends_on.iter().all(|dependency| {
        goal.work_packages
            .iter()
            .find(|candidate| candidate.id == *dependency)
            .is_some_and(|candidate| candidate.status == Status::Done)
    });
    if dependencies_done {
        "ready"
    } else {
        "waiting_dependencies"
    }
}

fn derive_ready_work(goal: &GoalRecord) -> Vec<Value> {
    let mut packages = goal
        .work_packages
        .iter()
        .enumerate()
        .filter(|(_, package)| package_readiness(goal, package) == "ready")
        .map(|(index, package)| {
            (
                package.priority,
                index,
                json!({
                    "kind": "work_package",
                    "id": package.id,
                    "title": package.title,
                    "priority": package.priority,
                    "feature_ids": package.feature_ids,
                    "readiness": "ready",
                }),
            )
        })
        .collect::<Vec<_>>();
    packages.sort_by_key(|(priority, index, _)| (std::cmp::Reverse(*priority), *index));
    let mut ready = packages
        .into_iter()
        .map(|(_, _, value)| value)
        .collect::<Vec<_>>();
    let packaged = goal
        .work_packages
        .iter()
        .flat_map(|package| &package.feature_ids)
        .collect::<std::collections::HashSet<_>>();
    ready.extend(
        goal.features
            .iter()
            .filter(|feature| {
                !packaged.contains(&feature.id)
                    && matches!(feature.status, Status::Planned | Status::Partial)
            })
            .map(|feature| {
                json!({
                    "kind": "feature",
                    "id": feature.id,
                    "title": feature.title,
                    "priority": 0,
                    "readiness": "ready",
                })
            }),
    );
    ready
}

fn escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn first_paragraph(value: &str) -> &str {
    value.split("\n\n").next().unwrap_or(value).trim()
}

fn metadata<T: serde::Serialize>(data: &T) -> Result<String, TrackerError> {
    Ok(format!(
        "<!-- {MARKER}:{} -->",
        serde_json::to_string(data).map_err(TrackerError::io)?
    ))
}

fn parse<T: DeserializeOwned>(path: &Path, kind: &str) -> Result<T, TrackerError> {
    let text = fs::read_to_string(path).map_err(|_| {
        TrackerError::new(
            404,
            format!("missing or empty {kind} file: {}", path.display()),
        )
    })?;
    let prefix = format!("<!-- {MARKER}:");
    let markers = text
        .lines()
        .filter(|line| line.starts_with(&prefix) && line.ends_with(" -->"))
        .collect::<Vec<_>>();
    if markers.len() != 1 {
        return Err(TrackerError::new(
            400,
            format!("invalid metadata marker in {}", path.display()),
        ));
    }
    let marker = markers[0];
    let raw = &marker[prefix.len()..marker.len() - 4];
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        TrackerError::new(
            400,
            format!("invalid metadata JSON in {}: {error}", path.display()),
        )
    })?;
    if value.get("kind") != Some(&Value::String(kind.into()))
        || value.get("version") != Some(&Value::from(1))
    {
        return Err(TrackerError::new(
            400,
            format!("unsupported {kind} metadata in {}", path.display()),
        ));
    }
    serde_json::from_value(value).map_err(TrackerError::io)
}

fn write_atomic(path: &Path, text: &str) -> Result<(), TrackerError> {
    let parent = path
        .parent()
        .ok_or_else(|| TrackerError::new(500, "invalid path"))?;
    fs::create_dir_all(parent).map_err(TrackerError::io)?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(TrackerError::io)?;
    temporary
        .write_all(text.as_bytes())
        .map_err(TrackerError::io)?;
    temporary.as_file().sync_all().map_err(TrackerError::io)?;
    temporary.persist(path).map_err(TrackerError::io)?;
    Ok(())
}

fn render_goal(data: &GoalRecord) -> Result<String, TrackerError> {
    let data = redacted_copy(data)
        .map_err(|error| TrackerError::new(500, format!("could not redact tracker: {error}")))?;
    let data = &data;
    let next_item = next_progress(&data.features);
    let updated_on = data.updated_at.get(..10).unwrap_or(&data.updated_at);
    let done = data
        .features
        .iter()
        .filter(|feature| feature.status == Status::Done)
        .count();
    let partial = data
        .features
        .iter()
        .filter(|feature| feature.status == Status::Partial)
        .count();
    let planned = data
        .features
        .iter()
        .filter(|feature| feature.status == Status::Planned)
        .count();
    let blocked = data
        .features
        .iter()
        .filter(|feature| feature.status == Status::Blocked)
        .count();
    let feature_total = data.features.len();
    let done_rate = if feature_total == 0 {
        0.0
    } else {
        done as f64 * 100.0 / feature_total as f64
    };
    let mut lines = vec![
        format!("# {} Production Implementation Tracker", data.title),
        String::new(),
        format!("Last updated: {updated_on}"),
        String::new(),
        format!(
            "This document tracks production implementation work for `{}`.",
            data.goal_id
        ),
        String::new(),
        data.description.clone(),
        String::new(),
        "## Status Legend".into(),
        String::new(),
        "| Status | Meaning |".into(),
        "| --- | --- |".into(),
        "| Done | Implemented, verified, and usable in the current application slice. |".into(),
        "| Partial | A useful version exists, but identified production work remains. |".into(),
        "| Planned | The implementation direction is known, but work has not started. |".into(),
        "| Blocked | Work depends on an unresolved decision, credential, service, or infrastructure choice. |".into(),
        String::new(),
        "## Update Rules".into(),
        String::new(),
        "- Update this tracker after every meaningful implementation or verification slice.".into(),
        "- Keep feature rows production-focused; mark only the work selected for the next coherent slice as `next`.".into(),
        "- Mark a step complete only with concrete implementation or verification evidence.".into(),
        "- Status Rollup counts, Done %, and checklist progress are calculated and rendered by `golazo-tracker`; agents must not calculate or hand-edit them.".into(),
        "- Preserve slice records as append-only audit history; do not use them as the handoff queue.".into(),
        "- Run `golazo-tracker validate` after tracker mutations or repairs.".into(),
        String::new(),
        "## Status Rollup".into(),
        String::new(),
        format!("Last counted: {updated_on}"),
        String::new(),
        "| Status | Count |".into(),
        "| --- | ---: |".into(),
        format!("| Done | {done} |"),
        format!("| Partial | {partial} |"),
        format!("| Planned | {planned} |"),
        format!("| Blocked | {blocked} |"),
        format!("| Total | {feature_total} |"),
        String::new(),
        format!("Done %: **{done_rate:.1}%**"),
        String::new(),
        format!(
            "Next Slice Checklist progress: **{} / {} complete ({}%)**",
            next_item.completed_steps, next_item.total_steps, next_item.completion_rate
        ),
        String::new(),
        "## Implementation Matrix".into(),
        String::new(),
        "| ID | Status | Area | Production scope | Completion bar |".into(),
        "| --- | --- | --- | --- | --- |".into(),
    ];
    if data.features.is_empty() {
        lines.push("| — | Planned | _No features yet_ | — | 0/0 steps (0%) |".into());
    }
    for feature in &data.features {
        let fp = feature_progress(feature);
        lines.push(format!(
            "| `{}` | {:?} | {} | {} | {}/{} steps ({}%) |",
            escape(&feature.id),
            feature.status,
            escape(&feature.title),
            escape(first_paragraph(&feature.description)),
            fp.completed_steps,
            fp.total_steps,
            fp.completion_rate
        ));
    }
    lines.extend([String::new(), "## Work Package Plan".into(), String::new()]);
    if data.work_packages.is_empty() {
        lines.push(
            "_No explicit work packages. Incomplete Features remain individually claimable._"
                .into(),
        );
    } else {
        lines.extend([
            "| Package | Status | Priority | Features | Dependencies | Readiness | Integration | Completion |".into(),
            "| --- | --- | ---: | --- | --- | --- | --- | --- |".into(),
        ]);
        for package in &data.work_packages {
            let item = package_progress(data, package);
            lines.push(format!(
                "| `{}` — {} | {:?} | {} | {} | {} | {} | {:?} | {}/{} steps ({}%) |",
                escape(&package.id),
                escape(&package.title),
                package.status,
                package.priority,
                escape(&package.feature_ids.join(", ")),
                if package.depends_on.is_empty() {
                    "—".into()
                } else {
                    escape(&package.depends_on.join(", "))
                },
                package_readiness(data, package),
                package.integration_scope,
                item.completed_steps,
                item.total_steps,
                item.completion_rate,
            ));
        }
    }
    lines.extend([
        String::new(),
        "## Next Slice Checklist".into(),
        String::new(),
    ]);
    let mut has_next_steps = false;
    for feature in &data.features {
        for step in feature.steps.iter().filter(|step| step.next) {
            has_next_steps = true;
            lines.push(format!(
                "- [{}] `{}/{}` — {}",
                if step.done { "x" } else { " " },
                feature.id,
                step.id,
                step.title
            ));
        }
    }
    if !has_next_steps {
        lines.push("_No implementation steps are marked next._".into());
    }
    lines.extend([String::new(), "## Feature Details".into()]);
    for feature in &data.features {
        lines.extend([
            String::new(),
            format!("### {} (`{}`)", feature.title, feature.id),
            String::new(),
            feature.description.clone(),
            String::new(),
        ]);
        if feature.steps.is_empty() {
            lines.push("_No implementation steps yet._".into());
        }
        for step in &feature.steps {
            lines.push(format!(
                "- [{}] `{}` — {}",
                if step.done { "x" } else { " " },
                step.id,
                step.title
            ));
        }
    }
    lines.extend([String::new(), "## Slice Records".into(), String::new()]);
    lines.push("Slice records are append-only audit history. Selected near-term work belongs in the Next Slice Checklist; all remaining work stays in feature details.".into());
    lines.extend([
        String::new(),
        "| Range | Record |".into(),
        "| --- | --- |".into(),
    ]);
    if data.slice_files.is_empty() {
        lines.push("| — | _No implementation slices yet_ |".into());
    }
    for (index, name) in data.slice_files.iter().enumerate() {
        let start = index * MAX_SLICES_PER_FILE + 1;
        let end = start + MAX_SLICES_PER_FILE - 1;
        lines.push(format!("| {start:03}–{end:03} | [{name}](./{name}) |"));
    }
    lines.extend([String::new(), metadata(data)?]);
    Ok(lines.join("\n").trim_end().to_string() + "\n")
}

fn render_slices(data: &SliceFile) -> Result<String, TrackerError> {
    let data = redacted_copy(data)
        .map_err(|error| TrackerError::new(500, format!("could not redact slices: {error}")))?;
    let data = &data;
    let mut lines = vec![
        format!("# Implementation slices {:03}", data.index),
        String::new(),
        format!(
            "Goal: `{}` · Entries: {}/{}",
            data.goal_id,
            data.slices.len(),
            MAX_SLICES_PER_FILE
        ),
    ];
    for item in &data.slices {
        lines.extend([
            String::new(),
            format!("## {} — {}", item.id, item.feature_id),
            String::new(),
            format!("- Timestamp: {}", item.at),
            format!("- Status after slice: {:?}", item.status),
            format!("- Summary: {}", item.summary),
            "- Evidence:".into(),
        ]);
        if item.evidence.is_empty() {
            lines.push("  - _None recorded._".into());
        }
        for entry in &item.evidence {
            lines.push(format!("  - {entry}"));
        }
    }
    lines.extend([String::new(), metadata(data)?]);
    Ok(lines.join("\n").trim_end().to_string() + "\n")
}

fn validate_package_membership(
    goal: &GoalRecord,
    package_id: &str,
    feature_ids: &[String],
    depends_on: &[String],
) -> Result<(), TrackerError> {
    if feature_ids.is_empty() {
        return Err(TrackerError::new(
            422,
            "a work package must contain at least one feature",
        ));
    }
    let known_features = goal
        .features
        .iter()
        .map(|feature| feature.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut membership = std::collections::HashSet::new();
    for feature_id in feature_ids {
        valid_id(feature_id, "feature_id")?;
        if !known_features.contains(feature_id.as_str()) {
            return Err(TrackerError::new(
                422,
                format!("work package references missing feature: {feature_id}"),
            ));
        }
        if !membership.insert(feature_id) {
            return Err(TrackerError::new(
                422,
                format!("duplicate feature in work package: {feature_id}"),
            ));
        }
        if goal
            .work_packages
            .iter()
            .any(|package| package.id != package_id && package.feature_ids.contains(feature_id))
        {
            return Err(TrackerError::new(
                422,
                format!("feature already belongs to another work package: {feature_id}"),
            ));
        }
    }
    let known_packages = goal
        .work_packages
        .iter()
        .map(|package| package.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut dependencies = std::collections::HashSet::new();
    for dependency in depends_on {
        valid_id(dependency, "work_package_dependency")?;
        if dependency == package_id {
            return Err(TrackerError::new(
                422,
                format!("work package cannot depend on itself: {package_id}"),
            ));
        }
        if !known_packages.contains(dependency.as_str()) {
            return Err(TrackerError::new(
                422,
                format!("work package references missing dependency: {dependency}"),
            ));
        }
        if !dependencies.insert(dependency) {
            return Err(TrackerError::new(
                422,
                format!("duplicate work package dependency: {dependency}"),
            ));
        }
    }
    Ok(())
}

fn validate_work_package_graph(goal: &GoalRecord) -> Result<(), TrackerError> {
    let mut package_ids = std::collections::HashSet::new();
    let mut feature_membership = std::collections::HashMap::new();
    let feature_ids = goal
        .features
        .iter()
        .map(|feature| feature.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    for package in &goal.work_packages {
        valid_id(&package.id, "work_package_id")?;
        if !package_ids.insert(package.id.as_str()) {
            return Err(TrackerError::new(
                400,
                format!("duplicate work package: {}", package.id),
            ));
        }
        if package.feature_ids.is_empty() {
            return Err(TrackerError::new(
                400,
                format!("work package has no features: {}", package.id),
            ));
        }
        for feature_id in &package.feature_ids {
            if !feature_ids.contains(feature_id.as_str()) {
                return Err(TrackerError::new(
                    400,
                    format!("work package references missing feature: {feature_id}"),
                ));
            }
            if let Some(existing) = feature_membership.insert(feature_id.as_str(), &package.id) {
                return Err(TrackerError::new(
                    400,
                    format!(
                        "feature {feature_id} belongs to multiple work packages: {existing}, {}",
                        package.id
                    ),
                ));
            }
        }
        if package.status == Status::Done
            && package.feature_ids.iter().any(|feature_id| {
                goal.features
                    .iter()
                    .find(|feature| feature.id == *feature_id)
                    .is_none_or(|feature| feature.status != Status::Done)
            })
        {
            return Err(TrackerError::new(
                400,
                format!(
                    "done work package contains an incomplete feature: {}",
                    package.id
                ),
            ));
        }
    }
    for package in &goal.work_packages {
        let mut dependencies = std::collections::HashSet::new();
        for dependency in &package.depends_on {
            if dependency == &package.id {
                return Err(TrackerError::new(
                    400,
                    format!("work package cannot depend on itself: {}", package.id),
                ));
            }
            if !package_ids.contains(dependency.as_str()) {
                return Err(TrackerError::new(
                    400,
                    format!("work package references missing dependency: {dependency}"),
                ));
            }
            if !dependencies.insert(dependency) {
                return Err(TrackerError::new(
                    400,
                    format!("duplicate work package dependency: {dependency}"),
                ));
            }
        }
    }

    fn visit<'a>(
        id: &'a str,
        packages: &'a [TrackerWorkPackage],
        visiting: &mut std::collections::HashSet<&'a str>,
        visited: &mut std::collections::HashSet<&'a str>,
    ) -> Result<(), TrackerError> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(TrackerError::new(
                400,
                format!("work package dependency cycle includes: {id}"),
            ));
        }
        let package = packages.iter().find(|package| package.id == id).unwrap();
        for dependency in &package.depends_on {
            visit(dependency, packages, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id);
        Ok(())
    }

    let mut visited = std::collections::HashSet::new();
    for package in &goal.work_packages {
        visit(
            &package.id,
            &goal.work_packages,
            &mut std::collections::HashSet::new(),
            &mut visited,
        )?;
    }
    Ok(())
}

impl Tracker {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    fn directory(&self, goal_id: &str) -> Result<PathBuf, TrackerError> {
        valid_id(goal_id, "goal_id")?;
        Ok(self.root.join(goal_id))
    }
    fn with_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, TrackerError>,
    ) -> Result<T, TrackerError> {
        fs::create_dir_all(&self.root).map_err(TrackerError::io)?;
        let lock = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(".lock"))
            .map_err(TrackerError::io)?;
        lock.lock_exclusive().map_err(TrackerError::io)?;
        let result = operation();
        let _ = lock.unlock();
        result
    }
    fn read_goal(&self, id: &str) -> Result<GoalRecord, TrackerError> {
        parse(&self.directory(id)?.join("implementation.md"), "goal")
    }
    fn read_slices(&self, goal: &GoalRecord) -> Result<Vec<SliceRecord>, TrackerError> {
        let dir = self.directory(&goal.goal_id)?;
        let mut result = Vec::new();
        for name in &goal.slice_files {
            result.extend(parse::<SliceFile>(&dir.join(name), "slices")?.slices);
        }
        Ok(result)
    }
    fn save_goal(&self, goal: &mut GoalRecord) -> Result<(), TrackerError> {
        goal.updated_at = utc_now();
        write_atomic(
            &self.directory(&goal.goal_id)?.join("implementation.md"),
            &render_goal(goal)?,
        )
    }
    fn rewrite_goal_documents(&self, goal: &GoalRecord) -> Result<usize, TrackerError> {
        let dir = self.directory(&goal.goal_id)?;
        for name in &goal.slice_files {
            let slices: SliceFile = parse(&dir.join(name), "slices")?;
            write_atomic(&dir.join(name), &render_slices(&slices)?)?;
        }
        write_atomic(&dir.join("implementation.md"), &render_goal(goal)?)?;
        Ok(1 + goal.slice_files.len())
    }
    fn enriched(&self, goal: &GoalRecord, slices: &[SliceRecord]) -> Value {
        let mut value = serde_json::to_value(goal).unwrap();
        if let Some(features) = value.get_mut("features").and_then(Value::as_array_mut) {
            for (index, feature) in features.iter_mut().enumerate() {
                feature["progress"] =
                    serde_json::to_value(feature_progress(&goal.features[index])).unwrap();
                feature["slice_count"] = Value::from(
                    slices
                        .iter()
                        .filter(|item| item.feature_id == goal.features[index].id)
                        .count(),
                );
            }
        }
        value["progress"] = serde_json::to_value(progress(&goal.features)).unwrap();
        value["ready_work"] = Value::Array(derive_ready_work(goal));
        value["slice_count"] = Value::from(slices.len());
        value["slices"] = serde_json::to_value(slices).unwrap();
        redact_sensitive_value(&mut value);
        value
    }
    pub fn create_goal(
        &self,
        goal_id: &str,
        title: &str,
        description: &str,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let path = self.directory(goal_id)?.join("implementation.md");
            if path.exists() {
                return Err(TrackerError::new(
                    409,
                    format!("goal already exists: {goal_id}"),
                ));
            }
            let now = utc_now();
            let goal = GoalRecord {
                version: 1,
                kind: "goal".into(),
                goal_id: goal_id.into(),
                title: title.into(),
                description: description.into(),
                created_at: now.clone(),
                updated_at: now,
                features: vec![],
                work_packages: vec![],
                slice_files: vec![],
            };
            write_atomic(&path, &render_goal(&goal)?)?;
            Ok(self.enriched(&goal, &[]))
        })
    }
    pub fn create_goal_with_features(
        &self,
        goal_id: &str,
        title: &str,
        description: &str,
        features: Vec<Feature>,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let path = self.directory(goal_id)?.join("implementation.md");
            if path.exists() {
                return Err(TrackerError::new(
                    409,
                    format!("goal already exists: {goal_id}"),
                ));
            }
            let mut feature_ids = std::collections::HashSet::new();
            for feature in &features {
                valid_id(&feature.id, "feature_id")?;
                if !feature_ids.insert(&feature.id) {
                    return Err(TrackerError::new(
                        422,
                        format!("duplicate feature: {}", feature.id),
                    ));
                }
                let mut step_ids = std::collections::HashSet::new();
                for step in &feature.steps {
                    valid_id(&step.id, "step_id")?;
                    if !step_ids.insert(&step.id) {
                        return Err(TrackerError::new(
                            422,
                            format!("duplicate step in feature {}: {}", feature.id, step.id),
                        ));
                    }
                }
            }
            let now = utc_now();
            let goal = GoalRecord {
                version: 1,
                kind: "goal".into(),
                goal_id: goal_id.into(),
                title: title.into(),
                description: description.into(),
                created_at: now.clone(),
                updated_at: now,
                features,
                work_packages: vec![],
                slice_files: vec![],
            };
            write_atomic(&path, &render_goal(&goal)?)?;
            Ok(self.enriched(&goal, &[]))
        })
    }
    pub fn list_goals(&self) -> Result<Vec<Value>, TrackerError> {
        self.with_lock(|| {
            if !self.root.exists() {
                return Ok(vec![]);
            }
            let mut paths = fs::read_dir(&self.root)
                .map_err(TrackerError::io)?
                .filter_map(Result::ok)
                .map(|entry| entry.path().join("implementation.md"))
                .filter(|path| path.exists())
                .collect::<Vec<_>>();
            paths.sort();
            paths
                .into_iter()
                .map(|path| {
                    let goal: GoalRecord = parse(&path, "goal")?;
                    let slices = self.read_slices(&goal)?;
                    Ok(self.enriched(&goal, &slices))
                })
                .collect()
        })
    }
    pub fn get_goal(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let goal = self.read_goal(goal_id)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }
    #[allow(dead_code)] // Used by the standalone golazo-tracker binary, not the backend binary.
    pub fn format_goal(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let goal = self.read_goal(goal_id)?;
            let files = self.rewrite_goal_documents(&goal)?;
            Ok(json!({"formatted": true, "goal_id": goal_id, "files": files}))
        })
    }
    #[allow(dead_code)] // Used by the standalone golazo-tracker binary, not the backend binary.
    pub fn format_all(&self) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            if !self.root.exists() {
                return Ok(json!({"formatted": true, "goals": 0, "files": 0}));
            }
            let mut paths = fs::read_dir(&self.root)
                .map_err(TrackerError::io)?
                .filter_map(Result::ok)
                .map(|entry| entry.path().join("implementation.md"))
                .filter(|path| path.exists())
                .collect::<Vec<_>>();
            paths.sort();
            let mut files = 0usize;
            for path in &paths {
                let goal: GoalRecord = parse(path, "goal")?;
                files += self.rewrite_goal_documents(&goal)?;
            }
            Ok(json!({"formatted": true, "goals": paths.len(), "files": files}))
        })
    }
    pub fn add_feature(
        &self,
        goal_id: &str,
        id: &str,
        title: &str,
        description: &str,
        status: Status,
    ) -> Result<Value, TrackerError> {
        valid_id(id, "feature_id")?;
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            if goal.features.iter().any(|feature| feature.id == id) {
                return Err(TrackerError::new(
                    409,
                    format!("feature already exists: {id}"),
                ));
            }
            goal.features.push(Feature {
                id: id.into(),
                title: title.into(),
                description: description.into(),
                status,
                steps: vec![],
            });
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_work_package(
        &self,
        goal_id: &str,
        id: &str,
        title: &str,
        description: &str,
        feature_ids: Vec<String>,
        depends_on: Vec<String>,
        priority: i32,
        integration_scope: IntegrationScope,
    ) -> Result<Value, TrackerError> {
        valid_id(id, "work_package_id")?;
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            if goal.work_packages.iter().any(|package| package.id == id) {
                return Err(TrackerError::new(
                    409,
                    format!("work package already exists: {id}"),
                ));
            }
            validate_package_membership(&goal, id, &feature_ids, &depends_on)?;
            goal.work_packages.push(TrackerWorkPackage {
                id: id.into(),
                title: title.into(),
                description: description.into(),
                feature_ids,
                depends_on,
                priority,
                status: Status::Planned,
                readiness_policy: ReadinessPolicy::AllDependenciesDone,
                integration_scope,
            });
            validate_work_package_graph(&goal)?;
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }

    pub fn update_work_package(
        &self,
        goal_id: &str,
        package_id: &str,
        update: WorkPackageUpdate,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let index = goal
                .work_packages
                .iter()
                .position(|package| package.id == package_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("work package not found: {package_id}"))
                })?;
            let feature_ids = update
                .feature_ids
                .as_ref()
                .unwrap_or(&goal.work_packages[index].feature_ids)
                .clone();
            let depends_on = update
                .depends_on
                .as_ref()
                .unwrap_or(&goal.work_packages[index].depends_on)
                .clone();
            validate_package_membership(&goal, package_id, &feature_ids, &depends_on)?;
            let package = &mut goal.work_packages[index];
            if let Some(value) = update.title {
                package.title = value;
            }
            if let Some(value) = update.description {
                package.description = value;
            }
            package.feature_ids = feature_ids;
            package.depends_on = depends_on;
            if let Some(value) = update.priority {
                package.priority = value;
            }
            if let Some(value) = update.status {
                package.status = value;
            }
            if let Some(value) = update.readiness_policy {
                package.readiness_policy = value;
            }
            if let Some(value) = update.integration_scope {
                package.integration_scope = value;
            }
            validate_work_package_graph(&goal)?;
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }

    pub fn reorder_work_package(
        &self,
        goal_id: &str,
        package_id: &str,
        index: usize,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let current = goal
                .work_packages
                .iter()
                .position(|package| package.id == package_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("work package not found: {package_id}"))
                })?;
            if index >= goal.work_packages.len() {
                return Err(TrackerError::new(
                    422,
                    format!(
                        "work package index {index} is outside 0..{}",
                        goal.work_packages.len()
                    ),
                ));
            }
            let package = goal.work_packages.remove(current);
            goal.work_packages.insert(index, package);
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }

    pub fn list_work_packages(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let goal = self.read_goal(goal_id)?;
            Ok(serde_json::to_value(goal.work_packages).map_err(TrackerError::io)?)
        })
    }

    pub fn ready_work(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let goal = self.read_goal(goal_id)?;
            validate_work_package_graph(&goal)?;
            Ok(Value::Array(derive_ready_work(&goal)))
        })
    }
    pub fn set_status(
        &self,
        goal_id: &str,
        feature_id: &str,
        status: Status,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let feature = goal
                .features
                .iter_mut()
                .find(|item| item.id == feature_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("feature not found: {feature_id}"))
                })?;
            feature.status = status;
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }
    pub fn add_step(
        &self,
        goal_id: &str,
        feature_id: &str,
        id: &str,
        title: &str,
        done: bool,
        next: bool,
    ) -> Result<Value, TrackerError> {
        valid_id(id, "step_id")?;
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let feature = goal
                .features
                .iter_mut()
                .find(|item| item.id == feature_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("feature not found: {feature_id}"))
                })?;
            if feature.steps.iter().any(|step| step.id == id) {
                return Err(TrackerError::new(409, format!("step already exists: {id}")));
            }
            feature.steps.push(Step {
                id: id.into(),
                title: title.into(),
                done,
                next,
            });
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }
    pub fn set_step(
        &self,
        goal_id: &str,
        feature_id: &str,
        step_id: &str,
        done: bool,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let feature = goal
                .features
                .iter_mut()
                .find(|item| item.id == feature_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("feature not found: {feature_id}"))
                })?;
            let step = feature
                .steps
                .iter_mut()
                .find(|item| item.id == step_id)
                .ok_or_else(|| TrackerError::new(404, format!("step not found: {step_id}")))?;
            step.done = done;
            if done {
                step.next = false;
            }
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }

    pub fn set_next(
        &self,
        goal_id: &str,
        feature_id: &str,
        step_id: &str,
        next: bool,
    ) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let target = goal
                .features
                .iter()
                .find(|item| item.id == feature_id)
                .ok_or_else(|| TrackerError::new(404, format!("feature not found: {feature_id}")))?
                .steps
                .iter()
                .find(|item| item.id == step_id)
                .ok_or_else(|| TrackerError::new(404, format!("step not found: {step_id}")))?;
            if next && target.done {
                return Err(TrackerError::new(
                    409,
                    "a completed step cannot be marked Next",
                ));
            }
            if next {
                for feature in &mut goal.features {
                    for step in &mut feature.steps {
                        step.next = false;
                    }
                }
            }
            let step = goal
                .features
                .iter_mut()
                .find(|item| item.id == feature_id)
                .and_then(|feature| feature.steps.iter_mut().find(|item| item.id == step_id))
                .expect("target step was validated before mutation");
            step.next = next;
            self.save_goal(&mut goal)?;
            let slices = self.read_slices(&goal)?;
            Ok(self.enriched(&goal, &slices))
        })
    }
    pub fn add_slice(
        &self,
        goal_id: &str,
        feature_id: &str,
        summary: &str,
        status: Option<Status>,
        evidence: Vec<String>,
    ) -> Result<Value, TrackerError> {
        let summary = redact_sensitive_text(summary);
        let evidence = evidence
            .into_iter()
            .map(|entry| redact_sensitive_text(&entry))
            .collect::<Vec<_>>();
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let feature = goal
                .features
                .iter_mut()
                .find(|item| item.id == feature_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("feature not found: {feature_id}"))
                })?;
            if let Some(value) = status {
                feature.status = value;
            }
            let current_status = feature.status;
            let dir = self.directory(goal_id)?;
            let mut file = if let Some(name) = goal.slice_files.last() {
                let candidate: SliceFile = parse(&dir.join(name), "slices")?;
                if candidate.slices.len() < MAX_SLICES_PER_FILE {
                    Some(candidate)
                } else {
                    None
                }
            } else {
                None
            };
            if file.is_none() {
                let index = goal.slice_files.len() + 1;
                goal.slice_files.push(format!("slices-{index:03}.md"));
                file = Some(SliceFile {
                    version: 1,
                    kind: "slices".into(),
                    goal_id: goal_id.into(),
                    index,
                    slices: vec![],
                });
            }
            let mut file = file.unwrap();
            let prior = goal.slice_files[..file.index - 1]
                .iter()
                .map(|name| {
                    parse::<SliceFile>(&dir.join(name), "slices").map(|part| part.slices.len())
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>();
            let item = SliceRecord {
                id: format!("slice-{:06}", prior + file.slices.len() + 1),
                feature_id: feature_id.into(),
                at: utc_now(),
                summary,
                status: current_status,
                evidence,
            };
            file.slices.push(item.clone());
            let name = &goal.slice_files[file.index - 1];
            write_atomic(&dir.join(name), &render_slices(&file)?)?;
            self.save_goal(&mut goal)?;
            Ok(serde_json::to_value(item).unwrap())
        })
    }

    pub fn apply_integration_finalization(
        &self,
        goal_id: &str,
        feature_id: &str,
        step_ids: &[String],
        summary: &str,
        status: Status,
        mut evidence: Vec<String>,
        finalization_id: &str,
    ) -> Result<Value, TrackerError> {
        if summary.trim().is_empty() || finalization_id.trim().is_empty() {
            return Err(TrackerError::new(
                422,
                "integration finalization requires a summary and a durable identity",
            ));
        }
        if status == Status::Done && step_ids.is_empty() {
            return Err(TrackerError::new(
                422,
                "a completed step is required when finalizing as Done",
            ));
        }
        let summary = redact_sensitive_text(summary);
        evidence = evidence
            .into_iter()
            .map(|entry| redact_sensitive_text(&entry))
            .collect();
        self.with_lock(|| {
            let mut goal = self.read_goal(goal_id)?;
            let existing_slices = self.read_slices(&goal)?;
            let marker = format!("integration-finalization:{finalization_id}");
            let feature = goal
                .features
                .iter_mut()
                .find(|feature| feature.id == feature_id)
                .ok_or_else(|| {
                    TrackerError::new(404, format!("feature not found: {feature_id}"))
                })?;
            for step_id in step_ids {
                let step = feature
                    .steps
                    .iter_mut()
                    .find(|step| &step.id == step_id)
                    .ok_or_else(|| TrackerError::new(404, format!("step not found: {step_id}")))?;
                step.done = true;
                step.next = false;
            }
            feature.status = status;
            if let Some(existing) = existing_slices
                .iter()
                .find(|slice| slice.evidence.iter().any(|item| item == &marker))
            {
                self.save_goal(&mut goal)?;
                return Ok(serde_json::to_value(existing).unwrap());
            }

            evidence.insert(0, marker);
            let dir = self.directory(goal_id)?;
            let mut file = if let Some(name) = goal.slice_files.last() {
                let candidate: SliceFile = parse(&dir.join(name), "slices")?;
                (candidate.slices.len() < MAX_SLICES_PER_FILE).then_some(candidate)
            } else {
                None
            };
            if file.is_none() {
                let index = goal.slice_files.len() + 1;
                goal.slice_files.push(format!("slices-{index:03}.md"));
                file = Some(SliceFile {
                    version: 1,
                    kind: "slices".into(),
                    goal_id: goal_id.into(),
                    index,
                    slices: Vec::new(),
                });
            }
            let mut file = file.expect("slice file was initialized");
            let prior = goal.slice_files[..file.index - 1]
                .iter()
                .map(|name| {
                    parse::<SliceFile>(&dir.join(name), "slices").map(|part| part.slices.len())
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<usize>();
            let item = SliceRecord {
                id: format!("slice-{:06}", prior + file.slices.len() + 1),
                feature_id: feature_id.into(),
                at: utc_now(),
                summary,
                status,
                evidence,
            };
            file.slices.push(item.clone());
            let name = &goal.slice_files[file.index - 1];
            write_atomic(&dir.join(name), &render_slices(&file)?)?;
            self.save_goal(&mut goal)?;
            Ok(serde_json::to_value(item).unwrap())
        })
    }
    pub fn validate(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
            let goal = self.read_goal(goal_id)?;
            let mut feature_ids = std::collections::HashSet::new();
            let mut expected = 1usize;
            let mut count = 0usize;
            let mut next_steps = 0usize;
            for feature in &goal.features {
                valid_id(&feature.id, "feature_id")?;
                if !feature_ids.insert(&feature.id) {
                    return Err(TrackerError::new(
                        400,
                        format!("duplicate feature: {}", feature.id),
                    ));
                }
                let mut steps = std::collections::HashSet::new();
                for step in &feature.steps {
                    valid_id(&step.id, "step_id")?;
                    if !steps.insert(&step.id) {
                        return Err(TrackerError::new(
                            400,
                            format!("duplicate step in feature {}: {}", feature.id, step.id),
                        ));
                    }
                    if step.next {
                        if step.done {
                            return Err(TrackerError::new(
                                400,
                                format!(
                                    "completed step cannot be marked Next: {}/{}",
                                    feature.id, step.id
                                ),
                            ));
                        }
                        next_steps += 1;
                    }
                }
            }
            if next_steps > 1 {
                return Err(TrackerError::new(
                    400,
                    "implementation tracker may contain at most one Next step",
                ));
            }
            validate_work_package_graph(&goal)?;
            for (offset, name) in goal.slice_files.iter().enumerate() {
                let index = offset + 1;
                if name != &format!("slices-{index:03}.md") {
                    return Err(TrackerError::new(
                        400,
                        format!("unexpected slice filename or order: {name}"),
                    ));
                }
                let part: SliceFile = parse(&self.directory(goal_id)?.join(name), "slices")?;
                if part.goal_id != goal_id
                    || part.index != index
                    || part.slices.len() > MAX_SLICES_PER_FILE
                {
                    return Err(TrackerError::new(
                        400,
                        format!("slice file identity mismatch: {name}"),
                    ));
                }
                for item in part.slices {
                    if item.id != format!("slice-{expected:06}")
                        || !feature_ids.contains(&item.feature_id)
                    {
                        return Err(TrackerError::new(
                            400,
                            format!("invalid slice reference: {}", item.id),
                        ));
                    }
                    expected += 1;
                    count += 1;
                }
            }
            Ok(json!({"valid": true, "goal_id": goal_id, "features": feature_ids.len(), "slices": count}))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_step_is_unique_and_cleared_when_completed() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("release", "Release", "Ship").unwrap();
        tracker
            .add_feature("release", "api", "API", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("release", "api", "one", "One", false, false)
            .unwrap();
        tracker
            .add_step("release", "api", "two", "Two", false, false)
            .unwrap();
        tracker.set_next("release", "api", "one", true).unwrap();
        tracker.set_next("release", "api", "two", true).unwrap();
        let goal = tracker.get_goal("release").unwrap();
        let next = goal["features"][0]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|step| step["next"] == true)
            .collect::<Vec<_>>();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0]["id"], "two");

        tracker.set_step("release", "api", "two", true).unwrap();
        let goal = tracker.get_goal("release").unwrap();
        assert!(
            goal["features"][0]["steps"]
                .as_array()
                .unwrap()
                .iter()
                .all(|step| step["next"] != true)
        );
        assert!(tracker.set_next("release", "api", "two", true).is_err());
        assert_eq!(tracker.validate("release").unwrap()["valid"], true);
    }

    #[test]
    fn progress_and_rollover_are_compatible() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("release", "Release", "Ship").unwrap();
        tracker
            .add_feature("release", "api", "API", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("release", "api", "routes", "Routes", false, false)
            .unwrap();
        tracker
            .add_step("release", "api", "tests", "Tests", false, false)
            .unwrap();
        tracker.set_next("release", "api", "tests", true).unwrap();
        tracker.set_step("release", "api", "routes", true).unwrap();
        tracker
            .add_slice(
                "release",
                "api",
                "Added routes",
                Some(Status::Partial),
                vec!["src/api.rs".into()],
            )
            .unwrap();
        let goal = tracker.get_goal("release").unwrap();
        assert_eq!(goal["progress"]["completion_rate"], 50);
        assert_eq!(goal["features"][0]["status"], "Partial");
        assert_eq!(tracker.validate("release").unwrap()["valid"], true);
        let goal_text = fs::read_to_string(temp.path().join("release/implementation.md")).unwrap();
        assert!(goal_text.starts_with("# Release Production Implementation Tracker\n"));
        assert!(goal_text.contains("## Status Legend"));
        assert!(goal_text.contains("## Status Rollup"));
        assert!(goal_text.contains(
            "Status Rollup counts, Done %, and checklist progress are calculated and rendered by `golazo-tracker`"
        ));
        assert!(goal_text.contains("## Implementation Matrix"));
        assert!(goal_text.contains("| `api` | Partial | API |"));
        assert!(goal_text.contains("## Next Slice Checklist"));
        assert!(goal_text.contains("- [ ] `api/tests` — Tests"));
        assert!(!goal_text.contains("- [ ] `api/routes` — Routes"));
        assert!(goal_text.contains("## Slice Records"));
        assert!(
            goal_text
                .lines()
                .last()
                .unwrap()
                .starts_with("<!-- codex-goal-manager:")
        );
        let slice_text = fs::read_to_string(temp.path().join("release/slices-001.md")).unwrap();
        assert!(slice_text.starts_with("# Implementation slices 001\n"));
        assert!(
            slice_text
                .lines()
                .last()
                .unwrap()
                .starts_with("<!-- codex-goal-manager:")
        );
    }

    #[test]
    fn redacts_sensitive_tracker_text_before_storage_and_projection() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker
            .create_goal(
                "redaction",
                "Redaction",
                "Use OPENAI_API_KEY=sk-goal-description-secret safely",
            )
            .unwrap();
        tracker
            .add_feature("redaction", "audit", "Audit", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("redaction", "audit", "verify", "Verify", false, false)
            .unwrap();
        let slice = tracker
            .add_slice(
                "redaction",
                "audit",
                "Captured Authorization: Bearer slice-summary-secret",
                Some(Status::Partial),
                vec!["password=evidence-secret".into()],
            )
            .unwrap();

        let document =
            std::fs::read_to_string(temp.path().join("redaction").join("implementation.md"))
                .unwrap();
        let slices =
            std::fs::read_to_string(temp.path().join("redaction").join("slices-001.md")).unwrap();
        let projection = tracker.get_goal("redaction").unwrap().to_string();
        let combined = format!("{document}\n{slices}\n{projection}\n{slice}");
        assert!(!combined.contains("goal-description-secret"));
        assert!(!combined.contains("slice-summary-secret"));
        assert!(!combined.contains("evidence-secret"));
        assert!(combined.contains(crate::redaction::REDACTED_CREDENTIAL));
    }

    #[test]
    fn creates_a_scaffolded_goal_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        let goal = tracker
            .create_goal_with_features(
                "scaffolded",
                "Scaffolded",
                "Created from accepted suggestions",
                vec![Feature {
                    id: "vertical-slice".into(),
                    title: "First vertical slice".into(),
                    description: "Prove the architecture\n\nRisks: Keep the matrix concise".into(),
                    status: Status::Planned,
                    steps: vec![Step {
                        id: "smoke".into(),
                        title: "Verify the primary workflow".into(),
                        done: false,
                        next: false,
                    }],
                }],
            )
            .unwrap();
        assert_eq!(goal["features"][0]["id"], "vertical-slice");
        assert_eq!(goal["progress"]["total_steps"], 1);
        assert_eq!(tracker.validate("scaffolded").unwrap()["valid"], true);
        let text = fs::read_to_string(temp.path().join("scaffolded/implementation.md")).unwrap();
        assert!(text.contains(
            "| `vertical-slice` | Planned | First vertical slice | Prove the architecture |"
        ));
        assert!(!text.contains("Prove the architecture Risks:"));
    }

    #[test]
    fn work_packages_preserve_feature_progress_and_slice_history() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("packaged", "Packaged", "").unwrap();
        tracker
            .add_feature("packaged", "domain", "Domain", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("packaged", "domain", "types", "Define types", false, false)
            .unwrap();
        tracker
            .add_feature("packaged", "store", "Store", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("packaged", "store", "sqlite", "Add SQLite", false, false)
            .unwrap();
        tracker
            .add_slice(
                "packaged",
                "domain",
                "Planned domain",
                None,
                vec!["design.md".into()],
            )
            .unwrap();
        tracker
            .add_work_package(
                "packaged",
                "foundation",
                "Foundation",
                "Domain first",
                vec!["domain".into()],
                vec![],
                10,
                IntegrationScope::WorkPackage,
            )
            .unwrap();
        tracker
            .add_work_package(
                "packaged",
                "persistence",
                "Persistence",
                "Store second",
                vec!["store".into()],
                vec!["foundation".into()],
                5,
                IntegrationScope::Repository,
            )
            .unwrap();
        tracker
            .update_work_package(
                "packaged",
                "persistence",
                WorkPackageUpdate {
                    priority: Some(20),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            tracker.ready_work("packaged").unwrap()[0]["id"],
            "foundation"
        );
        assert_eq!(
            tracker
                .ready_work("packaged")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        tracker
            .set_status("packaged", "domain", Status::Done)
            .unwrap();
        tracker
            .update_work_package(
                "packaged",
                "foundation",
                WorkPackageUpdate {
                    status: Some(Status::Done),
                    ..Default::default()
                },
            )
            .unwrap();
        tracker
            .reorder_work_package("packaged", "persistence", 0)
            .unwrap();

        let goal = tracker.get_goal("packaged").unwrap();
        assert_eq!(goal["progress"]["total_steps"], 2);
        assert_eq!(goal["progress"]["completed_steps"], 0);
        assert_eq!(goal["slice_count"], 1);
        assert_eq!(goal["features"][0]["id"], "domain");
        assert_eq!(goal["features"][1]["id"], "store");
        assert_eq!(goal["work_packages"][0]["id"], "persistence");
        assert_eq!(goal["work_packages"][0]["priority"], 20);
        assert_eq!(goal["ready_work"][0]["id"], "persistence");
        assert_eq!(
            tracker.list_work_packages("packaged").unwrap()[1]["id"],
            "foundation"
        );
        let text = fs::read_to_string(temp.path().join("packaged/implementation.md")).unwrap();
        assert!(text.contains("## Work Package Plan"));
        assert!(text.contains("| `persistence` — Persistence | Planned | 20 | store | foundation | ready | Repository |"));
    }

    #[test]
    fn work_package_cycles_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("cycles", "Cycles", "").unwrap();
        for feature_id in ["one", "two"] {
            tracker
                .add_feature("cycles", feature_id, feature_id, "", Status::Planned)
                .unwrap();
        }
        tracker
            .add_work_package(
                "cycles",
                "first",
                "First",
                "",
                vec!["one".into()],
                vec![],
                0,
                IntegrationScope::WorkPackage,
            )
            .unwrap();
        tracker
            .add_work_package(
                "cycles",
                "second",
                "Second",
                "",
                vec!["two".into()],
                vec!["first".into()],
                0,
                IntegrationScope::WorkPackage,
            )
            .unwrap();

        let error = tracker
            .update_work_package(
                "cycles",
                "first",
                WorkPackageUpdate {
                    depends_on: Some(vec!["second".into()]),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("dependency cycle"));
        assert_eq!(tracker.validate("cycles").unwrap()["valid"], true);
    }

    #[test]
    fn legacy_goals_without_work_packages_remain_readable() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker
            .create_goal("legacy-packages", "Legacy", "")
            .unwrap();
        tracker
            .add_feature(
                "legacy-packages",
                "legacy-feature",
                "Legacy feature",
                "",
                Status::Planned,
            )
            .unwrap();
        tracker
            .add_step(
                "legacy-packages",
                "legacy-feature",
                "legacy-step",
                "Legacy step",
                false,
                false,
            )
            .unwrap();
        let path = temp.path().join("legacy-packages/implementation.md");
        let text = fs::read_to_string(&path).unwrap();
        let mut lines = text.lines().map(str::to_string).collect::<Vec<_>>();
        let marker = lines.last().unwrap();
        let prefix = format!("<!-- {MARKER}:");
        let raw = &marker[prefix.len()..marker.len() - 4];
        let mut value: Value = serde_json::from_str(raw).unwrap();
        value.as_object_mut().unwrap().remove("work_packages");
        *lines.last_mut().unwrap() = metadata(&value).unwrap();
        write_atomic(&path, &(lines.join("\n") + "\n")).unwrap();

        let goal = tracker.get_goal("legacy-packages").unwrap();
        assert_eq!(goal["work_packages"], json!([]));
        assert_eq!(goal["ready_work"][0]["kind"], "feature");
        assert_eq!(goal["ready_work"][0]["id"], "legacy-feature");
        assert_eq!(goal["progress"]["total_steps"], 1);
        assert_eq!(tracker.validate("legacy-packages").unwrap()["valid"], true);
        tracker.format_goal("legacy-packages").unwrap();
        let formatted = fs::read_to_string(path).unwrap();
        assert!(formatted.contains(
            "_No explicit work packages. Incomplete Features remain individually claimable._"
        ));
    }

    #[test]
    fn legacy_header_metadata_can_be_read_and_formatted_to_the_footer() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker
            .create_goal("legacy", "Legacy", "Compatible")
            .unwrap();
        let path = temp.path().join("legacy/implementation.md");
        let text = fs::read_to_string(&path).unwrap();
        let mut lines = text.lines().map(str::to_string).collect::<Vec<_>>();
        let marker = lines.pop().unwrap();
        lines.insert(0, marker);
        write_atomic(&path, &(lines.join("\n") + "\n")).unwrap();

        assert_eq!(tracker.get_goal("legacy").unwrap()["title"], "Legacy");
        assert_eq!(tracker.format_goal("legacy").unwrap()["files"], 1);
        let formatted = fs::read_to_string(path).unwrap();
        assert!(formatted.starts_with("# Legacy Production Implementation Tracker\n"));
        assert!(
            formatted
                .lines()
                .last()
                .unwrap()
                .starts_with("<!-- codex-goal-manager:")
        );
    }

    #[test]
    fn slice_files_roll_over_after_one_hundred_entries() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("rollover", "Rollover", "").unwrap();
        tracker
            .add_feature("rollover", "core", "Core", "", Status::Planned)
            .unwrap();
        for index in 0..101 {
            tracker
                .add_slice(
                    "rollover",
                    "core",
                    &format!("Slice {}", index + 1),
                    None,
                    vec![],
                )
                .unwrap();
        }
        let goal = tracker.get_goal("rollover").unwrap();
        assert_eq!(
            goal["slice_files"],
            json!(["slices-001.md", "slices-002.md"])
        );
        assert_eq!(goal["slice_count"], 101);
        assert_eq!(goal["slices"][100]["id"], "slice-000101");
        assert_eq!(tracker.validate("rollover").unwrap()["slices"], 101);
    }
}
