use crate::models::{Feature, GoalRecord, Progress, SliceFile, SliceRecord, Status, Step};
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

fn escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
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
    let item = progress(&data.features);
    let mut lines = vec![
        format!("# {}", data.title),
        String::new(),
        data.description.clone(),
        String::new(),
        "## Progress".into(),
        String::new(),
        format!(
            "{} of {} steps complete ({}%).",
            item.completed_steps, item.total_steps, item.completion_rate
        ),
        String::new(),
        "## Features".into(),
        String::new(),
        "| Feature | Status | Steps | Completion |".into(),
        "| --- | --- | ---: | ---: |".into(),
    ];
    if data.features.is_empty() {
        lines.push("| _No features yet_ | Planned | 0/0 | 0% |".into());
    }
    for feature in &data.features {
        let fp = feature_progress(feature);
        lines.push(format!(
            "| `{}` — {} | {:?} | {}/{} | {}% |",
            escape(&feature.id),
            escape(&feature.title),
            feature.status,
            fp.completed_steps,
            fp.total_steps,
            fp.completion_rate
        ));
    }
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
    lines.extend([String::new(), "## Slice audit files".into(), String::new()]);
    if data.slice_files.is_empty() {
        lines.push("_No implementation slices yet._".into());
    }
    for name in &data.slice_files {
        lines.push(format!("- [{name}](./{name})"));
    }
    lines.extend([String::new(), metadata(data)?]);
    Ok(lines.join("\n").trim_end().to_string() + "\n")
}

fn render_slices(data: &SliceFile) -> Result<String, TrackerError> {
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
        value["slice_count"] = Value::from(slices.len());
        value["slices"] = serde_json::to_value(slices).unwrap();
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
                summary: summary.into(),
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
    pub fn validate(&self, goal_id: &str) -> Result<Value, TrackerError> {
        self.with_lock(|| {
        let goal = self.read_goal(goal_id)?; let mut feature_ids = std::collections::HashSet::new(); let mut expected = 1usize; let mut count = 0usize;
        for feature in &goal.features { valid_id(&feature.id, "feature_id")?; if !feature_ids.insert(&feature.id) { return Err(TrackerError::new(400, format!("duplicate feature: {}", feature.id))); } let mut steps = std::collections::HashSet::new(); for step in &feature.steps { valid_id(&step.id, "step_id")?; if !steps.insert(&step.id) { return Err(TrackerError::new(400, format!("duplicate step in feature {}: {}", feature.id, step.id))); } } }
        for (offset, name) in goal.slice_files.iter().enumerate() { let index = offset + 1; if name != &format!("slices-{index:03}.md") { return Err(TrackerError::new(400, format!("unexpected slice filename or order: {name}"))); } let part: SliceFile = parse(&self.directory(goal_id)?.join(name), "slices")?; if part.goal_id != goal_id || part.index != index || part.slices.len() > MAX_SLICES_PER_FILE { return Err(TrackerError::new(400, format!("slice file identity mismatch: {name}"))); } for item in part.slices { if item.id != format!("slice-{expected:06}") || !feature_ids.contains(&item.feature_id) { return Err(TrackerError::new(400, format!("invalid slice reference: {}", item.id))); } expected += 1; count += 1; } }
        Ok(json!({"valid": true, "goal_id": goal_id, "features": feature_ids.len(), "slices": count}))
    })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn progress_and_rollover_are_compatible() {
        let temp = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(temp.path());
        tracker.create_goal("release", "Release", "Ship").unwrap();
        tracker
            .add_feature("release", "api", "API", "", Status::Planned)
            .unwrap();
        tracker
            .add_step("release", "api", "routes", "Routes", false)
            .unwrap();
        tracker
            .add_step("release", "api", "tests", "Tests", false)
            .unwrap();
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
        assert!(goal_text.starts_with("# Release\n"));
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
                    description: "Prove the architecture".into(),
                    status: Status::Planned,
                    steps: vec![Step {
                        id: "smoke".into(),
                        title: "Verify the primary workflow".into(),
                        done: false,
                    }],
                }],
            )
            .unwrap();
        assert_eq!(goal["features"][0]["id"], "vertical-slice");
        assert_eq!(goal["progress"]["total_steps"], 1);
        assert_eq!(tracker.validate("scaffolded").unwrap()["valid"], true);
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
        assert!(formatted.starts_with("# Legacy\n"));
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
