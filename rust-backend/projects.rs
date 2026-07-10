use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: String,
    pub name: String,
    pub path: String,
    pub created_at: String,
    pub last_opened_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RegistryState {
    active_project_id: Option<String>,
    projects: Vec<ProjectRecord>,
}

#[derive(Debug, Clone)]
pub struct ProjectRegistry {
    pub path: PathBuf,
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn project_id(path: &Path) -> String {
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let name = resolved
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("project")
        .to_lowercase();
    let mut slug = String::new();
    let mut dash = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character);
            dash = false;
        } else if !dash && !slug.is_empty() {
            slug.push('-');
            dash = true;
        }
    }
    let slug = slug.trim_matches('-');
    let slug = if slug.is_empty() { "project" } else { slug };
    let mut hasher = Sha256::new();
    hasher.update(resolved.to_string_lossy().as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("{}-{}", &slug[..slug.len().min(54)], &digest[..8])
}

impl ProjectRegistry {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
    fn load(&self) -> RegistryState {
        let Ok(text) = fs::read_to_string(&self.path) else {
            return RegistryState::default();
        };
        if let Ok(state) = serde_json::from_str::<RegistryState>(&text) {
            return state;
        }
        if let Ok(projects) = serde_json::from_str::<Vec<ProjectRecord>>(&text) {
            return RegistryState {
                active_project_id: None,
                projects,
            };
        }
        RegistryState::default()
    }
    fn save(&self, state: &RegistryState) -> Result<(), String> {
        let parent = self.path.parent().ok_or("invalid registry path")?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let mut file = NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
        serde_json::to_writer_pretty(&mut file, state).map_err(|error| error.to_string())?;
        file.write_all(b"\n").map_err(|error| error.to_string())?;
        file.as_file()
            .sync_all()
            .map_err(|error| error.to_string())?;
        file.persist(&self.path)
            .map_err(|error| error.to_string())?;
        Ok(())
    }
    pub fn add(&self, path: &Path, activate: bool) -> Result<ProjectRecord, String> {
        let resolved = path.canonicalize().map_err(|error| error.to_string())?;
        let id = project_id(&resolved);
        let mut state = self.load();
        let record = if let Some(record) = state.projects.iter_mut().find(|item| item.id == id) {
            if activate {
                record.last_opened_at = now();
            }
            record.clone()
        } else {
            let timestamp = now();
            let record = ProjectRecord {
                id: id.clone(),
                name: resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_else(|| resolved.to_str().unwrap_or("project"))
                    .into(),
                path: resolved.to_string_lossy().into(),
                created_at: timestamp.clone(),
                last_opened_at: timestamp,
            };
            state.projects.push(record.clone());
            record
        };
        if activate {
            state.active_project_id = Some(id);
        }
        self.save(&state)?;
        Ok(record)
    }
    pub fn get(&self, id: &str) -> Option<ProjectRecord> {
        self.load().projects.into_iter().find(|item| item.id == id)
    }
    pub fn active(&self) -> Option<ProjectRecord> {
        let state = self.load();
        state
            .active_project_id
            .and_then(|id| state.projects.into_iter().find(|item| item.id == id))
    }
    pub fn list(&self) -> Vec<ProjectRecord> {
        self.load().projects
    }
    pub fn remove(&self, id: &str) -> Result<bool, String> {
        let mut state = self.load();
        let before = state.projects.len();
        state.projects.retain(|item| item.id != id);
        if before == state.projects.len() {
            return Ok(false);
        }
        if state.active_project_id.as_deref() == Some(id) {
            state.active_project_id = None;
        }
        self.save(&state)?;
        Ok(true)
    }
    pub fn rename(&self, id: &str, name: &str) -> Result<Option<ProjectRecord>, String> {
        let mut state = self.load();
        let Some(record) = state.projects.iter_mut().find(|item| item.id == id) else {
            return Ok(None);
        };
        record.name = name.trim().to_string();
        let result = record.clone();
        self.save(&state)?;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn registry_persists_active_project() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("demo");
        fs::create_dir(&project).unwrap();
        let registry = ProjectRegistry::new(temp.path().join("app/projects.json"));
        let added = registry.add(&project, true).unwrap();
        assert_eq!(registry.active().unwrap().id, added.id);
    }
}
