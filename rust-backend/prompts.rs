use crate::models::WorkMode;

const SPEC_MODE: &str = include_str!("../prompts/spec-mode.md");
const BUILD_MODE: &str = include_str!("../prompts/build-mode.md");
const SCAFFOLD_GOAL: &str = include_str!("../prompts/scaffold-goal.md");
const FILE_ATTACHMENTS: &str = include_str!("../prompts/file-attachments.md");
const AUTONOMOUS_WORKER: &str = include_str!("../prompts/autonomous-worker.md");
const SUPERVISOR: &str = include_str!("../prompts/supervisor.md");
const MANAGE_IMPLEMENTATION_SKILL: &str = include_str!("../skills/manage-implementation/SKILL.md");

const SCAFFOLD_SKILL: &str = include_str!("../skills/scaffold-implementation-goal/SKILL.md");
const SCAFFOLD_GOAL_TYPES: &str =
    include_str!("../skills/scaffold-implementation-goal/references/goal-types.md");
const SCAFFOLD_GREENFIELD: &str =
    include_str!("../skills/scaffold-implementation-goal/references/greenfield.md");
const SCAFFOLD_CONTRACT: &str =
    include_str!("../skills/scaffold-implementation-goal/references/suggestion-contract.md");

fn render(template: &str, values: &[(&str, &str)]) -> String {
    values
        .iter()
        .fold(template.to_string(), |text, (name, value)| {
            text.replace(&format!("{{{{{name}}}}}"), value)
        })
        .trim()
        .to_string()
}

pub fn goal_execution(goal_id: &str, work_mode: &WorkMode) -> String {
    let template = match work_mode {
        WorkMode::Spec => SPEC_MODE,
        WorkMode::Build => BUILD_MODE,
    };
    render(
        template,
        &[
            ("GOAL_ID", goal_id),
            ("MANAGE_IMPLEMENTATION_SKILL", MANAGE_IMPLEMENTATION_SKILL),
        ],
    )
}

pub fn scaffold_goal(user_goal_json: &str) -> String {
    render(
        SCAFFOLD_GOAL,
        &[
            ("SCAFFOLD_SKILL", SCAFFOLD_SKILL),
            ("SCAFFOLD_GOAL_TYPES", SCAFFOLD_GOAL_TYPES),
            ("SCAFFOLD_GREENFIELD", SCAFFOLD_GREENFIELD),
            ("SCAFFOLD_CONTRACT", SCAFFOLD_CONTRACT),
            ("USER_GOAL_JSON", user_goal_json),
        ],
    )
}

pub fn file_attachments(file_list: &str) -> String {
    render(FILE_ATTACHMENTS, &[("FILE_LIST", file_list)])
}

pub fn autonomous_worker(context_packet_json: &str) -> String {
    render(
        AUTONOMOUS_WORKER,
        &[("CONTEXT_PACKET_JSON", context_packet_json)],
    )
}

pub fn supervisor(context_packet_json: &str) -> String {
    render(SUPERVISOR, &[("CONTEXT_PACKET_JSON", context_packet_json)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_mode_prompts_from_markdown_templates() {
        let spec = goal_execution("goal-1", &WorkMode::Spec);
        assert!(spec.starts_with("# Golazo Spec Mode"));
        assert!(spec.contains("goal `goal-1`"));
        assert!(spec.contains("golazo-tracker --root .goal-manager"));
        assert!(spec.contains("never substitute manual tracker edits"));
        assert!(!spec.contains("{{GOAL_ID}}"));
        assert!(!spec.contains("{{MANAGE_IMPLEMENTATION_SKILL}}"));

        let build = goal_execution("goal-1", &WorkMode::Build);
        assert!(build.starts_with("# Golazo Build Mode"));
        assert!(build.contains("does not itself authorize starting work"));
        assert!(build.contains("golazo-tracker --root .goal-manager"));
    }

    #[test]
    fn composes_the_scaffold_template_with_skill_sources() {
        let prompt = scaffold_goal(r#"{"title":"Demo"}"#);
        assert!(prompt.starts_with("# Golazo Goal Scaffolding"));
        assert!(prompt.contains("# Scaffold implementation goal"));
        assert!(prompt.contains("# Goal type patterns"));
        assert!(prompt.contains(r#"{"title":"Demo"}"#));
        assert!(!prompt.contains("{{SCAFFOLD_SKILL}}"));
    }

    #[test]
    fn renders_file_attachment_context() {
        let prompt = file_attachments("- /tmp/brief.md (brief.md)");
        assert!(prompt.starts_with("# User File Attachments"));
        assert!(prompt.contains("/tmp/brief.md"));
        assert!(!prompt.contains("{{FILE_LIST}}"));
    }

    #[test]
    fn renders_autonomous_worker_protocol_with_context_packet() {
        let prompt = autonomous_worker(r#"{"version":1,"goal":{"id":"goal-a"}}"#);
        assert!(prompt.starts_with("# Golazo Autonomous Worker Protocol"));
        assert!(prompt.contains("Your claim is your authority boundary"));
        assert!(prompt.contains("major edits, validation, synchronization, and integration"));
        assert!(prompt.contains(r#"{"version":1,"goal":{"id":"goal-a"}}"#));
        assert!(!prompt.contains("{{CONTEXT_PACKET_JSON}}"));
    }

    #[test]
    fn renders_supervisor_protocol_with_structured_output_contract() {
        let prompt = supervisor(r#"{"schemaVersion":1,"goalId":"goal-a"}"#);
        assert!(prompt.starts_with("# Golazo Exception Supervisor Protocol"));
        assert!(prompt.contains("Do not assign routine work"));
        assert!(prompt.contains("Return exactly one JSON object"));
        assert!(prompt.contains(r#"{"schemaVersion":1,"goalId":"goal-a"}"#));
        assert!(!prompt.contains("{{CONTEXT_PACKET_JSON}}"));
    }
}
