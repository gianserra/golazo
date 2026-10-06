#![allow(dead_code)]

mod models;
mod redaction;
mod tracker;

use models::{IntegrationScope, Status};
use serde::Serialize;
use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracker::{Tracker, TrackerError, WorkPackageUpdate};

const VALID_STATUSES: &str = "Planned, Blocked, Partial, Done";

#[derive(Debug)]
struct CliError(String);

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<TrackerError> for CliError {
    fn from(error: TrackerError) -> Self {
        Self(error.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug)]
struct Parsed {
    root: PathBuf,
    command: Command,
}

#[derive(Debug)]
enum Command {
    CreateGoal {
        goal_id: String,
        title: String,
        description: String,
    },
    AddFeature {
        goal_id: String,
        feature_id: String,
        title: String,
        description: String,
        status: Status,
    },
    SetStatus {
        goal_id: String,
        feature_id: String,
        status: Status,
    },
    AddPackage {
        goal_id: String,
        package_id: String,
        title: String,
        description: String,
        feature_ids: Vec<String>,
        depends_on: Vec<String>,
        priority: i32,
        integration_scope: IntegrationScope,
    },
    UpdatePackage {
        goal_id: String,
        package_id: String,
        update: WorkPackageUpdate,
    },
    ReorderPackage {
        goal_id: String,
        package_id: String,
        index: usize,
    },
    ListPackages {
        goal_id: String,
    },
    ReadyWork {
        goal_id: String,
    },
    AddStep {
        goal_id: String,
        feature_id: String,
        step_id: String,
        title: String,
    },
    SetStep {
        goal_id: String,
        feature_id: String,
        step_id: String,
        done: bool,
    },
    SetNext {
        goal_id: String,
        feature_id: String,
        step_id: String,
        next: bool,
    },
    AddSlice {
        goal_id: String,
        feature_id: String,
        summary: String,
        status: Option<Status>,
        evidence: Vec<String>,
    },
    Show {
        goal_id: String,
    },
    Validate {
        goal_id: String,
    },
    Format {
        goal_id: Option<String>,
    },
    List,
    GenerateId {
        title: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), CliError> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{}", help());
        return Ok(());
    }
    let parsed = parse(&mut args)?;
    let tracker = Tracker::new(parsed.root);
    match parsed.command {
        Command::CreateGoal {
            goal_id,
            title,
            description,
        } => print_json(tracker.create_goal(&goal_id, &title, &description)?)?,
        Command::AddFeature {
            goal_id,
            feature_id,
            title,
            description,
            status,
        } => {
            print_json(tracker.add_feature(&goal_id, &feature_id, &title, &description, status)?)?
        }
        Command::SetStatus {
            goal_id,
            feature_id,
            status,
        } => print_json(tracker.set_status(&goal_id, &feature_id, status)?)?,
        Command::AddPackage {
            goal_id,
            package_id,
            title,
            description,
            feature_ids,
            depends_on,
            priority,
            integration_scope,
        } => print_json(tracker.add_work_package(
            &goal_id,
            &package_id,
            &title,
            &description,
            feature_ids,
            depends_on,
            priority,
            integration_scope,
        )?)?,
        Command::UpdatePackage {
            goal_id,
            package_id,
            update,
        } => print_json(tracker.update_work_package(&goal_id, &package_id, update)?)?,
        Command::ReorderPackage {
            goal_id,
            package_id,
            index,
        } => print_json(tracker.reorder_work_package(&goal_id, &package_id, index)?)?,
        Command::ListPackages { goal_id } => print_json(tracker.list_work_packages(&goal_id)?)?,
        Command::ReadyWork { goal_id } => print_json(tracker.ready_work(&goal_id)?)?,
        Command::AddStep {
            goal_id,
            feature_id,
            step_id,
            title,
        } => {
            print_json(tracker.add_step(&goal_id, &feature_id, &step_id, &title, false, false)?)?
        }
        Command::SetStep {
            goal_id,
            feature_id,
            step_id,
            done,
        } => print_json(tracker.set_step(&goal_id, &feature_id, &step_id, done)?)?,
        Command::SetNext {
            goal_id,
            feature_id,
            step_id,
            next,
        } => print_json(tracker.set_next(&goal_id, &feature_id, &step_id, next)?)?,
        Command::AddSlice {
            goal_id,
            feature_id,
            summary,
            status,
            evidence,
        } => print_json(tracker.add_slice(&goal_id, &feature_id, &summary, status, evidence)?)?,
        Command::Show { goal_id } => print_json(tracker.get_goal(&goal_id)?)?,
        Command::Validate { goal_id } => print_json(tracker.validate(&goal_id)?)?,
        Command::Format { goal_id } => match goal_id {
            Some(goal_id) => print_json(tracker.format_goal(&goal_id)?)?,
            None => print_json(tracker.format_all()?)?,
        },
        Command::List => print_json(tracker.list_goals()?)?,
        Command::GenerateId { title } => print_json(serde_json::json!({
            "goal_id": tracker::generate_goal_id(&title)
        }))?,
    }
    Ok(())
}

fn print_json(value: impl Serialize) -> Result<(), CliError> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn parse(args: &mut Vec<String>) -> Result<Parsed, CliError> {
    let mut root = PathBuf::from(".goal-manager");
    while args.first().is_some_and(|arg| arg.starts_with("--")) {
        let flag = args.remove(0);
        match flag.as_str() {
            "--root" => {
                root = PathBuf::from(take(args, "--root value")?);
            }
            _ => return Err(CliError(format!("unknown option: {flag}"))),
        }
    }

    let name = take(args, "command")?;
    let command = match name.as_str() {
        "create-goal" => Command::CreateGoal {
            goal_id: take(args, "goal_id")?,
            title: take(args, "title")?,
            description: optional_value(args, "--description")?.unwrap_or_default(),
        },
        "add-feature" => Command::AddFeature {
            goal_id: take(args, "goal_id")?,
            feature_id: take(args, "feature_id")?,
            title: take(args, "title")?,
            description: optional_value(args, "--description")?.unwrap_or_default(),
            status: optional_value(args, "--status")?
                .map(|value| parse_status(&value))
                .transpose()?
                .unwrap_or(Status::Planned),
        },
        "set-status" => Command::SetStatus {
            goal_id: take(args, "goal_id")?,
            feature_id: take(args, "feature_id")?,
            status: parse_status(&take(args, "status")?)?,
        },
        "add-package" => Command::AddPackage {
            goal_id: take(args, "goal_id")?,
            package_id: take(args, "package_id")?,
            title: take(args, "title")?,
            description: optional_value(args, "--description")?.unwrap_or_default(),
            feature_ids: required_csv(args, "--features")?,
            depends_on: optional_csv(args, "--depends-on")?.unwrap_or_default(),
            priority: optional_value(args, "--priority")?
                .map(|value| parse_number(&value, "priority"))
                .transpose()?
                .unwrap_or_default(),
            integration_scope: optional_value(args, "--integration-scope")?
                .map(|value| parse_integration_scope(&value))
                .transpose()?
                .unwrap_or_default(),
        },
        "update-package" => Command::UpdatePackage {
            goal_id: take(args, "goal_id")?,
            package_id: take(args, "package_id")?,
            update: WorkPackageUpdate {
                title: optional_value(args, "--title")?,
                description: optional_value(args, "--description")?,
                feature_ids: optional_csv(args, "--features")?,
                depends_on: optional_csv(args, "--depends-on")?,
                priority: optional_value(args, "--priority")?
                    .map(|value| parse_number(&value, "priority"))
                    .transpose()?,
                status: optional_value(args, "--status")?
                    .map(|value| parse_status(&value))
                    .transpose()?,
                readiness_policy: None,
                integration_scope: optional_value(args, "--integration-scope")?
                    .map(|value| parse_integration_scope(&value))
                    .transpose()?,
            },
        },
        "reorder-package" => Command::ReorderPackage {
            goal_id: take(args, "goal_id")?,
            package_id: take(args, "package_id")?,
            index: take(args, "index")?
                .parse()
                .map_err(|_| CliError("invalid package index".into()))?,
        },
        "list-packages" => Command::ListPackages {
            goal_id: take(args, "goal_id")?,
        },
        "ready-work" => Command::ReadyWork {
            goal_id: take(args, "goal_id")?,
        },
        "add-step" => Command::AddStep {
            goal_id: take(args, "goal_id")?,
            feature_id: take(args, "feature_id")?,
            step_id: take(args, "step_id")?,
            title: take(args, "title")?,
        },
        "set-step" => Command::SetStep {
            goal_id: take(args, "goal_id")?,
            feature_id: take(args, "feature_id")?,
            step_id: take(args, "step_id")?,
            done: parse_step_state(&take(args, "done|open")?)?,
        },
        "set-next" => Command::SetNext {
            goal_id: take(args, "goal_id")?,
            feature_id: take(args, "feature_id")?,
            step_id: take(args, "step_id")?,
            next: parse_next_state(&take(args, "next|later")?)?,
        },
        "add-slice" => {
            let goal_id = take(args, "goal_id")?;
            let feature_id = take(args, "feature_id")?;
            let summary = take(args, "summary")?;
            let mut status = None;
            let mut evidence = Vec::new();
            while !args.is_empty() {
                let flag = args.remove(0);
                match flag.as_str() {
                    "--status" => status = Some(parse_status(&take(args, "--status value")?)?),
                    "--evidence" => evidence.push(take(args, "--evidence value")?),
                    _ => return Err(CliError(format!("unknown option for add-slice: {flag}"))),
                }
            }
            Command::AddSlice {
                goal_id,
                feature_id,
                summary,
                status,
                evidence,
            }
        }
        "show" => Command::Show {
            goal_id: take(args, "goal_id")?,
        },
        "validate" => Command::Validate {
            goal_id: take(args, "goal_id")?,
        },
        "format" => Command::Format {
            goal_id: if args.is_empty() {
                None
            } else {
                Some(take(args, "goal_id")?)
            },
        },
        "list" | "list-goals" => Command::List,
        "generate-id" => Command::GenerateId {
            title: take(args, "title")?,
        },
        _ => return Err(CliError(format!("unknown command: {name}"))),
    };

    if !args.is_empty() {
        return Err(CliError(format!("unexpected argument: {}", args[0])));
    }
    Ok(Parsed { root, command })
}

fn take(args: &mut Vec<String>, label: &str) -> Result<String, CliError> {
    if args.is_empty() {
        Err(CliError(format!("missing {label}")))
    } else {
        Ok(args.remove(0))
    }
}

fn optional_value(args: &mut Vec<String>, flag: &str) -> Result<Option<String>, CliError> {
    if let Some(position) = args.iter().position(|arg| arg == flag) {
        args.remove(position);
        if position >= args.len() {
            return Err(CliError(format!("missing value for {flag}")));
        }
        Ok(Some(args.remove(position)))
    } else {
        Ok(None)
    }
}

fn csv(value: String) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn optional_csv(args: &mut Vec<String>, flag: &str) -> Result<Option<Vec<String>>, CliError> {
    optional_value(args, flag).map(|value| value.map(csv))
}

fn required_csv(args: &mut Vec<String>, flag: &str) -> Result<Vec<String>, CliError> {
    optional_csv(args, flag)?.ok_or_else(|| CliError(format!("missing {flag}")))
}

fn parse_number(value: &str, label: &str) -> Result<i32, CliError> {
    value
        .parse()
        .map_err(|_| CliError(format!("invalid {label}: expected an integer")))
}

fn parse_integration_scope(value: &str) -> Result<IntegrationScope, CliError> {
    match value {
        "work_package" => Ok(IntegrationScope::WorkPackage),
        "repository" => Ok(IntegrationScope::Repository),
        _ => Err(CliError(
            "invalid integration scope: expected work_package or repository".into(),
        )),
    }
}

fn parse_status(value: &str) -> Result<Status, CliError> {
    match value {
        "Planned" => Ok(Status::Planned),
        "Blocked" => Ok(Status::Blocked),
        "Partial" => Ok(Status::Partial),
        "Done" => Ok(Status::Done),
        _ => Err(CliError(format!(
            "invalid status: expected one of {VALID_STATUSES}"
        ))),
    }
}

fn parse_step_state(value: &str) -> Result<bool, CliError> {
    match value {
        "done" => Ok(true),
        "open" => Ok(false),
        _ => Err(CliError("invalid step state: expected done or open".into())),
    }
}

fn parse_next_state(value: &str) -> Result<bool, CliError> {
    match value {
        "next" => Ok(true),
        "later" => Ok(false),
        _ => Err(CliError(
            "invalid next state: expected next or later".into(),
        )),
    }
}

fn help() -> &'static str {
    "Deterministic Markdown-backed implementation tracker.

Usage:
  golazo-tracker [--root .goal-manager] <command> [args]

Commands:
  create-goal <goal_id> <title> [--description <text>]
  add-feature <goal_id> <feature_id> <title> [--description <text>] [--status Planned|Blocked|Partial|Done]
  set-status <goal_id> <feature_id> <status>
  add-package <goal_id> <package_id> <title> --features <id,id> [--depends-on <id,id>] [--priority <number>] [--integration-scope work_package|repository] [--description <text>]
  update-package <goal_id> <package_id> [--title <text>] [--description <text>] [--features <id,id>] [--depends-on <id,id>] [--priority <number>] [--status <status>] [--integration-scope work_package|repository]
  reorder-package <goal_id> <package_id> <index>
  list-packages <goal_id>
  ready-work <goal_id>
  add-step <goal_id> <feature_id> <step_id> <title>
  set-step <goal_id> <feature_id> <step_id> <done|open>
  set-next <goal_id> <feature_id> <step_id> <next|later>
  add-slice <goal_id> <feature_id> <summary> [--status <status>] [--evidence <text>]...
  show <goal_id>
  validate <goal_id>
  format [goal_id]
  list
  generate-id <title>"
}
