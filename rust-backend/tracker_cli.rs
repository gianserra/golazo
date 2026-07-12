#![allow(dead_code)]

mod models;
mod tracker;

use models::Status;
use serde::Serialize;
use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;
use tracker::{Tracker, TrackerError};

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
        Command::AddStep {
            goal_id,
            feature_id,
            step_id,
            title,
        } => print_json(tracker.add_step(&goal_id, &feature_id, &step_id, &title, false)?)?,
        Command::SetStep {
            goal_id,
            feature_id,
            step_id,
            done,
        } => print_json(tracker.set_step(&goal_id, &feature_id, &step_id, done)?)?,
        Command::AddSlice {
            goal_id,
            feature_id,
            summary,
            status,
            evidence,
        } => print_json(tracker.add_slice(&goal_id, &feature_id, &summary, status, evidence)?)?,
        Command::Show { goal_id } => print_json(tracker.get_goal(&goal_id)?)?,
        Command::Validate { goal_id } => print_json(tracker.validate(&goal_id)?)?,
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

fn help() -> &'static str {
    "Deterministic Markdown-backed implementation tracker.

Usage:
  golazo-tracker [--root .goal-manager] <command> [args]

Commands:
  create-goal <goal_id> <title> [--description <text>]
  add-feature <goal_id> <feature_id> <title> [--description <text>] [--status Planned|Blocked|Partial|Done]
  set-status <goal_id> <feature_id> <status>
  add-step <goal_id> <feature_id> <step_id> <title>
  set-step <goal_id> <feature_id> <step_id> <done|open>
  add-slice <goal_id> <feature_id> <summary> [--status <status>] [--evidence <text>]...
  show <goal_id>
  validate <goal_id>
  list
  generate-id <title>"
}
