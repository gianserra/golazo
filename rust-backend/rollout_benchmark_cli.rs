#![allow(dead_code)]

#[path = "coordination/benchmark.rs"]
mod benchmark;

use benchmark::{BenchmarkInput, RolloutBenchmarkReport};
use std::env;
use std::fs;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<String, String> {
    let mut args = env::args().skip(1);
    let path = args.next().ok_or_else(|| {
        "usage: golazo-rollout-benchmark <observations.json> [--markdown]".to_string()
    })?;
    let markdown = match args.next().as_deref() {
        None => false,
        Some("--markdown") => true,
        Some(argument) => return Err(format!("unexpected argument: {argument}")),
    };
    if let Some(argument) = args.next() {
        return Err(format!("unexpected argument: {argument}"));
    }
    let input: BenchmarkInput = serde_json::from_str(
        &fs::read_to_string(&path).map_err(|error| format!("read {path}: {error}"))?,
    )
    .map_err(|error| format!("parse {path}: {error}"))?;
    let report = RolloutBenchmarkReport::compare(&input).map_err(|error| error.to_string())?;
    if markdown {
        Ok(report.to_markdown())
    } else {
        serde_json::to_string_pretty(&report).map_err(|error| error.to_string())
    }
}
