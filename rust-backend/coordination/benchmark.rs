use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

pub const ROLLOUT_BENCHMARK_SCHEMA: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMode {
    SingleWorker,
    Pooled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkObservation {
    pub id: String,
    pub workload_id: String,
    pub mode: BenchmarkMode,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub completed_units: u64,
    pub total_tokens: u64,
    pub conflict_events: u64,
    pub integration_latency_milliseconds: Vec<u64>,
    pub human_interruptions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkInput {
    pub observations: Vec<BenchmarkObservation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkMetrics {
    pub observation_count: usize,
    pub completed_units: u64,
    pub elapsed_seconds: f64,
    pub completed_units_per_hour: f64,
    pub total_tokens: u64,
    pub tokens_per_completed_unit: f64,
    pub conflict_events: u64,
    pub conflicts_per_completed_unit: f64,
    pub integration_samples: usize,
    pub average_integration_latency_milliseconds: f64,
    pub p95_integration_latency_milliseconds: u64,
    pub human_interruptions: u64,
    pub human_interruptions_per_completed_unit: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkDelta {
    pub throughput_change_percent: f64,
    pub tokens_per_unit_change_percent: f64,
    pub conflict_rate_change_percent: Option<f64>,
    pub integration_latency_change_percent: f64,
    pub human_interruption_rate_change_percent: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RolloutBenchmarkReport {
    pub schema_version: u16,
    pub workload_counts: BTreeMap<String, usize>,
    pub single_worker: BenchmarkMetrics,
    pub pooled: BenchmarkMetrics,
    pub pooled_relative_to_single: BenchmarkDelta,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BenchmarkError {
    #[error("benchmark observations require non-empty unique IDs and workload IDs")]
    InvalidIdentity,
    #[error("benchmark observation IDs must be unique: {0}")]
    DuplicateObservation(String),
    #[error("benchmark observation {0} must have positive elapsed time and completed work")]
    InvalidWork(String),
    #[error("benchmark observation {0} must include at least one integration latency sample")]
    MissingIntegrationSamples(String),
    #[error("single-worker and pooled observations must contain the same workload multiset")]
    IncomparableWorkloads,
    #[error("both single-worker and pooled observations are required")]
    MissingCohort,
}

impl RolloutBenchmarkReport {
    pub fn compare(input: &BenchmarkInput) -> Result<Self, BenchmarkError> {
        let mut seen = std::collections::BTreeSet::new();
        let mut by_mode = BTreeMap::<BenchmarkMode, Vec<&BenchmarkObservation>>::new();
        let mut workloads = BTreeMap::<BenchmarkMode, BTreeMap<String, usize>>::new();
        for observation in &input.observations {
            if observation.id.trim().is_empty() || observation.workload_id.trim().is_empty() {
                return Err(BenchmarkError::InvalidIdentity);
            }
            if !seen.insert(observation.id.clone()) {
                return Err(BenchmarkError::DuplicateObservation(observation.id.clone()));
            }
            if observation.finished_at <= observation.started_at
                || observation.completed_units == 0
                || observation.total_tokens == 0
            {
                return Err(BenchmarkError::InvalidWork(observation.id.clone()));
            }
            if observation.integration_latency_milliseconds.is_empty() {
                return Err(BenchmarkError::MissingIntegrationSamples(
                    observation.id.clone(),
                ));
            }
            by_mode
                .entry(observation.mode)
                .or_default()
                .push(observation);
            *workloads
                .entry(observation.mode)
                .or_default()
                .entry(observation.workload_id.clone())
                .or_default() += 1;
        }
        let single = by_mode
            .get(&BenchmarkMode::SingleWorker)
            .ok_or(BenchmarkError::MissingCohort)?;
        let pooled = by_mode
            .get(&BenchmarkMode::Pooled)
            .ok_or(BenchmarkError::MissingCohort)?;
        let single_workloads = workloads
            .get(&BenchmarkMode::SingleWorker)
            .ok_or(BenchmarkError::MissingCohort)?;
        if workloads.get(&BenchmarkMode::Pooled) != Some(single_workloads) {
            return Err(BenchmarkError::IncomparableWorkloads);
        }
        let single_metrics = aggregate(single);
        let pooled_metrics = aggregate(pooled);
        Ok(Self {
            schema_version: ROLLOUT_BENCHMARK_SCHEMA,
            workload_counts: single_workloads.clone(),
            pooled_relative_to_single: BenchmarkDelta {
                throughput_change_percent: percent_change(
                    single_metrics.completed_units_per_hour,
                    pooled_metrics.completed_units_per_hour,
                )
                .expect("single-worker throughput is positive"),
                tokens_per_unit_change_percent: percent_change(
                    single_metrics.tokens_per_completed_unit,
                    pooled_metrics.tokens_per_completed_unit,
                )
                .expect("single-worker token rate is positive"),
                conflict_rate_change_percent: percent_change(
                    single_metrics.conflicts_per_completed_unit,
                    pooled_metrics.conflicts_per_completed_unit,
                ),
                integration_latency_change_percent: percent_change(
                    single_metrics.average_integration_latency_milliseconds,
                    pooled_metrics.average_integration_latency_milliseconds,
                )
                .expect("single-worker integration latency is positive"),
                human_interruption_rate_change_percent: percent_change(
                    single_metrics.human_interruptions_per_completed_unit,
                    pooled_metrics.human_interruptions_per_completed_unit,
                ),
            },
            single_worker: single_metrics,
            pooled: pooled_metrics,
        })
    }

    pub fn to_markdown(&self) -> String {
        let delta = &self.pooled_relative_to_single;
        format!(
            "# Autonomous worker rollout benchmark\n\n\
             This report compares matched workload observations. Positive throughput change is better; negative changes are better for token cost, conflict rate, integration latency, and human interruption rate. `n/a` means the single-worker baseline was zero.\n\n\
             ## Cohort metrics\n\n\
             | Metric | Single worker | Pooled | Pooled change |\n\
             | --- | ---: | ---: | ---: |\n\
             | Completed units per hour | {:.2} | {:.2} | {} |\n\
             | Tokens per completed unit | {:.2} | {:.2} | {} |\n\
             | Conflicts per completed unit | {:.4} | {:.4} | {} |\n\
             | Average integration latency (ms) | {:.2} | {:.2} | {} |\n\
             | P95 integration latency (ms) | {} | {} | — |\n\
             | Human interruptions per completed unit | {:.4} | {:.4} | {} |\n\n\
             ## Sample coverage\n\n\
             - Single-worker observations: {}\n\
             - Pooled observations: {}\n\
             - Completed units: {} single-worker, {} pooled\n\
             - Integration samples: {} single-worker, {} pooled\n\
             - Matched workload counts: {}\n",
            self.single_worker.completed_units_per_hour,
            self.pooled.completed_units_per_hour,
            render_percent(Some(delta.throughput_change_percent)),
            self.single_worker.tokens_per_completed_unit,
            self.pooled.tokens_per_completed_unit,
            render_percent(Some(delta.tokens_per_unit_change_percent)),
            self.single_worker.conflicts_per_completed_unit,
            self.pooled.conflicts_per_completed_unit,
            render_percent(delta.conflict_rate_change_percent),
            self.single_worker.average_integration_latency_milliseconds,
            self.pooled.average_integration_latency_milliseconds,
            render_percent(Some(delta.integration_latency_change_percent)),
            self.single_worker.p95_integration_latency_milliseconds,
            self.pooled.p95_integration_latency_milliseconds,
            self.single_worker.human_interruptions_per_completed_unit,
            self.pooled.human_interruptions_per_completed_unit,
            render_percent(delta.human_interruption_rate_change_percent),
            self.single_worker.observation_count,
            self.pooled.observation_count,
            self.single_worker.completed_units,
            self.pooled.completed_units,
            self.single_worker.integration_samples,
            self.pooled.integration_samples,
            self.workload_counts
                .iter()
                .map(|(workload, count)| format!("{workload} × {count}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn aggregate(observations: &[&BenchmarkObservation]) -> BenchmarkMetrics {
    let completed_units = observations
        .iter()
        .map(|observation| observation.completed_units)
        .sum::<u64>();
    let elapsed_seconds = observations
        .iter()
        .map(|observation| {
            (observation.finished_at - observation.started_at).num_milliseconds() as f64 / 1_000.0
        })
        .sum::<f64>();
    let total_tokens = observations
        .iter()
        .map(|observation| observation.total_tokens)
        .sum::<u64>();
    let conflict_events = observations
        .iter()
        .map(|observation| observation.conflict_events)
        .sum::<u64>();
    let human_interruptions = observations
        .iter()
        .map(|observation| observation.human_interruptions)
        .sum::<u64>();
    let mut latencies = observations
        .iter()
        .flat_map(|observation| observation.integration_latency_milliseconds.iter().copied())
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let average_latency =
        latencies.iter().map(|value| *value as f64).sum::<f64>() / latencies.len() as f64;
    let p95_index = ((latencies.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(latencies.len() - 1);
    BenchmarkMetrics {
        observation_count: observations.len(),
        completed_units,
        elapsed_seconds,
        completed_units_per_hour: completed_units as f64 * 3_600.0 / elapsed_seconds,
        total_tokens,
        tokens_per_completed_unit: total_tokens as f64 / completed_units as f64,
        conflict_events,
        conflicts_per_completed_unit: conflict_events as f64 / completed_units as f64,
        integration_samples: latencies.len(),
        average_integration_latency_milliseconds: average_latency,
        p95_integration_latency_milliseconds: latencies[p95_index],
        human_interruptions,
        human_interruptions_per_completed_unit: human_interruptions as f64 / completed_units as f64,
    }
}

fn percent_change(baseline: f64, candidate: f64) -> Option<f64> {
    (baseline != 0.0).then_some((candidate - baseline) * 100.0 / baseline)
}

fn render_percent(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".into(), |value| format!("{value:+.2}%"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(
        id: &str,
        workload_id: &str,
        mode: BenchmarkMode,
        seconds: i64,
        tokens: u64,
        conflicts: u64,
        latencies: &[u64],
        interruptions: u64,
    ) -> BenchmarkObservation {
        let started_at = DateTime::parse_from_rfc3339("2026-10-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        BenchmarkObservation {
            id: id.into(),
            workload_id: workload_id.into(),
            mode,
            started_at,
            finished_at: started_at + chrono::Duration::seconds(seconds),
            completed_units: 2,
            total_tokens: tokens,
            conflict_events: conflicts,
            integration_latency_milliseconds: latencies.to_vec(),
            human_interruptions: interruptions,
        }
    }

    #[test]
    fn compares_matched_single_worker_and_pooled_observations() {
        let report = RolloutBenchmarkReport::compare(&BenchmarkInput {
            observations: vec![
                observation(
                    "single-a",
                    "workload-a",
                    BenchmarkMode::SingleWorker,
                    3_600,
                    20_000,
                    1,
                    &[120_000, 180_000],
                    2,
                ),
                observation(
                    "single-b",
                    "workload-b",
                    BenchmarkMode::SingleWorker,
                    3_600,
                    18_000,
                    0,
                    &[90_000, 150_000],
                    1,
                ),
                observation(
                    "pooled-a",
                    "workload-a",
                    BenchmarkMode::Pooled,
                    1_800,
                    11_000,
                    1,
                    &[90_000, 120_000],
                    1,
                ),
                observation(
                    "pooled-b",
                    "workload-b",
                    BenchmarkMode::Pooled,
                    1_800,
                    10_000,
                    0,
                    &[60_000, 90_000],
                    0,
                ),
            ],
        })
        .unwrap();
        assert_eq!(report.schema_version, ROLLOUT_BENCHMARK_SCHEMA);
        assert_eq!(report.single_worker.completed_units_per_hour, 2.0);
        assert_eq!(report.pooled.completed_units_per_hour, 4.0);
        assert_eq!(report.single_worker.tokens_per_completed_unit, 9_500.0);
        assert_eq!(report.pooled.tokens_per_completed_unit, 5_250.0);
        assert_eq!(
            report
                .single_worker
                .average_integration_latency_milliseconds,
            135_000.0
        );
        assert_eq!(report.pooled.p95_integration_latency_milliseconds, 120_000);
        assert_eq!(
            report.pooled_relative_to_single.throughput_change_percent,
            100.0
        );
        assert!(report.to_markdown().contains("Matched workload counts"));
    }

    #[test]
    fn rejects_unmatched_or_incomplete_measurements() {
        let mut observations = vec![observation(
            "single-a",
            "workload-a",
            BenchmarkMode::SingleWorker,
            3_600,
            20_000,
            0,
            &[100],
            0,
        )];
        assert_eq!(
            RolloutBenchmarkReport::compare(&BenchmarkInput {
                observations: observations.clone()
            }),
            Err(BenchmarkError::MissingCohort)
        );
        observations.push(observation(
            "pooled-b",
            "workload-b",
            BenchmarkMode::Pooled,
            1_800,
            10_000,
            0,
            &[100],
            0,
        ));
        assert_eq!(
            RolloutBenchmarkReport::compare(&BenchmarkInput { observations }),
            Err(BenchmarkError::IncomparableWorkloads)
        );
    }
}
