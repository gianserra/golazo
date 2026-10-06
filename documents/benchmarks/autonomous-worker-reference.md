# Autonomous worker rollout benchmark

This is a deterministic reference comparison for the benchmark pipeline, not a production performance claim. Replace the fixture observations with matched shadow-mode and pilot observations before using the result as a rollout gate.

The benchmark accepts only cohorts with the same workload multiset. Every observation must include positive elapsed time, completed work, measured token use, and at least one integration-latency sample. Rates are normalized per completed unit so a larger pooled cohort cannot look better merely because it attempted more work.

Generate this report as JSON or Markdown:

```sh
node scripts/run-cargo.mjs run --quiet --bin golazo-rollout-benchmark -- \
  rust-backend/coordination/fixtures/rollout-benchmark-reference.json

node scripts/run-cargo.mjs run --quiet --bin golazo-rollout-benchmark -- \
  rust-backend/coordination/fixtures/rollout-benchmark-reference.json --markdown
```

Positive throughput change is better. Negative changes are better for token cost, conflict rate, integration latency, and human interruption rate. `n/a` means the single-worker baseline was zero.

## Reference cohort metrics

| Metric | Single worker | Pooled | Pooled change |
| --- | ---: | ---: | ---: |
| Completed units per hour | 2.00 | 4.00 | +100.00% |
| Tokens per completed unit | 9500.00 | 5250.00 | -44.74% |
| Conflicts per completed unit | 0.2500 | 0.2500 | +0.00% |
| Average integration latency (ms) | 135000.00 | 90000.00 | -33.33% |
| P95 integration latency (ms) | 180000 | 120000 | — |
| Human interruptions per completed unit | 0.7500 | 0.2500 | -66.67% |

## Reference coverage

- Single-worker observations: 2
- Pooled observations: 2
- Completed units: 4 single-worker, 4 pooled
- Integration samples: 4 single-worker, 4 pooled
- Matched workloads: `independent-feature-pair-a` and `independent-feature-pair-b`

For a rollout decision, retain the raw observation file alongside the generated report and record the repository revision, model configuration, permission profile, worker concurrency, and acceptance thresholds used for that cohort.
