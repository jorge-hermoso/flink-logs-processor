# Plan and tasks

Goal: turn the keyed Random Cut Forest sample into a resilient, production-ready anomaly
detector for production-line logs, with infrastructure as code and a synthetic data source.

Status legend: ✅ done · 🟡 partly done · ⬜ open. Priorities for open work: **P1** before
production, **P2** soon after, **P3** when useful.

## Review of the starting point

Findings in the inherited code (commit `b99e491`) that shaped the plan:

| # | Finding | Impact | Addressed by |
|---|---|---|---|
| R1 | The only source was an in-job sine-wave generator; nothing read logs. | Not a log processor yet. | T-03, T-04 |
| R2 | Compiled for Java 21, which Managed Service for Apache Flink does not support. | Would not run on AWS. | T-02 |
| R3 | Models were copied to Flink state on a timer, while input offsets are checkpointed continuously. | After a failure, models silently rolled back by up to the timer interval and relearned data inconsistently. | T-09 |
| R4 | `RandomCutForestState` was stored as a Flink value (Kryo fallback). | Snapshots break on library or Flink upgrades. | T-09 |
| R5 | Per-key overrides never applied: the key was extracted by removing `ModelParameters:` from groups named `ModelParameters-K1`. | The `K1` configuration in the dev file was silently ignored. | T-15 |
| R6 | One record with an unexpected number of values threw in `processElement`. | A single bad record would put the job in a restart loop. | T-05 |
| R7 | Unbounded in-memory model map and timer-flag map, never cleaned. | Memory grows with the number of keys. | T-10 |
| R8 | Raw RCF score against a fixed threshold, counted in a metric only. SNS alerts and the DynamoDB sink described in the README did not exist. | No detection output anyone could act on. | T-08, T-12, T-14 |
| R9 | The only test mixed JUnit 4 `@Test` with JUnit 5 `@Disabled` and asserted nothing. | No test coverage. | T-19 |
| R10 | Stale log4j logger for the sample's package; a build-output `META-INF/MANIFEST.MF` in the repository root. | Noise. | T-01 |

## Done in this iteration

| ID | Task | Notes |
|---|---|---|
| ✅ T-01 | Review the sample; remove sample-only code and stray build output | Findings above. |
| ✅ T-02 | Target Java 17 | `pom.xml`. |
| ✅ T-03 | Read raw log lines from Kinesis | `KinesisStreamsSource`; one record may carry several newline-separated lines. |
| ✅ T-04 | Format-agnostic parser with configurable station profiles | Timestamps (epoch, ISO, syslog), severities, metrics by label. No per-format code. |
| ✅ T-05 | Never fail on bad input | Dead-letter stream with reason and provenance; parser never throws; model errors reset the model. |
| ✅ T-06 | Event-time windows and feature vectors | Incremental aggregation; last-value carry-forward; late events dead-lettered. |
| ✅ T-07 | **Per-shard watermarks** | Found by the LocalStack end-to-end run: with watermarks after the source, catching up on a backlog made half the events late. Event time is now assigned in the source. |
| ✅ T-08 | Thresholded RCF | Normalisation, adaptive threshold, grade, expected values, attribution. |
| ✅ T-09 | Exact, versioned model state | Written after every update; own byte format with version + dimensions; incompatible state is reset, not fatal. |
| ✅ T-10 | Bounded model cache, optional state TTL | LRU cache; models of retired stations can expire. |
| ✅ T-11 | Deterministic models | Seeded per station: replays reach the same decisions; evaluations are reproducible. |
| ✅ T-12 | Alert policy | Minimum grade, warm-up grace, cool-down with escalation, deterministic alert ids. |
| ✅ T-13 | Rules for silence and stuck sensors | Recall 70 % → 95 % on the synthetic benchmark. Silence also caught when watermarks jump (backlog). |
| ✅ T-14 | Outputs | DynamoDB scores (idempotent key, TTL), alert stream, SNS via EventBridge Pipe (no code), dead-letter stream. |
| ✅ T-15 | Validated configuration | Defaults for everything optional; partial per-type / per-station model overrides; errors name the property. |
| ✅ T-16 | Observability | Custom metrics; alarms for downtime, crash loop, failed checkpoints, lag, no output, dead letters, alert delivery; dashboard; runbook. |
| ✅ T-17 | OpenTofu deployment | Streams, table, bucket, Flink app, least-privilege IAM, Pipe, SNS, alarms, dashboard, optional Fargate generator. `tofu test` with a mocked provider. |
| ✅ T-18 | Synthetic log generator (separate app) | 5 vendor dialects, chatter, garbage, late delivery, 5 anomaly kinds, labelled ground truth, Kinesis sink with partial-failure retries, Docker image. |
| ✅ T-19 | Tests | 40 Java tests (unit, operator harness incl. snapshot/restore, embedded pipeline, generator contract), 17 Python tests, 4 tofu tests, LocalStack end-to-end, offline detection evaluation. |
| ✅ T-20 | Documentation | Architecture, configuration, deployment, operations, local development, detection quality, this plan. |
| ✅ T-21 | CI | Builds and tests the job, the generator and the tofu template on every push and pull request. |

## Open

| ID | Pri | Task | Why / acceptance |
|---|---|---|---|
| ⬜ T-22 | P1 | **First deployment to a real AWS dev account** | The mocks cannot prove IAM policies are sufficient, that the Pipe's input template renders as intended, or that alarms see the custom metrics with `metrics_level = APPLICATION`. Done when the generator → detector → SNS e-mail path works and every alarm has been triggered once. |
| ⬜ T-23 | P1 | **Evaluate on real, labelled incidents** | Detection numbers are from synthetic data. Collect a few weeks of real logs with known incidents; run the offline evaluation; tune station profiles and parameters. |
| ⬜ T-24 | P1 | Snapshot compatibility check in CI | Restore a savepoint from the previous release before merging, so a renamed `uid` or changed state type is caught before deployment. |
| ⬜ T-25 | P1 | Run `local/e2e.sh` in CI | Needs Docker in the runner; it is the only automated check of per-shard watermarks. |
| ⬜ T-26 | P2 | Fewer count-driven false alerts | Remaining false alerts come from `events`/`warnings` (routine warnings). Options: variance-stabilising transform of counts, per-feature tolerance (`ignoreNearExpected*` in TRCF), or splitting known-routine messages out. Measure with the offline evaluation. |
| ⬜ T-27 | P2 | Per-station tuning workflow | Per-station precision/recall from the evaluation tool, and documented overrides for noisy stations. |
| ⬜ T-28 | P2 | Per-station model reset | Today only all models can be reset. Add an admin control stream (broadcast state) to reset one station after maintenance or recalibration. |
| ⬜ T-29 | P2 | Load and sizing test | Hundreds of stations: KPUs, checkpoint size and duration, model cache size, DynamoDB write cost. |
| ⬜ T-30 | P2 | Security hardening | Customer-managed KMS keys for streams and table, Flink in a VPC with endpoints, permissions boundary for the roles. |
| ⬜ T-31 | P2 | Delivery pipeline | Build the JAR and image once, promote dev → prod with plan review; remote state per environment. |
| ⬜ T-32 | P3 | Alert enrichment and routing | Station owner, runbook link, severity; route to chat or paging tools by station and type. |
| ⬜ T-33 | P3 | Operator feedback loop | Let operators mark alerts as false or true; use it for tuning and as labels for T-23. |
| ⬜ T-34 | P3 | Score dashboards | Per-station history from the scores table (e.g. Grafana or QuickSight). |
| ⬜ T-35 | P3 | Disaster recovery drill | Practise restoring from a named snapshot and rolling back an application version; time it. |

## Decisions

* **Rules next to the model.** RCF finds unknown unknowns, but a frozen sensor or a silent
  station looks normal to it. Known failure modes get explicit, cheap and explainable rules.
* **Precision over recall by default.** `min.grade` 0.5 trades some subtle spikes for about three
  times fewer false alerts. An ignored alert stream catches nothing. Tunable per deployment.
* **Effectively-once outputs instead of exactly-once sinks.** Idempotent keys (scores) and
  deterministic ids (alerts) give the same result as transactional sinks, without their latency
  and coupling to checkpoints.
* **Station profiles as configuration, not code.** New layouts usually need no change; new
  metrics are a tofu variable.
* **EventBridge Pipe for notifications.** Alert stream → SNS with no function to deploy,
  patch or monitor.
* **Generator in Python, separate from the job.** No shared code, so the parser is tested against
  logs it had no hand in producing. The contract test pins the two together.
