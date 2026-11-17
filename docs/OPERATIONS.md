# Operations runbook

Dashboard: CloudWatch dashboard `<env>-plant-anomaly` (`tofu output dashboard_url`).
All alarms notify the `<env>-plant-anomaly-ops` SNS topic when they fire and when they recover.

## Custom metrics

Exported by the job to CloudWatch, namespace `AWS/KinesisAnalytics`, dimension `Application`:

| Metric | Meaning |
|---|---|
| `parsedRecords` | Lines attributed to a station. |
| `unattributedRecords` | Valid lines without a known station: gateway chatter, or a **station type without a profile**. |
| `arrivalTimestampRecords` | Lines whose own timestamp was missing or untrusted; event time = Kinesis arrival time. |
| `deadLetterRecords` | Lines sent to the dead-letter stream. |
| `incompleteWindows` | Windows skipped because some metric of the station has never been seen yet. |
| `scoredWindows` | Windows scored by a model. |
| `anomalies` | Windows with grade > 0. |
| `alertsEmitted` / `alertsSuppressed` | Alerts sent / held back by the minimum grade, warm-up or cool-down. |
| `modelResets` | Models discarded and relearned (incompatible state or model error). |
| `rejectedFeatureVectors` | Feature vectors with non-finite values. |

## Alarms

### `<env>-plant-anomaly-downtime`: the job is not running
1. Console → Managed Apache Flink → application → status. `RUNNING` + downtime means it is restarting.
2. CloudWatch log group `/aws/kinesis-analytics/<env>-plant-anomaly`: search for `Exception` near
   the restart. Typical causes:
   * Configuration error (`Missing application parameter`, `Invalid value`): fix the tofu variables, apply.
   * Snapshot restore failure after an update (`state for operator ... cannot be mapped`): an
     operator uid changed. Roll back, or redeploy with `allow_non_restored_state = true` if the
     loss is intended.
   * AWS access errors (`AccessDenied`): IAM policy in `iam.tf`.
3. Nothing is lost while the job is down, as long as it is back before the input stream's
   retention runs out (`log_retention_hours`, 72 h by default).

### `-restarts`: crash loop
Same investigation as downtime. Bad input cannot cause this by design (it is dead-lettered).
Look for resource exhaustion (`OutOfMemoryError`: lower `model.cache.max.size` or raise
parallelism) or an unavailable dependency.

### `-failed-checkpoints`
Usually a symptom of back-pressure or a sink that cannot write. Check the dashboard for lag and
the logs for sink errors (DynamoDB throttling, Kinesis throughput). While checkpoints fail,
a restart reprocesses more input. Outputs are idempotent, so this costs time, not correctness.

### `-lag`: more than 10 minutes behind the logs
Detection is delayed by that much. Check `cpuUtilization` and `KPUs`:
* CPU high, autoscaling at its limit: raise `parallelism`, or reduce model cost
  (`number.of.trees`, `shingle.size`).
* CPU low: a sink is slow (throttled DynamoDB/Kinesis) or a shard is hot.
After a long outage, lag spikes while the job catches up. That is expected.

### `-no-scores`: running, but nothing scored for 15 minutes
* Input stream `IncomingRecords` is zero: the plant or the log shipper stopped sending. Not a
  detector problem, but nobody is watching the line right now.
* Input arrives but `parsedRecords` ≈ 0 and `unattributedRecords` is high: station ids are no
  longer recognised (renamed hosts, new format). Read a few lines from the input stream and
  compare them with the station profiles.
* `parsedRecords` > 0 but `incompleteWindows` high: a configured metric label never matches.
  Fix the label in `station_profiles`.

### `-dead-letters`: unusual number of unprocessable records
Read the dead-letter stream. Every record has `reason`, the truncated `raw` line, and shard +
sequence number of the source record:

| `reason` | Usual cause | Action |
|---|---|---|
| `NON_TEXT`, `OVERSIZED` | Binary data or a broken shipper | Fix the producer. |
| `NO_TIMESTAMP` | Records without arrival time (only in tests) | — |
| `LATE_EVENT` | Lines delayed more than `max.out.of.orderness.ms` | Raise it, or fix the shipper's buffering. |
| `INVALID_FEATURE_VECTOR`, `MODEL_ERROR` | Should not happen | Open a bug with the record. |

### `-alert-delivery`: the alert pipe fails
Check the Pipe in the EventBridge console. Usually the pipe role lost access (`sns:Publish`, KMS)
or the topic was removed. Alerts stay in the alerts stream for 7 days. Once fixed, the pipe
resumes from where it stopped.

## Common tasks

**A station shows false alerts all the time.** First check whether it really behaves differently
(compare `features` with `expected` in the scores table). If it is genuinely noisier, give it
its own parameters: `ModelParameters-<STATION-ID>` with a lower `anomaly.rate`, or raise
`min.grade` globally.

**A new station type comes online.** Add it to `station_profiles` and apply. Its stations are
counted as `unattributedRecords` until then. Its models train for ~20 minutes before alerting.

**Reset one station's model.** There is no per-key reset. Either wait for
`model.state.ttl.hours`, or reset all models with `restore_from_snapshot = false`. (Task T-28 in
TASKS.md tracks a per-station reset.)

**Replay a time range.** Stop the application, start it with
`source.init.position = AT_TIMESTAMP` *and* without snapshot restore. Models start from
scratch, so the first ~20 minutes of the replay are warm-up.

**Inspect a station's history.** Query the scores table by `station_id`, with `window_end` as the
range key (epoch milliseconds). Anomalies have `anomaly = true`.
