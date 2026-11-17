# Architecture

## What the detector does

Production stations (mixers, extruders, kilns, presses, packers) write free-text logs. Every
vendor uses its own line layout, timestamp notation and severity words, and the layouts change
with firmware updates. The detector reads those lines from a Kinesis stream, works out each
station's behaviour per time window, learns what normal looks like **per station**, and reports
windows that do not fit.

No labelled data and no per-station rules are needed. Configuration only names the numbers worth
reading in each station type's logs (see [CONFIGURATION.md](CONFIGURATION.md)).

## Data flow

```
                 ┌──────────────────────── Managed Service for Apache Flink ────────────────────────┐
 Kinesis         │                                                                                   │
 <env>-…-logs ──►│ source: event time ──► parse ──► 10 s windows ──► feature ──┬─► RCF score ──► alert │
 (raw lines)     │ + watermarks per shard    │       per station      vector    │   per station   policy │
                 │                           │            │                    └─► rules ─────────┐ │   │
                 │                           ▼            ▼                      (silence, stuck)  ▼ ▼   │
                 │                    dead letters ◄── late events     DynamoDB <env>-…-scores  Kinesis │
                 └───────────────────────────┼──────────────────────────────────────────────────────┼──┘
                     ▼                                                                       ▼
            Kinesis <env>-…-dead-letters                                  Kinesis <env>-…-alerts
                                                                                             │
                                                                       EventBridge Pipe ─────┤
                                                                                             ▼
                                                                                 SNS <env>-…-anomalies
```

| Stage | Class | What it does |
|---|---|---|
| Source | `RawLogDeserializationSchema`, `RawLogTimestampAssigner` | One Kinesis record → one `RawLog` per non-empty line (shippers batch lines). Keeps shard, sequence number and arrival time for traceability. Never throws. Event time and watermarks are assigned **here, per shard** (see below). |
| Parse | `LogParser`, `LogParseFunction` | Finds the station id (`<TYPE>-<n>`, case-insensitive, first token whose type has a profile), the timestamp (epoch ms/s, ISO-8601 with or without zone, syslog), the severity and each configured metric by its label. Position-independent, so new layouts usually need no change. |
| Windows | `LogEventAggregator`, `FeatureVectorFunction` | Tumbling event-time windows per station, aggregated incrementally (O(1) state per window). Feature vector = `[events, warnings, errors, mean(metric₁) … mean(metricₙ)]`. Metrics missing from a window carry their last value forward. |
| Score | `RcfScoringFunction`, `ModelCodec` | One Thresholded Random Cut Forest per station: internal shingling (looks at the last *k* windows together), per-feature normalisation, adaptive threshold. Emits score, **grade** (0 = normal … 1), the expected values and each feature's share of the anomaly. |
| Alert policy | `AlertFunction` | Turns graded windows into `ANOMALY` alerts: minimum grade, no alerts while a new model settles, one alert per station per cool-down unless the grade escalates. Alert id `<station>#<windowEnd>` is deterministic. |
| Rules | `StationRulesFunction` | `SILENCE` (a station stops logging) and `STUCK_SENSOR` (a reading that used to vary freezes): failure modes that look normal to the model. |
| Sinks | `Sinks`, `ScoredRecordConverter` | Every scored window → DynamoDB; alerts → Kinesis; dead letters → Kinesis. |

The topology is built by `LogAnomalyPipeline.build(...)` independently of sources and sinks, so
the exact production graph runs in `LogAnomalyPipelineTest` on an embedded cluster.

## Why Random Cut Forest, plus rules

* Unsupervised and streaming: learns continuously, adapts to slow process changes (time decay),
  needs no training phase outside the job.
* Cheap per point, small state (a few hundred KB per station with the default parameters).
* Explains itself: attribution names the features behind an anomaly, which goes into the alert
  text ("zone_temp=910.2 (expected 842.1)").
* The *thresholded* variant (`randomcutforest-parkservices`) adds normalisation and an adaptive
  threshold, so a single configuration works for features of very different scales and stations
  of very different noise levels. The sample's original raw `RandomCutForest` needed a hand-tuned
  score threshold per deployment.
* What RCF cannot see: values that are *normal but wrong*. A frozen sensor keeps reporting a
  plausible value, and a silent station produces no data to score. These known failure modes get
  explicit rules. On the synthetic benchmark the rules raised recall from about 65 % to 89 % without a
  single false alert ([DETECTION_QUALITY.md](DETECTION_QUALITY.md)).

## Resilience design

The application must keep running through bad input, keep its learned models through failures
and deployments, and never silently lose data. The decisions that achieve this:

### Bad input never fails the job
* The deserializer decodes any bytes; invalid UTF-8 becomes replacement characters.
* The parser returns a result for every line instead of throwing. Unusable lines (oversized,
  binary, no timestamp) go to the **dead-letter stream** with the reason, the truncated line,
  and shard + sequence number for replay. Lines that belong to no configured station (gateway
  heartbeats, HMI chatter) are only counted (`unattributedRecords`); they can be dead-lettered
  with `dead.letter.unattributed=true`.
* A feature vector with non-finite values is dead-lettered; a model that throws is discarded,
  counted (`modelResets`) and relearned. One poisoned record must never put the job in a restart
  loop, because a restart loop stops detection for every station.

### Time is handled defensively
* Event time comes from the line. A line without a usable timestamp uses the Kinesis arrival time.
* **Watermarks are tracked per shard**, by assigning event time in the source. When the job
  catches up on a backlog, the reader may read far ahead in one shard. With a single watermark
  after the source, every record of the other shards would then be "late" and dropped. This was
  found by the LocalStack end-to-end test, which dropped half the events before the fix.
* A timestamp more than `max.future.skew.ms` ahead of its arrival time is not trusted: one
  controller with a broken clock would otherwise drag the watermark forward and make every
  other station's data "late".
* Watermark idleness keeps windows closing when some shards go quiet.
* Events behind the watermark go to the dead-letter stream as `LATE_EVENT` instead of vanishing.

### Model state is exact and portable
* The model is written to keyed state **after every update**. Models change once per station
  per window (every 10 s by default), so this is cheap, and it makes every checkpoint contain
  exactly the models that match the consumed input. After a failure, no training data is lost
  or learned twice. (The original sample saved models on a timer, so a restore could roll a model
  back by up to the timer interval while the input was not rolled back.)
* Models are stored as `byte[]` with an explicit format version and dimension count
  (`ModelCodec`), never as a Kryo-serialised object graph. Kryo state breaks on library and Flink
  upgrades; with the versioned format an incompatible or corrupt model is detected, logged,
  counted and relearned instead of making the snapshot unrestorable.
* A station profile change (metrics added or removed) changes the vector length; the old model
  is detected as incompatible and reset for that station only.
* Deserialised models live in a bounded LRU cache (`model.cache.max.size`); since state is always
  current, eviction is always safe and memory stays bounded however many stations appear.
* Optional state TTL (`model.state.ttl.hours`) removes models of decommissioned stations.
* Models are **seeded per station**, so the same input always gives the same scores. A replay
  after a restore reaches the same decisions, and quality evaluations are reproducible.

### Rules that do not depend on timing luck
* Silence is detected by an event-time timer while the station is quiet, **and** by the gap
  before its next window when it comes back. When the watermark jumps during a catch-up, the
  station's later windows can arrive before the watermark that would fire the timer. Both paths
  emit the same alert id and never both alert.
* The end-of-input watermark (bounded replays, stop with drain) is not taken as silence.

### Delivery guarantees
| Output | Guarantee | How duplicates are handled |
|---|---|---|
| DynamoDB scores | at-least-once writes, **effectively exactly-once** | Item key `(station_id, window_end)` comes from the data; a replayed window overwrites itself. |
| Alerts stream | at-least-once | `alertId` is deterministic; consumers de-duplicate on it. The policy runs in event time on seeded models, so a replay makes the same decisions. |
| Dead letters | at-least-once | Carry shard + sequence number. |

Sinks retry failed writes and apply back-pressure rather than drop records (`failOnError=false`
retries in the async sink). If an output is unavailable for long, the job slows down, the
source lag grows and the `lag` alarm fires. Nothing is lost while the input stream retains the data
(`log_retention_hours`, 72 h by default).

### Operations
* Snapshots on every stop/update, restore from the latest snapshot on start: trained models
  survive deployments and scaling.
* Stable operator `uid`s (`kinesis-source`, `parse`, `features`, `rcf`, `alerts`, `rules`, sinks): renaming one
  drops its state on the next restore. Treat them as a public contract.
* Custom counters in the `kinesisAnalytics` metric group are exported to CloudWatch and used by
  the alarms and dashboard (see [OPERATIONS.md](OPERATIONS.md)).
* Configuration is validated at start-up with messages that name the offending property, so a
  bad deployment fails immediately and visibly instead of at the first record.

## Repository layout

```
src/main/java/cloud/hermoso/flink_logs_processor
├── DataStreamJob.java          entry point: config, Kinesis source, sinks
├── LogAnomalyPipeline.java     the topology, and its configuration
├── ApplicationConfigUtils.java typed, validated access to runtime properties
├── config/                     station profiles (which numbers to read per station type)
├── source/                     Kinesis record → log lines
├── parser/                     log line → LogEvent, dead-lettering
├── features/                   windows → feature vectors
├── operator/                   Random Cut Forest scoring and model state
├── alert/                      alert policy and summaries
├── rules/                      silence and stuck-sensor rules
├── sink/                       DynamoDB item mapping, sink builders
├── monitor/                    CloudWatch-exported counters
└── model/                      POJOs flowing between stages
src/test/…/tools/OfflineDetection.java   runs the topology over a log file, for tuning
log-generator/                  synthetic plant logs + detection evaluator (separate Python app)
deploy/tofu/                    OpenTofu deployment
local/                          LocalStack for local runs
docs/                           this documentation
```
