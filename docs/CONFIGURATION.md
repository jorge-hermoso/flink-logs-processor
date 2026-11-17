# Configuration reference

The job reads *property groups*: a group id and a map of string properties. On AWS they come
from the Managed Service for Apache Flink application configuration (rendered by
`deploy/tofu/main.tf` from tofu variables). Locally they come from
`src/main/resources/flink-application-properties-dev.json` (override the resource with
`-Dapp.properties.resource=<name>`).

Everything is validated at start-up. A missing or invalid value stops the job immediately with a
message naming the group and property.

Properties starting with `aws.` in the source and sink groups are passed to the AWS SDK clients
(`aws.region`, `aws.endpoint`, `aws.credentials.provider`, ...; see the
[Flink AWS connector options](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kinesis/)).
On AWS only `aws.region` is needed: credentials come from the application's IAM role.

## Inputs and outputs

| Group | Property | Default | Description |
|---|---|---|---|
| `Source` | `stream.arn` | *required* | ARN of the Kinesis stream with raw log lines. |
| | `source.init.position` | `LATEST` | `LATEST`, `TRIM_HORIZON` or `AT_TIMESTAMP`. Only used when the job starts **without** state; otherwise it resumes from the snapshot. |
| `AlertStream` | `stream.name` | *required* | Kinesis stream for alerts. |
| `DeadLetterStream` | `stream.name` | *required* | Kinesis stream for unprocessable records. |
| `ScoresTable` | `table.name` | *required* | DynamoDB table for scored windows (key `station_id` S, `window_end` N). |
| | `ttl.days` | `30` | Items get `expires_at` = window end + this; `0` disables expiry. Enable TTL on `expires_at` in the table. |

## Station profiles: which numbers to read

One group per station type, `Station-<TYPE>` (2 to 16 letters, case-insensitive). Each property
`metric.<name>` gives the **label** that precedes the number in the log text:

```json
{ "PropertyGroupId": "Station-KILN",
  "PropertyMap": { "metric.zone_temp": "zone temp reading", "metric.o2": "o2" } }
```

* A line is attributed to a station when it contains `<TYPE>-<number>` (or `<type>_<number>`, any
  case) and `<TYPE>` has a profile. Station ids are normalised to `KILN-01`.
* A metric matches its label, not preceded by a letter or digit, followed by optional `=`, `:` or
  a quote, then a number (`-61.2`, `1.5e3`). Spaces in the label match any whitespace.
  `o2` matches `o2=3.1%` and `o2: 3.1` but not `co2=400`.
* Metrics are ordered by name to build the feature vector:
  `[events, warnings, errors, <metrics sorted by name>]`.
* **Adding or removing a metric resets the models of that type's stations.** The old model has
  a different number of dimensions; it is discarded, counted in `modelResets` and relearned.
  Changing only a label does not reset anything.
* At least one profile is required.

## Parser

| Group `Parser` | Default | Description |
|---|---|---|
| `max.line.length` | `16384` | Longer lines are dead-lettered as `OVERSIZED`. |
| `max.future.skew.ms` | `300000` | A line timestamp further than this ahead of its Kinesis arrival time is replaced by the arrival time (protects the watermark from controllers with wrong clocks). |
| `dead.letter.unattributed` | `false` | Also dead-letter lines without a known station. Normally only counted. |

Timestamps recognised: epoch milliseconds or seconds at the start of the line; ISO-8601
(`T` or space, `.` or `,` fraction, optional `Z`/offset, **UTC assumed when absent**); syslog
`Mmm dd HH:MM:SS` (year from the arrival time, December lines arriving in January go to the
previous year). Severity: a `|W|`-style field, else the first level word (`warn`, `error`,
`crit`, `fatal`, `alarm`, ...).

## Features

| Group `Features` | Default | Description |
|---|---|---|
| `window.size.ms` | `10000` | Window length. Shorter detects faster, but counts get noisier. |
| `max.out.of.orderness.ms` | `30000` | How late a line may arrive and still count. Larger values delay every result by as much. |
| `idle.timeout.ms` | `60000` | A shard without data for this long stops holding back the watermark. |

## Model

| Group `RcfOperator` | Default | Description |
|---|---|---|
| `model.cache.max.size` | `1000` | Deserialised models kept in memory per parallel task. Others are reloaded from state. |
| `model.state.ttl.hours` | `0` | Drop the model of a station silent for longer than this. `0` keeps models for ever. |

Model hyper-parameters: group `ModelParameters-DEFAULT`, overridden per station type
(`ModelParameters-KILN`) or per station (`ModelParameters-KILN-02`). An override only needs the
properties it changes; the rest comes from `DEFAULT`, then from the built-in defaults.

| Property | Default | Description |
|---|---|---|
| `shingle.size` | `6` | Consecutive windows the model looks at together. Larger catches slower patterns (drifts, oscillations), at the cost of state and CPU. |
| `number.of.trees` | `50` | More trees give steadier scores, at the cost of CPU and state. |
| `sample.size` | `256` | Points each tree remembers. With the time decay, this is the model's memory. |
| `output.after` | `64` | Windows observed before scoring starts (≤ `sample.size`). |
| `anomaly.rate` | `0.005` | Expected share of anomalous windows; drives the adaptive threshold. |
| `time.decay` | `0` (= 1 / (10 × `sample.size`)) | How quickly old behaviour is forgotten. |

Changing hyper-parameters of an existing station applies to its next fresh model only; a
restored model keeps the parameters it was built with. Use `model.state.ttl.hours`, a new station
profile, or a restart without snapshot to rebuild models.

## Alerting

| Group `Alerting` | Default | Description |
|---|---|---|
| `min.grade` | `0.5` | Minimum anomaly grade (0–1) for an alert. Every window is stored with its grade regardless. |
| `cooldown.ms` | `300000` | After an alert, no new alert for the same station for this long (event time), **unless the grade is higher** than the last alert's. |
| `warmup.windows` | `60` | No alerts until a model has scored this many windows. A new model's threshold over-reacts. |

## Rules

Explicit checks for failure modes the model cannot see (see ARCHITECTURE.md).

| Group `Rules` | Default | Description |
|---|---|---|
| `silence.after.ms` | `60000` | A station that logged before and is silent for this long (event time) raises a `SILENCE` alert. `0` disables. |
| `stuck.windows` | `12` | A metric that has varied before and repeats exactly the same window mean this many times raises a `STUCK_SENSOR` alert. `0` disables. |

## Defaults in practice

With 10-second windows: the model starts scoring after ~12 minutes (`output.after` 64 +
`shingle.size` 6) and alerting starts ~10 minutes later (`warmup.windows` 60). The defaults were
chosen with the offline evaluation in [DETECTION_QUALITY.md](DETECTION_QUALITY.md); re-run it
when tuning. With snapshots
enabled this happens once per station, not on every deployment.
