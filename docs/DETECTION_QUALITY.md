# Detection quality

How well the detector finds problems, how that was measured, and how to measure it again after
changing anything that affects detection.

## Results with the shipped defaults

Three synthetic datasets from `log-generator` (seeds 99, 7 and 31). Each has 30 minutes of
clean history, then 3 hours with about one random anomaly per station per hour, across 7 stations.
Seed 31 was held out: it was not used to choose the parameters.

| Anomaly kind | What happens in the logs | Detected | By |
|---|---|---|---|
| Silence | Station stops logging for 2–5 min | **12 / 12** | rule `SILENCE` |
| Error burst | Controller floods errors for 30–90 s | **9 / 9** | model |
| Stuck sensor | One reading freezes for 3–7 min | **12 / 13** | rule `STUCK_SENSOR` |
| Spike | One reading jumps 9σ for 1–3 min | 9 / 12 | model |
| Drift | One reading creeps 12σ away over 5–10 min | 9 / 11 | model |
| **Total** | | **51 / 57 (89 %)** | |

False alerts: 19 over 63 station-hours, i.e. **about one per station every 3.3 hours**. All 19
came from the model; the rules raised none.

| Dataset | Recall | Precision | False alerts / station-hour |
|---|---|---|---|
| seed 99 | 95 % | 89 % | 0.14 |
| seed 7 | 91 % | 79 % | 0.38 |
| seed 31 (holdout) | 79 % | 67 % | 0.38 |

Caveat: the data is synthetic. Real logs will have other noise and failure patterns, so treat
these numbers as a regression baseline and a sanity check, not a promise. Measuring on real,
labelled incidents is task T-23 in [TASKS.md](TASKS.md).

## How the defaults were chosen

| Change | Effect (seeds 99 / 7) |
|---|---|
| Baseline (shingle 4, every grade alerts, plain cool-down) | Missed an incident after a weak false alert; ~0.7 false alerts per station-hour |
| Cool-down **escalation**: a stronger anomaly breaks through | Fixed the hidden incident |
| **Warm-up grace** of 60 windows before alerting | Removed the grade-1.0 false alerts right after a model starts |
| **Seeded models** (per station) | Runs became reproducible; previously identical runs differed by ~10 points of recall |
| **Rules** for silence and stuck sensors | Recall 70 % → 95 % and 61 % → 91 % |
| Grid over `anomaly.rate` × `min.grade` × `shingle.size` | Chose `shingle.size` 6, `min.grade` 0.5: false alerts 0.62 → 0.14 and 0.86 → 0.38 per station-hour at the same recall |

On the holdout seed the old settings got 93 % recall with 1.29 false alerts per station-hour;
the new ones get 79 % with 0.38. That is a deliberate choice: an alert stream people
ignore is worth less than one that misses some subtle spikes. To favour recall instead, lower
`min.grade` (see [CONFIGURATION.md](CONFIGURATION.md)).

## Where the remaining false alerts come from

Almost all are driven by the count features: `warnings=2 (expected 0)`,
`events=13 (expected 10)`. Routine warnings are rare but normal, and a window that happens to
contain two of them looks unusual to the model. Ideas for reducing this are tasks T-26 and T-27.

## Measuring it yourself

Offline evaluation runs the production topology over a log file (no Kinesis, no LocalStack) in
a few seconds, then scores the alerts against the generator's ground truth:

```bash
# 1. a dataset with labelled anomalies
cd log-generator
python3 -m loggen --sink file --file /tmp/plant.log --seed 7 --start 2026-11-17T00:00:00Z \
  --backfill 1800 --duration 10800 --speed 0 --anomaly-rate 1 --labels-file /tmp/labels.jsonl
cd ..

# 2. run the detector over it (extra arguments override single properties)
mvn -q test-compile dependency:build-classpath -Dmdep.outputFile=target/cp-test.txt -Dmdep.includeScope=test
java -cp "target/test-classes:target/classes:$(cat target/cp-test.txt)" \
  cloud.hermoso.flink_logs_processor.tools.OfflineDetection /tmp/plant.log /tmp/alerts.jsonl \
  Alerting.min.grade=0.5 ModelParameters-DEFAULT.shingle.size=6

# 3. score it
cd log-generator && python3 -m loggen.evaluate --labels /tmp/labels.jsonl --alerts /tmp/alerts.jsonl --hours 3
```

`loggen.evaluate` lists every injected anomaly as detected or missed, every false alert with its
summary, and the totals. An alert counts as a detection when it names the right station and
falls between the anomaly's start and two minutes after its end.

`local/e2e.sh` checks the same thing through the real Kinesis connector on LocalStack, with
a fixed scenario. It is slower, but it is the only check that covers per-shard watermarks.
