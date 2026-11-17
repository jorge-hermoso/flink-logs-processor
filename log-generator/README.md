# loggen: synthetic production logs

Generates unstructured logs for the **Zorblatt thermoplasmic sprocket line**, a fictional plant,
with anomalies whose times are written to a ground-truth file. It is a separate application
from the detector, sharing no code with it, so the detector's parser is tested against logs it
had no hand in producing.

## The plant

| Station | Vendor dialect | Sample line |
|---|---|---|
| `MIXER-01` | ISO-8601 + logfmt | `2026-11-17T08:00:01.034Z INFO  mixer-ctl MIXER-01 batch=B00000 visc=1521.9cP temp=64.5C agitator_rpm=118` |
| `EXTRUDER-01/02` | bracketed, comma millis, no zone | `[2026-11-17 08:00:01,848] EXTRUDER-01 \| INFO \| melt pressure 143.5 bar, screw torque 80.3 Nm, barrel temp 219.8 C` |
| `KILN-01` | syslog, no year, lower-case host | `Nov 17 08:00:02 kiln-01 kilnd[421]: <info> zone temp reading 841.3C (setpoint 840.0C) o2=3.11% belt 1.22 m/min` |
| `PRESS-01/02` | epoch millis, pipe separated | `1794902402368\|PRESS-01\|I\|cycle 2 done; peak_force=311.8kN cycle_time=4.27s hyd_press=182.5bar` |
| `PACKER-01` | JSON envelope, free-text message | `{"ts":"2026-11-17T08:00:01.303Z","unit":"PACKER-01","lvl":"info","msg":"rate 42.1 units/min, rejects 0.83 %, vacuum -60.6 kPa"}` |

Readings are a base level, a slow wave and autocorrelated noise. The plant also produces
routine operator events and warnings, chatter from sources that are not stations (SCADA gateway
heartbeats, MES sync, HMI actions, multi-line Java stack traces, shift banners), a small share
of binary garbage, and lines delivered up to `--max-delay` seconds late and out of order.

## Anomalies

| Kind | Effect | Duration |
|---|---|---|
| `spike` | One reading jumps 9σ and stays there; the controller logs warnings | 1–3 min |
| `drift` | One reading creeps up to 12σ away | 5–10 min |
| `error_burst` | 3–8 error lines per telemetry cycle | 30–90 s |
| `stuck` | One reading freezes on its last value | 3–7 min |
| `silence` | The station stops logging | 2–5 min |

Random anomalies arrive at `--anomaly-rate` per station per hour (never overlapping on one
station). Specific ones can be scheduled with `--inject STATION:KIND:START_S:DURATION_S[:METRIC]`.
`--labels-file` writes every anomaly as a JSON line: station, kind, start, end (epoch seconds),
metric.

## Usage

```bash
pip install '.[kinesis]'          # or run in place: python3 -m loggen ...

# a minute of logs on stdout, as fast as possible, reproducible
loggen --seed 1 --duration 60 --speed 0

# live into Kinesis, after training the detector with one hour of clean history
loggen --sink kinesis --stream dev-plant-anomaly-logs --region eu-west-1 \
       --backfill 3600 --anomaly-rate 1 --lines-per-record 10 --labels-file labels.jsonl

# a labelled dataset for offline evaluation
loggen --sink file --file plant.log --seed 7 --backfill 1800 --duration 10800 --speed 0 \
       --labels-file labels.jsonl
python3 -m loggen.evaluate --labels labels.jsonl --alerts alerts.jsonl --hours 3
```

`loggen --help` lists every option. Notable ones: `--speed` (plant seconds per wall second, 0 =
as fast as possible), `--rate` (volume multiplier), `--start` (plant clock), `--garbage-ratio`,
`--chatter`, `--max-delay`, `--kinds`.

The Kinesis sink batches up to 500 records per `PutRecords` call. It uses the source (station)
as partition key, so one station's lines stay ordered, and retries only the records Kinesis
rejected, with exponential backoff. If records are still rejected after the last attempt it fails
loudly instead of dropping logs. SIGTERM flushes the batch in flight.

## Container

```bash
docker build -t zorblatt-loggen .
docker run --rm zorblatt-loggen --seed 1 --duration 10 --speed 0
```

Runs as a non-root user and works with a read-only root filesystem. `deploy/tofu` can run it on ECS
Fargate against the deployed input stream (`generator_enabled`).

## Tests

```bash
python3 -m unittest discover -s tests
```
