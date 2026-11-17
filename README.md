# Production Logs Anomaly Detector

A streaming anomaly detector for production-line logs, built on Apache Flink 2.3 and run on
Amazon Managed Service for Apache Flink. It reads free-text logs from many station types,
learns what normal looks like for each station, and raises explained alerts when a station
misbehaves. It needs no labelled data and no per-format parsing code.

It started as a clone of the
[AWS Flink Keyed Random Cut Forest example](https://github.com/aws-samples/flink-keyed-random-cut-forest-example)
and is a portfolio project meant to be used by the production-line-ml infrastructure repository.

```
log lines ─► Kinesis ─► Flink: parse ─► 10 s windows per station ─► Random Cut Forest ─┬─► alerts ─► SNS
                               │                                     + rules            ├─► scores ─► DynamoDB
                               └──────────────── dead letters ◄──────────────────────────┘
```

## What it does

* **Reads any log layout.** Each station type only declares which numbers to read and the
  label in front of them (`zone temp reading`, `o2`, ...). Timestamps (epoch, ISO-8601, syslog),
  severities and station ids are found wherever they are in the line.
* **Learns each station separately** with a Thresholded Random Cut Forest. It needs no training
  set, adapts to slow process changes, and explains alerts:
  `KILN-01 anomaly (grade 1.00): warnings=5 (expected 0), zone_temp=829.46 (expected 839.85)`.
* **Catches what a model cannot:** rules for silent stations and frozen sensors.
* **Alerts sparingly:** minimum grade, warm-up grace, cool-down that still lets a worse
  anomaly through.
* **Keeps going:** bad input goes to a dead-letter stream instead of failing the job. Model
  state is exact and versioned, so trained models survive failures, deployments and upgrades.
  Watermarks are tracked per shard, and outputs are idempotent.

Detection on synthetic benchmarks: 89 % of 57 injected incidents found, about one false alert
per station every 3 hours ([details](docs/DETECTION_QUALITY.md)).

## Repository

| Path | What |
|---|---|
| `src/` | The Flink job (Java 17, Maven). |
| `log-generator/` | **Separate app**: synthetic logs from a fictional plant, with labelled anomalies, plus a detection evaluator ([README](log-generator/README.md)). |
| `deploy/tofu/` | OpenTofu deployment: streams, table, Flink application, IAM, alerting, alarms, dashboard, optional generator on Fargate. |
| `local/` | LocalStack setup and an end-to-end check. |
| `docs/` | Documentation, below. |

## Documentation

| Document | For |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | How it works and why; the resilience design. |
| [Configuration](docs/CONFIGURATION.md) | Every property: station profiles, model, alerting, rules. |
| [Deployment](docs/DEPLOYMENT.md) | Deploying, updating, rolling back with OpenTofu. |
| [Operations](docs/OPERATIONS.md) | Metrics, alarms and what to do about each. |
| [Local development](docs/LOCAL_DEVELOPMENT.md) | Building, testing, running against LocalStack. |
| [Detection quality](docs/DETECTION_QUALITY.md) | Benchmark results and how to measure after a change. |
| [Plan and tasks](docs/TASKS.md) | Review findings, completed work, prioritised backlog. |

## Quick start

```bash
mvn clean package                                    # JDK 17: build and test the job
cd log-generator && python3 -m unittest discover -s tests && cd ..

# run everything locally
docker compose -f local/docker-compose.yml up -d --wait
mvn compile exec:exec &                              # the job, on an embedded Flink cluster
cd log-generator && pip install '.[kinesis]' && AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  loggen --sink kinesis --stream plant-logs --region eu-west-1 --endpoint-url http://localhost:4566 \
         --backfill 1800 --speed 10

# or check the whole path end to end
local/e2e.sh

# deploy
cd deploy/tofu && tofu init && tofu apply -var-file=environments/dev.tfvars
```

## Build notes

* **Java 17.** Managed Service for Apache Flink does not support Java 21.
* `mvn package` builds `target/flink_logs_processor_2.3.0-1.0.jar` with the Maven Shade plugin.
  Flink itself is `provided` (the cluster supplies it). `mvn compile exec:exec` still runs the job
  locally, because the exec plugin uses the compile classpath, which includes `provided`. It forks a
  JVM (`exec:exec`, not `exec:java`): Flink's JobMaster cannot load user classes through the
  classloader `exec:java` sets up.
* **Kinesis connector** `flink-connector-aws-kinesis-streams` 6.0.1-2.0 and **DynamoDB
  connector** `flink-connector-dynamodb` 6.0.1-2.0. The older bundled `flink-connector-kinesis`
  stops at Flink 1.20 and fails under Flink 2.x.
* **`commons-lang` 2.6** is declared explicitly: `aws-kinesisanalytics-runtime` needs it when
  reading the local properties file but does not declare it, and Flink 2.x no longer brings it
  in transitively.
* Running the shaded JAR with `java -jar` does not work: it deliberately contains no Flink runtime.
