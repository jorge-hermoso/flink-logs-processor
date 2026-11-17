# Local development

## Build and test

```bash
mvn clean package                         # compile, run the tests, build the shaded JAR
cd log-generator && python3 -m unittest discover -s tests && cd ..
cd deploy/tofu && tofu init -backend=false && tofu test && cd ../..
```

Requires JDK 17, Maven 3.9, Python ≥ 3.9 and OpenTofu ≥ 1.8. Without a local JDK, Maven can run
in Docker:

```bash
docker run --rm -v "$PWD":/w -v ~/.m2:/root/.m2 -w /w maven:3.9-eclipse-temurin-21 mvn package
```

What the Java tests cover:

| Test | Covers |
|---|---|
| `LogParserTest` | Every timestamp / layout dialect, metric matching rules, untrusted clocks, garbage. |
| `ConfigTest` | Property parsing, defaults, override resolution, validation messages. |
| `RcfScoringFunctionTest` | Warm-up, outlier detection, **snapshot/restore of models**, cache eviction, layout change, poison input. |
| `AlertFunctionTest` | Minimum grade, warm-up grace, cool-down, escalation through the cool-down, summary text. |
| `ScoredRecordConverterTest` | DynamoDB item shape, TTL, NaN handling. |
| `LogAnomalyPipelineTest` | The production topology on an embedded Flink cluster, raw text in → alerts out. |
| `GeneratedLogsContractTest` | Real `log-generator` output is fully understood with the shipped dev configuration. |

## Running the job locally against LocalStack

`local/docker-compose.yml` starts LocalStack with the streams and table that
`flink-application-properties-dev.json` points at.

```bash
docker compose -f local/docker-compose.yml up -d --wait
mvn compile exec:exec                     # runs the job on an embedded Flink cluster
```

In another terminal, feed it:

```bash
cd log-generator
pip install '.[kinesis]'
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  loggen --sink kinesis --stream plant-logs --region eu-west-1 --endpoint-url http://localhost:4566 \
         --backfill 1800 --speed 10 --lines-per-record 5
```

and watch the outputs:

```bash
docker exec local-localstack-1 awslocal dynamodb scan --table-name plant-scores --select COUNT
```

The Flink web UI runs on a random port printed in the job log (`Rest endpoint listening at`).

### Things that differ from AWS

* **LocalStack's Kinesis reports arrival timestamps about 90 minutes in the past.** The job uses
  arrival time to detect controllers with clocks running fast (`max.future.skew.ms`), so the
  dev configuration relaxes that check. Keep the default on AWS.
* The dev properties use static `test` credentials, `HTTP1_1` and `aws.trust.all.certificates`
  for the LocalStack endpoint. None of that belongs in a real deployment.
* Checkpoints are kept in memory: stopping the local job loses its state.

## End-to-end check

```bash
local/e2e.sh
```

Starts a fresh LocalStack, writes 40 minutes of synthetic logs with two known incidents (a
temperature spike, an error burst) as one backlog, runs the job and checks that both incidents
are alerted, false alerts are rare, **no event is late** and every window is scored. Writing the
whole history at once reproduces a job catching up after an outage. This is the regression
test for per-shard watermarks (see TASKS.md, T-07).

The script uses `mvn` from the `PATH`; set `MVN` to use a wrapper (for example one running
Maven in Docker with `--network host`).

## Regenerating test fixtures

`src/test/resources/generated-plant.log` is real generator output, used by the contract test:

```bash
cd log-generator && python3 -m loggen --seed 11 --start 2026-11-17T08:00:00Z --duration 60 --speed 0 \
  --anomaly-rate 0 --garbage-ratio 0.01 --inject KILN-01:error_burst:20:10 > ../src/test/resources/generated-plant.log
```
