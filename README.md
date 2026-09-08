# Production Logs Anomaly Detector

This application is a portfolio project designed to be used later by the production-line-ml infrastructure repository. It is a clone and enhancement of the [AWS Flink Keyed Random Cut Forest Example](https://github.com/aws-samples/flink-keyed-random-cut-forest-example).

## Project Overview

This Flink-based application implements an anomaly detection system for production line monitoring. It consumes log entries from a Kinesis stream, applies the Random Cut Forest (RCF) algorithm to detect anomalies, and triggers appropriate responses:

- **Anomaly Detection**: Uses Random Cut Forest algorithm to identify outliers in multi-dimensional log data
- **Alerting**: Sends notifications via Amazon SNS when anomalies are detected
- **Metrics**: Creates real-time metrics stored in a DynamoDB sink for dashboarding and historical analysis
- **Scalability**: Built on Apache Flink for fault-tolerant, distributed stream processing

## Package Structure

The application is organized under the `cloud.hermoso.flink_logs_processor` package:

```
cloud.hermoso.flink_logs_processor
├── model                 # Data models (InputData, OutputData)
├── operator              # RCF operator implementation
├── datagen               # Data generation functions (for testing)
├── monitor               # Monitoring and metrics functions
└── DataStreamJob.java    # Main application entry point
```

## Maven Commands

### Compile the Application
```bash
mvn clean compile
```

### Run Unit Tests
```bash
mvn test
```

### Package the Application
```bash
mvn clean package
```
This creates the deployment JAR (`target/keyed-rcf-example_2.3.0-1.0.jar`) via the Maven Shade
plugin. It bundles the application and its dependencies, but *not* the Flink runtime — those
dependencies are `provided` scope, because the Flink cluster supplies them.

### Skip Tests During Build
```bash
mvn clean package -DskipTests
```

### Run the Application Locally

```bash
mvn compile exec:exec
```

No local Flink cluster or Flink installation is needed: the job detects a `LocalStreamEnvironment`
(see `DataStreamJob.isLocal()`), starts an embedded mini-cluster, sets parallelism to 1 and
checkpointing to 30s, and reads its configuration from
`src/main/resources/flink-application-properties-dev.json` instead of the AWS runtime properties.

The exec plugin is preconfigured in `pom.xml`; two details make this work with `provided` scope:

- **`<classpathScope>compile</classpathScope>`** — Maven's *compile* classpath includes
  `provided` dependencies, so the Flink jars are on the classpath for the local run while still
  being excluded from the packaged JAR. No scope changes and no separate profile are required.
  (Note that `runtime`-scope dependencies are *not* on this classpath.)
- **`exec:exec`, not `exec:java`** — `exec:exec` forks a real JVM. `exec:java` runs in the Maven
  JVM, and Flink's JobMaster cannot load user classes through the classloader it sets up; the job
  fails at submission with `ClassNotFoundException: org.apache.flink.api.common.ExecutionConfig`.

#### AWS credentials

The Kinesis sink needs credentials even when the job runs locally, otherwise the sink task fails
on startup with `SdkClientException: Unable to load credentials from any of the providers in the
chain` and the job enters a restart loop. Export a profile before running:

```bash
export AWS_PROFILE=<profile-with-static-keys>
# or, for an SSO profile, materialise the session as environment variables:
eval "$(aws configure export-credentials --profile <sso-profile> --format env)"
```

The output stream configured in `flink-application-properties-dev.json` (`OutputStream0`) must
exist in the configured region, and the credentials need write access to it.

#### Alternative: plain `java`

Useful when you want to pass JVM flags or attach a debugger:

```bash
mvn compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt -Dmdep.includeScope=compile
java -cp "target/classes:$(cat target/cp.txt)" cloud.hermoso.flink_logs_processor.DataStreamJob
```

> Running the shaded JAR directly (`java -jar target/keyed-rcf-example_2.3.0-1.0.jar`) does **not**
> work — it deliberately contains no Flink runtime. Deploy that JAR to a Flink cluster instead.

## Configuration

The application expects application properties to be provided either:
1. From Amazon Managed Service for Apache Flink runtime (when deployed to AWS)
2. From a local JSON file (`flink-application-properties-dev.json`) for local development

See the `src/main/resources/flink-application-properties-dev.json` file for an example configuration.

## Flink Version

This project uses Apache Flink 2.3.0 with Java 21.

Two dependency details follow from the Flink 2.x upgrade:

- **Kinesis connector**: `flink-connector-aws-kinesis-streams` 6.0.1-2.0. The older bundled
  `flink-connector-kinesis` artifact stops at Flink 1.20 and fails at runtime under Flink 2.x with
  `NoClassDefFoundError: org/apache/flink/api/connector/sink2/StatefulSink`. The sink class name and
  package are unchanged, so no application code is affected. Unlike the old artifact this one does
  not shade the AWS SDK, which is why the packaged JAR is noticeably larger.
- **`commons-lang` 2.6** is declared explicitly at compile scope. `aws-kinesisanalytics-runtime`
  needs it when parsing the local properties file but never declares it; Flink 1.x used to pull it
  in transitively and Flink 2.x does not, so without it local runs fail with
  `NoClassDefFoundError: org/apache/commons/lang/StringUtils`.

## Notes

- The application is designed to run on Amazon Managed Service for Apache Flink (formerly Kinesis Data Analytics)
- All Flink dependencies are marked as `provided` scope since they are supplied by the runtime environment
- The Maven Shade plugin creates a JAR containing the application and its dependencies, excluding those provided by the Flink runtime