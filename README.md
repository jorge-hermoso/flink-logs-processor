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
This creates a fat JAR in the `target` directory with all dependencies included.

### Run the Application Locally
 
You can run the application using either of these methods:
 
#### Method 1: Direct classpath (recommended for reliability)
```bash
# First copy dependencies if needed
mvn dependency:copy-dependencies
 
# Then run with the correct classpath
java -cp "target/classes:target/dependency/*" cloud.hermoso.flink_logs_processor.DataStreamJob
```
 
#### Method 2: Using Maven exec plugin
```bash
mvn exec:java
```
> **Note**: The exec plugin may have classpath issues with provided scope dependencies in some environments. If you encounter problems, use Method 1.
 
> **Important Notes**:
> - Since Flink dependencies are marked as `provided` scope (not included in the shaded JAR for AWS deployment), you need to ensure dependencies are available when running locally.
> - For AWS deployment, use the shaded JAR as Flink runtime provides the dependencies:
>   ```bash
>   java -cp target/keyed-rcf-example_2.3.0-1.0.jar cloud.hermoso.flink_logs_processor.DataStreamJob
>   ```

### Skip Tests During Build
```bash
mvn clean package -DskipTests
```

## Configuration

The application expects application properties to be provided either:
1. From Amazon Managed Service for Apache Flink runtime (when deployed to AWS)
2. From a local JSON file (`flink-application-properties-dev.json`) for local development

See the `src/main/resources/flink-application-properties-dev.json` file for an example configuration.

## Flink Version

This project uses Apache Flink 2.3.0 with Java 21.

## Notes

- The application is designed to run on Amazon Managed Service for Apache Flink (formerly Kinesis Data Analytics)
- All Flink dependencies are marked as `provided` scope since they are supplied by the runtime environment
- The Maven Shade plugin is configured to create an executable JAR with all necessary dependencies excluded those provided by Flink runtime