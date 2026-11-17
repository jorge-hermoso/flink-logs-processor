package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.sink.Sinks;
import cloud.hermoso.flink_logs_processor.source.RawLogDeserializationSchema;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Production-line log anomaly detector.
 * <p>
 * Reads unstructured log lines from a Kinesis stream, scores each station's behaviour with a
 * Random Cut Forest, and writes scores to DynamoDB, alerts to a Kinesis stream and unprocessable
 * records to a dead-letter Kinesis stream. See docs/ARCHITECTURE.md.
 */
public class DataStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    /**
     * Local resource with the application properties, in the format Amazon Managed Service for
     * Apache Flink provides them at runtime. Override with -Dapp.properties.resource=...
     */
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            String resource = System.getProperty("app.properties.resource", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            LOG.info("Loading application properties from '{}'", resource);
            URL url = DataStreamJob.class.getClassLoader().getResource(resource);
            if (url == null) {
                throw new IOException("Application properties resource '" + resource + "' not found on the classpath");
            }
            return KinesisAnalyticsRuntime.getApplicationProperties(url.getPath());
        }
        LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }

    private static KinesisStreamsSource<RawLog> buildSource(Properties properties) {
        String streamArn = ApplicationConfigUtils.parseMandatoryString(properties, "stream.arn");
        String initialPosition = ApplicationConfigUtils.parseString(properties, "source.init.position", "LATEST");
        LOG.info("Reading logs from '{}', initial position {}", streamArn, initialPosition);

        // aws.* properties (region, endpoint, credentials provider...) are passed to the client
        Configuration sourceConfig = new Configuration();
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith("aws.")) {
                sourceConfig.setString(name, properties.getProperty(name));
            }
        }
        // Only applies when the job starts without state; otherwise it resumes from the checkpoint
        sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                KinesisSourceConfigOptions.InitialPosition.valueOf(initialPosition.toUpperCase(Locale.ROOT)));

        return KinesisStreamsSource.<RawLog>builder()
                .setStreamArn(streamArn)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new RawLogDeserializationSchema())
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationProperties = loadApplicationProperties(env);

        if (isLocal(env)) {
            // On AWS, checkpointing and parallelism are owned by the Managed Flink application configuration
            env.enableCheckpointing(30_000);
            env.setParallelism(1);
        }

        // Fail fast, before any resource is touched, when the configuration is wrong
        LogAnomalyPipeline.Config config = LogAnomalyPipeline.Config.fromApplicationProperties(applicationProperties);
        Properties sourceProperties = ApplicationConfigUtils.mandatoryGroup(applicationProperties, "Source");
        Properties alertProperties = ApplicationConfigUtils.mandatoryGroup(applicationProperties, "AlertStream");
        Properties deadLetterProperties = ApplicationConfigUtils.mandatoryGroup(applicationProperties, "DeadLetterStream");
        Properties scoresProperties = ApplicationConfigUtils.mandatoryGroup(applicationProperties, "ScoresTable");

        DataStream<RawLog> rawLogs = env
                // The source is generic: its output type cannot be inferred and must be given
                // Event time and watermarks are assigned in the source: per shard, not per reader
                .fromSource(buildSource(sourceProperties), LogAnomalyPipeline.watermarkStrategy(config), "Kinesis logs",
                        TypeInformation.of(RawLog.class))
                .uid("kinesis-source");

        LogAnomalyPipeline.Outputs outputs = LogAnomalyPipeline.build(rawLogs, config);

        outputs.scores
                .sinkTo(Sinks.scoresTable(scoresProperties))
                .uid("scores-sink").name("Scores to DynamoDB");
        outputs.alerts
                .sinkTo(Sinks.<Alert>kinesisJson(alertProperties, Alert::getStationId))
                .uid("alerts-sink").name("Alerts to Kinesis");
        outputs.deadLetters
                // Dead letters have no natural key: spread them over the shards
                .sinkTo(Sinks.<DeadLetter>kinesisJson(deadLetterProperties, letter -> UUID.randomUUID().toString()))
                .uid("dead-letter-sink").name("Dead letters to Kinesis");

        env.execute("Production logs anomaly detector");
    }
}
