package cloud.hermoso.flink_logs_processor.sink;

import cloud.hermoso.flink_logs_processor.ApplicationConfigUtils;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.sink.PartitionKeyGenerator;
import org.apache.flink.formats.json.JsonSerializationSchema;

import java.util.List;
import java.util.Properties;

/** Builds the sinks from their property groups. Any {@code aws.*} property is passed to the AWS client. */
public final class Sinks {

    private Sinks() {
    }

    /**
     * JSON to a Kinesis stream named by {@code stream.name}. The sink retries failed records
     * indefinitely (at-least-once) and applies back-pressure rather than dropping data.
     */
    public static <T> KinesisStreamsSink<T> kinesisJson(Properties properties, PartitionKeyGenerator<T> partitionKey) {
        return KinesisStreamsSink.<T>builder()
                .setStreamName(ApplicationConfigUtils.parseMandatoryString(properties, "stream.name"))
                .setKinesisClientProperties(properties)
                .setSerializationSchema(new JsonSerializationSchema<>())
                .setPartitionKeyGenerator(partitionKey)
                .setFailOnError(false)
                .build();
    }

    /** Scored windows to the DynamoDB table named by {@code table.name}, expiring after {@code ttl.days}. */
    public static DynamoDbSink<ScoredRecord> scoresTable(Properties properties) {
        long ttlDays = ApplicationConfigUtils.parseLong(properties, "ttl.days", 30);
        return DynamoDbSink.<ScoredRecord>builder()
                .setTableName(ApplicationConfigUtils.parseMandatoryString(properties, "table.name"))
                .setDynamoDbProperties(properties)
                .setElementConverter(new ScoredRecordConverter(ttlDays * 86_400L))
                // Two writes of the same window in one batch would be rejected by BatchWriteItem
                .setOverwriteByPartitionKeys(List.of(ScoredRecordConverter.PARTITION_KEY, ScoredRecordConverter.SORT_KEY))
                .setFailOnError(false)
                .build();
    }
}
