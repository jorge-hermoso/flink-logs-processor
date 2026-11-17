package cloud.hermoso.flink_logs_processor.source;

import cloud.hermoso.flink_logs_processor.model.RawLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.Collector;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.charset.StandardCharsets;

/**
 * Reads a Kinesis record as text and emits one {@link RawLog} per line. Log shippers commonly
 * batch several lines in one record, separated by newlines. Never fails: bytes that are not valid
 * UTF-8 are decoded with replacement characters and rejected downstream by the parser.
 */
public class RawLogDeserializationSchema implements KinesisDeserializationSchema<RawLog> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<RawLog> output) {
        long arrival = record.approximateArrivalTimestamp() == null ? 0L : record.approximateArrivalTimestamp().toEpochMilli();
        String text = new String(record.data().asByteArray(), StandardCharsets.UTF_8);
        for (String line : text.split("\n")) {
            String trimmed = line.strip();
            if (!trimmed.isEmpty()) {
                output.collect(new RawLog(trimmed, arrival, shardId, record.sequenceNumber()));
            }
        }
    }

    @Override
    public TypeInformation<RawLog> getProducedType() {
        return TypeInformation.of(RawLog.class);
    }
}
