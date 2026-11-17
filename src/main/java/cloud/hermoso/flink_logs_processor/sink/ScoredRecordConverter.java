package cloud.hermoso.flink_logs_processor.sink;

import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps a scored window to a DynamoDB item keyed by ({@code station_id}, {@code window_end}).
 * The key is derived from the data only, so a window written again after a restart overwrites
 * itself: the table is effectively exactly-once on top of an at-least-once sink.
 */
public class ScoredRecordConverter implements ElementConverter<ScoredRecord, DynamoDbWriteRequest> {
    private static final long serialVersionUID = 1L;

    public static final String PARTITION_KEY = "station_id";
    public static final String SORT_KEY = "window_end";
    public static final String TTL_ATTRIBUTE = "expires_at";

    private final long ttlSeconds;

    /** @param ttlSeconds items expire this long after their window; 0 keeps them for ever */
    public ScoredRecordConverter(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public DynamoDbWriteRequest apply(ScoredRecord record, SinkWriter.Context context) {
        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(toItem(record))
                .build();
    }

    Map<String, AttributeValue> toItem(ScoredRecord record) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PARTITION_KEY, AttributeValue.builder().s(record.getStationId()).build());
        item.put(SORT_KEY, number(record.getWindowEnd()));
        item.put("window_start", number(record.getWindowStart()));
        item.put("score", number(record.getScore()));
        item.put("grade", number(record.getGrade()));
        item.put("threshold", number(record.getThreshold()));
        item.put("anomaly", AttributeValue.builder().bool(record.getGrade() > 0).build());
        item.put("model_ready", AttributeValue.builder().bool(record.isModelReady()).build());
        item.put("features", namedValues(record.getFeatureNames(), record.getValues()));
        if (record.getExpectedValues().length == record.getValues().length) {
            item.put("expected", namedValues(record.getFeatureNames(), record.getExpectedValues()));
        }
        if (ttlSeconds > 0) {
            item.put(TTL_ATTRIBUTE, number(record.getWindowEnd() / 1000 + ttlSeconds));
        }
        return item;
    }

    private static AttributeValue namedValues(String[] names, double[] values) {
        Map<String, AttributeValue> map = new HashMap<>();
        for (int i = 0; i < names.length && i < values.length; i++) {
            map.put(names[i], number(values[i]));
        }
        return AttributeValue.builder().m(map).build();
    }

    private static AttributeValue number(double value) {
        // DynamoDB numbers cannot be NaN or infinite
        return AttributeValue.builder().n(Double.isFinite(value) ? Double.toString(value) : "0").build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
