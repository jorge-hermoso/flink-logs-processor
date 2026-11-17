package cloud.hermoso.flink_logs_processor.sink;

import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScoredRecordConverterTest {

    private static final ScoredRecord RECORD = new ScoredRecord("KILN-01", 10_000, 20_000,
            new String[]{"events", "temp"}, new double[]{5, 840.5}, Double.NaN, 0.8, 1.2, new double[0], new double[0], true);

    @Test
    void itemIsKeyedByStationAndWindowAndExpires() {
        Map<String, AttributeValue> item = new ScoredRecordConverter(3600).toItem(RECORD);
        assertEquals("KILN-01", item.get("station_id").s());
        assertEquals("20000", item.get("window_end").n());
        assertEquals("3620", item.get("expires_at").n());
        assertEquals("840.5", item.get("features").m().get("temp").n());
        assertTrue(item.get("anomaly").bool());
        assertEquals("0", item.get("score").n(), "NaN cannot be stored in DynamoDB");
        assertFalse(item.containsKey("expected"));
    }

    @Test
    void noTtlAttributeWhenExpiryIsDisabled() {
        assertFalse(new ScoredRecordConverter(0).toItem(RECORD).containsKey("expires_at"));
    }
}
