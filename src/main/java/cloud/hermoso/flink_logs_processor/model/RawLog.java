package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * One raw, unparsed log line as read from the input stream, with its provenance.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class RawLog {
    private String line;
    /** Kinesis approximate arrival timestamp (epoch millis); 0 when unknown. */
    private long arrivalTimestamp;
    private String shardId;
    private String sequenceNumber;
}
