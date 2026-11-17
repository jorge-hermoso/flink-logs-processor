package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * A record the pipeline could not process, with the reason and enough provenance to find it again.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class DeadLetter {
    private String reason;
    private String detail;
    /** Offending input, truncated. */
    private String raw;
    private String shardId;
    private String sequenceNumber;
    private long arrivalTimestamp;
}
