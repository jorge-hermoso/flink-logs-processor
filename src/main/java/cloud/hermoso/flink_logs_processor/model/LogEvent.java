package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * A log line attributed to a station, with the numeric readings found in it.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class LogEvent {
    /** Event time, epoch millis. */
    private long timestamp;
    /** Canonical station id, e.g. {@code KILN-01}. */
    private String stationId;
    /** Station type, e.g. {@code KILN}. Selects the station profile. */
    private String stationType;
    /** One of the {@code Severity} constants. */
    private int severity;
    /** Readings aligned with the station profile metric order; NaN when absent from this line. */
    private double[] metrics;
}
