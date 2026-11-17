package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Anomaly notification published to the alert stream. Deterministic id, so consumers can de-duplicate replays.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Alert {
    /** Model anomaly: {@code <stationId>#<windowEnd>}; rule: {@code <stationId>#<type>#<windowEnd>}. Stable across replays. */
    private String alertId;
    /** {@code ANOMALY} (from the model), {@code SILENCE} or {@code STUCK_SENSOR} (from the rules). */
    private String type;
    private String stationId;
    /** ISO-8601 end of the anomalous window. */
    private String detectedAt;
    private long windowEnd;
    private double score;
    private double grade;
    private double threshold;
    /** Human readable one-liner, most suspicious features first. */
    private String summary;
    private String[] featureNames;
    private double[] values;
    private double[] expectedValues;
}
