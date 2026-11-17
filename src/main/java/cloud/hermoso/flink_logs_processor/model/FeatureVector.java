package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Per-station, per-window numeric summary of the logs: the input of the anomaly model.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class FeatureVector {
    private String stationId;
    private long windowStart;
    private long windowEnd;
    private String[] featureNames;
    private double[] values;
}
