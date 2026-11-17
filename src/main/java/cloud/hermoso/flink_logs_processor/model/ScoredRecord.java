package cloud.hermoso.flink_logs_processor.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * A feature vector with the verdict of the anomaly model.
 * <p>Flink POJO (public no-arg constructor + getters/setters) so it is serialised by the POJO
 * serializer rather than Kryo.
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class ScoredRecord {
    private String stationId;
    private long windowStart;
    private long windowEnd;
    private String[] featureNames;
    private double[] values;
    /** Raw RCF score. 0 while the model is warming up. */
    private double score;
    /** Anomaly grade in [0, 1]; greater than 0 means the model considers the window anomalous. */
    private double grade;
    /** Adaptive score threshold the model applied. */
    private double threshold;
    /** Values the model expected, aligned with {@code values}; empty when not available. */
    private double[] expectedValues;
    /** Per-feature share of the anomaly score, aligned with {@code values}; empty when not available. */
    private double[] attribution;
    /** False while the model has seen fewer than {@code output.after} windows. */
    private boolean modelReady;
}
