package cloud.hermoso.flink_logs_processor.operator;

import cloud.hermoso.flink_logs_processor.ApplicationConfigUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Properties;

/**
 * Hyper-parameters of one Random Cut Forest model. The number of dimensions is not configured: it
 * is the length of the feature vector, which follows from the station profile.
 */
@Getter
@AllArgsConstructor
@Builder(toBuilder = true)
@ToString
public class RcfModelParams implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Number of consecutive windows the model looks at together. Larger values catch slower patterns. */
    private final int shingleSize;
    private final int numberOfTrees;
    /** Points each tree remembers: with the time decay, the length of the model's memory. */
    private final int sampleSize;
    /** Windows to observe before the model starts scoring. */
    private final int outputAfter;
    /** Expected share of anomalous windows. Drives the adaptive threshold. */
    private final double anomalyRate;
    /** How quickly old data is forgotten. 0 uses the library default of 1 / (10 * sampleSize). */
    private final double timeDecay;

    public static final class PropertiesNames {
        public static final String SHINGLE_SIZE = "shingle.size";
        public static final String NUMBER_OF_TREES = "number.of.trees";
        public static final String SAMPLE_SIZE = "sample.size";
        public static final String OUTPUT_AFTER = "output.after";
        public static final String ANOMALY_RATE = "anomaly.rate";
        public static final String TIME_DECAY = "time.decay";

        private PropertiesNames() {
        }
    }

    /**
     * Starting point for 10-second windows: about 40 minutes of memory, 12 minutes of warm-up.
     * Chosen with the offline evaluation in docs/DETECTION_QUALITY.md.
     */
    public static RcfModelParams defaults() {
        return new RcfModelParams(6, 50, 256, 64, 0.005, 0);
    }

    /** Parameters from properties; any property that is absent keeps the value of {@code base}. */
    public static RcfModelParams fromProperties(Properties properties, RcfModelParams base) {
        RcfModelParams params = new RcfModelParams(
                ApplicationConfigUtils.parseInt(properties, PropertiesNames.SHINGLE_SIZE, base.shingleSize),
                ApplicationConfigUtils.parseInt(properties, PropertiesNames.NUMBER_OF_TREES, base.numberOfTrees),
                ApplicationConfigUtils.parseInt(properties, PropertiesNames.SAMPLE_SIZE, base.sampleSize),
                ApplicationConfigUtils.parseInt(properties, PropertiesNames.OUTPUT_AFTER, base.outputAfter),
                ApplicationConfigUtils.parseDouble(properties, PropertiesNames.ANOMALY_RATE, base.anomalyRate),
                ApplicationConfigUtils.parseDouble(properties, PropertiesNames.TIME_DECAY, base.timeDecay));
        params.validate();
        return params;
    }

    private void validate() {
        Preconditions.checkArgument(shingleSize >= 1, "%s must be >= 1", PropertiesNames.SHINGLE_SIZE);
        Preconditions.checkArgument(numberOfTrees >= 1, "%s must be >= 1", PropertiesNames.NUMBER_OF_TREES);
        Preconditions.checkArgument(sampleSize >= 8, "%s must be >= 8", PropertiesNames.SAMPLE_SIZE);
        Preconditions.checkArgument(outputAfter >= 1 && outputAfter <= sampleSize,
                "%s must be between 1 and %s", PropertiesNames.OUTPUT_AFTER, PropertiesNames.SAMPLE_SIZE);
        Preconditions.checkArgument(anomalyRate > 0 && anomalyRate < 1, "%s must be in (0, 1)", PropertiesNames.ANOMALY_RATE);
        Preconditions.checkArgument(timeDecay >= 0, "%s must be >= 0", PropertiesNames.TIME_DECAY);
    }
}
