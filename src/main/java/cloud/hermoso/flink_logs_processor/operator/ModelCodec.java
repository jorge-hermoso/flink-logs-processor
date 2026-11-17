package cloud.hermoso.flink_logs_processor.operator;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Creates models and converts them to and from the bytes kept in Flink state.
 * <p>
 * The state is stored as {@code byte[]} with our own small header rather than as a Flink-serialized
 * object graph: Flink would fall back to Kryo for the library's state classes, and Kryo-serialized
 * state does not survive library or Flink upgrades. With an explicit format version, an
 * incompatible snapshot is detected and the model rebuilt instead of the job failing to restore.
 */
final class ModelCodec {

    /** Bump when the layout written by {@link #serialize} changes. */
    static final int FORMAT_VERSION = 1;

    private ModelCodec() {
    }

    /** Thrown when stored bytes cannot be turned back into a model for the current configuration. */
    static final class IncompatibleStateException extends Exception {
        private static final long serialVersionUID = 1L;

        IncompatibleStateException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * @param randomSeed seeds the forest, so that the same input always produces the same scores:
     *                   a replay after a restore then reaches the same decisions
     */
    static ThresholdedRandomCutForest newModel(RcfModelParams params, int inputDimensions, long randomSeed) {
        ThresholdedRandomCutForest.Builder<?> builder = ThresholdedRandomCutForest.builder()
                .randomSeed(randomSeed)
                .dimensions(inputDimensions * params.getShingleSize())
                .shingleSize(params.getShingleSize())
                .internalShinglingEnabled(true)
                .numberOfTrees(params.getNumberOfTrees())
                .sampleSize(params.getSampleSize())
                .outputAfter(params.getOutputAfter())
                .anomalyRate(params.getAnomalyRate())
                .precision(Precision.FLOAT_32)
                .forestMode(ForestMode.STANDARD)
                // Features have very different scales (event counts vs. temperatures): normalise them
                .transformMethod(TransformMethod.NORMALIZE)
                .autoAdjust(true)
                // The operator is single threaded per key; an RCF thread pool per model would not scale
                .parallelExecutionEnabled(false);
        if (params.getTimeDecay() > 0) {
            builder.timeDecay(params.getTimeDecay());
        }
        return builder.build();
    }

    static byte[] serialize(ThresholdedRandomCutForest model, int inputDimensions) throws IOException {
        ThresholdedRandomCutForestMapper mapper = new ThresholdedRandomCutForestMapper();
        ThresholdedRandomCutForestState state = mapper.toState(model);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64 * 1024);
        try (DataOutputStream header = new DataOutputStream(bytes)) {
            header.writeInt(FORMAT_VERSION);
            header.writeInt(inputDimensions);
            header.flush();
            try (ObjectOutputStream body = new ObjectOutputStream(header)) {
                body.writeObject(state);
            }
        }
        return bytes.toByteArray();
    }

    /**
     * @throws IncompatibleStateException if the bytes were written by another format version, for a
     *                                    different number of input dimensions, or cannot be read
     */
    static ThresholdedRandomCutForest deserialize(byte[] stored, int inputDimensions) throws IncompatibleStateException {
        try (DataInputStream header = new DataInputStream(new ByteArrayInputStream(stored))) {
            int version = header.readInt();
            if (version != FORMAT_VERSION) {
                throw new IncompatibleStateException("format version " + version + ", expected " + FORMAT_VERSION, null);
            }
            int storedDimensions = header.readInt();
            if (storedDimensions != inputDimensions) {
                throw new IncompatibleStateException(
                        "model has " + storedDimensions + " input dimensions, feature vector has " + inputDimensions, null);
            }
            try (ObjectInputStream body = new ObjectInputStream(header)) {
                ThresholdedRandomCutForestState state = (ThresholdedRandomCutForestState) body.readObject();
                return new ThresholdedRandomCutForestMapper().toModel(state);
            }
        } catch (IOException | ClassNotFoundException | RuntimeException e) {
            throw new IncompatibleStateException("unreadable model state: " + e, e);
        }
    }
}
