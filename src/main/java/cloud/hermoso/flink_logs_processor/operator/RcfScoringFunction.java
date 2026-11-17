package cloud.hermoso.flink_logs_processor.operator;

import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import cloud.hermoso.flink_logs_processor.monitor.Metrics;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Scores feature vectors with one Thresholded Random Cut Forest per key (station), and trains the
 * model with every vector it scores.
 * <p>
 * <b>State.</b> The model of the current key is written to Flink keyed state after every update,
 * so a checkpoint always contains exactly the model that corresponds to the consumed input: after
 * a failure the job resumes with no lost and no double-counted training data. Models change once
 * per key per feature window, which makes this affordable. Deserialised models are additionally
 * kept in a bounded in-memory LRU cache; since the state is always current, evicting from the
 * cache is always safe.
 * <p>
 * <b>Failure handling.</b> Nothing in the data can fail the job: vectors that cannot be scored go
 * to the dead-letter side output, and a model that cannot be restored (different feature layout,
 * incompatible library version, corrupted bytes) or that throws is discarded and relearned.
 */
public class RcfScoringFunction extends KeyedProcessFunction<String, FeatureVector, ScoredRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RcfScoringFunction.class);

    private static final double[] EMPTY = new double[0];

    private final RcfModelsConfig modelsConfig;
    private final int maxCachedModels;
    private final long stateTtlMillis;
    private final OutputTag<DeadLetter> deadLetterTag;

    private transient ValueState<byte[]> modelState;
    private transient Map<String, ThresholdedRandomCutForest> cache;

    private transient Counter scoredWindows;
    private transient Counter anomalies;
    private transient Counter modelResets;
    private transient Counter rejectedVectors;

    /**
     * @param maxCachedModels maximum number of deserialised models held in memory per parallel task
     * @param stateTtlMillis  models of stations silent for longer than this are dropped; 0 keeps them for ever
     */
    public RcfScoringFunction(RcfModelsConfig modelsConfig, int maxCachedModels, long stateTtlMillis, OutputTag<DeadLetter> deadLetterTag) {
        Preconditions.checkArgument(maxCachedModels >= 1, "The model cache must hold at least one model");
        Preconditions.checkArgument(stateTtlMillis >= 0, "The model state TTL cannot be negative");
        this.modelsConfig = Preconditions.checkNotNull(modelsConfig);
        this.maxCachedModels = maxCachedModels;
        this.stateTtlMillis = stateTtlMillis;
        this.deadLetterTag = deadLetterTag;
    }

    @Override
    public void open(OpenContext openContext) {
        // Changing this name orphans every trained model: bump it only together with a migration
        ValueStateDescriptor<byte[]> descriptor = new ValueStateDescriptor<>("trcf-model", byte[].class);
        if (stateTtlMillis > 0) {
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMillis(stateTtlMillis))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
        }
        modelState = getRuntimeContext().getState(descriptor);

        cache = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ThresholdedRandomCutForest> eldest) {
                return size() > maxCachedModels;
            }
        };

        scoredWindows = Metrics.counter(getRuntimeContext(), "scoredWindows");
        anomalies = Metrics.counter(getRuntimeContext(), "anomalies");
        modelResets = Metrics.counter(getRuntimeContext(), "modelResets");
        rejectedVectors = Metrics.counter(getRuntimeContext(), "rejectedFeatureVectors");
    }

    @Override
    public void processElement(FeatureVector vector, Context ctx, Collector<ScoredRecord> out) throws Exception {
        String key = ctx.getCurrentKey();
        double[] values = vector.getValues();

        if (values == null || values.length == 0 || !Arrays.stream(values).allMatch(Double::isFinite)) {
            rejectedVectors.inc();
            ctx.output(deadLetterTag, new DeadLetter(
                    "INVALID_FEATURE_VECTOR", "non-finite or empty values", vector.toString(), null, null, vector.getWindowEnd()));
            return;
        }

        ThresholdedRandomCutForest model = modelFor(key, values.length);

        AnomalyDescriptor result;
        try {
            // process() scores the point against what the model has seen so far, then learns it
            result = model.process(values, vector.getWindowEnd());
        } catch (RuntimeException e) {
            // A model that throws is of no use any more. Start over rather than fail the job for ever.
            LOG.error("Model for key '{}' failed on {}; resetting it", key, vector, e);
            modelResets.inc();
            cache.remove(key);
            modelState.clear();
            ctx.output(deadLetterTag, new DeadLetter(
                    "MODEL_ERROR", e.toString(), vector.toString(), null, null, vector.getWindowEnd()));
            return;
        }

        modelState.update(ModelCodec.serialize(model, values.length));

        boolean ready = model.getForest().isOutputReady();
        scoredWindows.inc();
        if (result.getAnomalyGrade() > 0) {
            anomalies.inc();
        }

        out.collect(new ScoredRecord(
                vector.getStationId(), vector.getWindowStart(), vector.getWindowEnd(),
                vector.getFeatureNames(), values,
                result.getRCFScore(), result.getAnomalyGrade(), result.getThreshold(),
                expectedValues(result, values.length),
                attribution(result, values.length),
                ready));
    }

    /** Model of the key: from the cache, else from state, else new. */
    private ThresholdedRandomCutForest modelFor(String key, int inputDimensions) throws Exception {
        ThresholdedRandomCutForest model = cache.get(key);
        // Compare with the model's own shingle size: a restored model keeps the parameters it was
        // built with, even when the configuration has changed since
        if (model != null && inputDimensionsOf(model) == inputDimensions) {
            return model;
        }
        RcfModelParams params = modelsConfig.getModelParams(key);

        model = null;
        byte[] stored = modelState.value();
        if (stored != null) {
            try {
                model = ModelCodec.deserialize(stored, inputDimensions);
                LOG.info("Restored model for key '{}' from state ({} bytes, {} updates)",
                        key, stored.length, model.getForest().getTotalUpdates());
            } catch (ModelCodec.IncompatibleStateException e) {
                LOG.warn("Discarding stored model for key '{}', it will be relearned: {}", key, e.getMessage());
                modelResets.inc();
            }
        }
        if (model == null) {
            LOG.info("Initialising model for key '{}' with {} input dimensions and {}", key, inputDimensions, params);
            // String.hashCode is specified by the JLS: the seed is stable across JVMs and versions
            model = ModelCodec.newModel(params, inputDimensions, key.hashCode());
        }
        cache.put(key, model);
        return model;
    }

    private static int inputDimensionsOf(ThresholdedRandomCutForest model) {
        return model.getForest().getDimensions() / model.getForest().getShingleSize();
    }

    /** Expected values for the current point, when the model produced them. */
    private static double[] expectedValues(AnomalyDescriptor result, int length) {
        double[][] expected = result.getExpectedValuesList();
        if (result.getAnomalyGrade() > 0 && expected != null && expected.length > 0
                && expected[0] != null && expected[0].length == length) {
            return expected[0];
        }
        return EMPTY;
    }

    /** Per-feature attribution, normalised to shares of the total. */
    private static double[] attribution(AnomalyDescriptor result, int length) {
        double[] attribution = result.getRelevantAttribution();
        if (result.getAnomalyGrade() <= 0 || attribution == null || attribution.length != length) {
            return EMPTY;
        }
        double total = Arrays.stream(attribution).map(Math::abs).sum();
        if (total <= 0) {
            return EMPTY;
        }
        return Arrays.stream(attribution).map(a -> Math.abs(a) / total).toArray();
    }
}
