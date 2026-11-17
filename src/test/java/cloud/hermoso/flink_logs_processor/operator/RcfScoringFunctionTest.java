package cloud.hermoso.flink_logs_processor.operator;

import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RcfScoringFunctionTest {

    private static final OutputTag<DeadLetter> DEAD_LETTERS = new OutputTag<>("dead-letters") {
    };
    private static final String[] NAMES = {"events", "warnings", "errors", "temp"};
    private static final int WARM_UP = 40;

    private final Random random = new Random(42);
    private long window = 0;

    private static KeyedOneInputStreamOperatorTestHarness<String, FeatureVector, ScoredRecord> harness(int maxCachedModels) throws Exception {
        RcfModelsConfig config = new RcfModelsConfig(
                RcfModelParams.defaults().toBuilder().shingleSize(2).numberOfTrees(30).sampleSize(128).outputAfter(WARM_UP).build());
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RcfScoringFunction(config, maxCachedModels, 0, DEAD_LETTERS)),
                FeatureVector::getStationId, Types.STRING);
    }

    private FeatureVector normal(String station) {
        return vector(station, 10 + random.nextInt(3), 0, 0, 840 + random.nextGaussian());
    }

    private FeatureVector vector(String station, double... values) {
        long start = window++ * 10_000;
        return new FeatureVector(station, start, start + 10_000, NAMES.clone(), values);
    }

    private static List<ScoredRecord> output(KeyedOneInputStreamOperatorTestHarness<String, FeatureVector, ScoredRecord> harness) {
        return harness.extractOutputValues();
    }

    @Test
    void warmsUpThenFlagsAnOutlier() throws Exception {
        try (var harness = harness(10)) {
            harness.open();
            for (int i = 0; i < 300; i++) {
                harness.processElement(normal("KILN-01"), i);
            }
            List<ScoredRecord> normal = output(harness);
            assertFalse(normal.get(0).isModelReady());
            assertEquals(0.0, normal.get(0).getGrade());
            assertTrue(normal.get(299).isModelReady());
            long falsePositives = normal.stream().filter(r -> r.getGrade() > 0).count();
            assertTrue(falsePositives <= 6, "too many anomalies in normal data: " + falsePositives);

            harness.processElement(vector("KILN-01", 11, 0, 9, 1100), 300);
            ScoredRecord outlier = output(harness).get(300);
            assertTrue(outlier.getGrade() > 0, "outlier not flagged: " + outlier);
            assertEquals(NAMES.length, outlier.getAttribution().length);
        }
    }

    @Test
    void modelSurvivesSnapshotAndRestore() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(10)) {
            harness.open();
            for (int i = 0; i < 200; i++) {
                harness.processElement(normal("KILN-01"), i);
            }
            snapshot = harness.snapshot(1, 1);
        }

        try (var restored = harness(10)) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(normal("KILN-01"), 200);
            // A model that started from scratch would still be warming up
            assertTrue(output(restored).get(0).isModelReady());

            restored.processElement(vector("KILN-01", 11, 0, 9, 1100), 201);
            assertTrue(output(restored).get(1).getGrade() > 0);
        }
    }

    @Test
    void eachKeyHasItsOwnModelEvenWhenTheCacheHoldsOnlyOne() throws Exception {
        try (var harness = harness(1)) {
            harness.open();
            // KILN-01 is trained, PRESS-01 is not: alternating keys forces cache eviction and reload from state
            for (int i = 0; i < 100; i++) {
                harness.processElement(normal("KILN-01"), i);
            }
            harness.processElement(normal("PRESS-01"), 100);
            harness.processElement(normal("KILN-01"), 101);

            List<ScoredRecord> records = output(harness);
            assertFalse(records.get(100).isModelReady());
            assertTrue(records.get(101).isModelReady());
        }
    }

    @Test
    void changedFeatureLayoutResetsTheModelInsteadOfFailing() throws Exception {
        OperatorSubtaskState snapshot;
        try (var harness = harness(10)) {
            harness.open();
            for (int i = 0; i < 100; i++) {
                harness.processElement(normal("KILN-01"), i);
            }
            snapshot = harness.snapshot(1, 1);
        }

        try (var restored = harness(10)) {
            restored.initializeState(snapshot);
            restored.open();
            // A metric was added to the station profile: one more dimension
            restored.processElement(new FeatureVector("KILN-01", 0, 10_000,
                    new String[]{"events", "warnings", "errors", "temp", "o2"}, new double[]{10, 0, 0, 840, 3.1}), 100);
            assertFalse(output(restored).get(0).isModelReady());
        }
    }

    @Test
    void nonFiniteVectorsGoToTheDeadLetterOutput() throws Exception {
        try (var harness = harness(10)) {
            harness.open();
            harness.processElement(vector("KILN-01", 10, 0, 0, Double.NaN), 0);
            harness.processElement(vector("KILN-01", 10, 0, 0, Double.POSITIVE_INFINITY), 1);

            assertTrue(output(harness).isEmpty());
            assertNotNull(harness.getSideOutput(DEAD_LETTERS));
            List<String> reasons = harness.getSideOutput(DEAD_LETTERS).stream()
                    .map(record -> record.getValue().getReason()).collect(Collectors.toList());
            assertEquals(List.of("INVALID_FEATURE_VECTOR", "INVALID_FEATURE_VECTOR"), reasons);
        }
    }

    @Test
    void unreadableStoredModelIsRejectedByTheCodec() {
        org.junit.jupiter.api.Assertions.assertThrows(ModelCodec.IncompatibleStateException.class,
                () -> ModelCodec.deserialize(new byte[]{0, 0, 0, 1, 0, 0, 0, 4, 1, 2, 3}, 4));
        org.junit.jupiter.api.Assertions.assertThrows(ModelCodec.IncompatibleStateException.class,
                () -> ModelCodec.deserialize(new byte[]{0, 0, 0, 99}, 4));
    }
}
