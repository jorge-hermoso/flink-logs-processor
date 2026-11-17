package cloud.hermoso.flink_logs_processor.rules;

import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StationRulesFunctionTest {

    private static final String[] NAMES = {"events", "warnings", "errors", "temp", "setpoint"};

    private static FeatureVector vector(String station, long windowEnd, double temp) {
        return new FeatureVector(station, windowEnd - 10_000, windowEnd, NAMES, new double[]{5, 0, 0, temp, 840});
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, FeatureVector, Alert> harness(long silenceMillis, int stuckWindows) throws Exception {
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new StationRulesFunction(silenceMillis, stuckWindows)), FeatureVector::getStationId, Types.STRING);
        harness.open();
        return harness;
    }

    private static List<String> summaries(KeyedOneInputStreamOperatorTestHarness<String, FeatureVector, Alert> harness) {
        return harness.extractOutputValues().stream().map(Alert::getSummary).collect(Collectors.toList());
    }

    @Test
    void silentStationAlertsOnceThenReArms() throws Exception {
        try (var harness = harness(60_000, 0)) {
            harness.processElement(vector("KILN-01", 10_000, 840), 10_000);
            harness.processElement(vector("PRESS-01", 10_000, 300), 10_000);
            harness.processWatermark(60_000);
            assertTrue(harness.extractOutputValues().isEmpty());

            harness.processElement(vector("PRESS-01", 70_000, 300), 70_000);
            harness.processWatermark(70_000);   // KILN-01 silent since its window ending at 10 s
            harness.processWatermark(200_000);  // no second alert for the same episode
            List<Alert> alerts = harness.extractOutputValues();
            assertEquals(2, alerts.size(), "KILN-01 at 70 s, PRESS-01 at 130 s");
            assertEquals("KILN-01#SILENCE#70000", alerts.get(0).getAlertId());
            assertEquals("SILENCE", alerts.get(0).getType());

            harness.processElement(vector("KILN-01", 210_000, 840), 210_000);  // back: re-armed
            harness.processWatermark(270_000);
            assertEquals(3, harness.extractOutputValues().size());
        }
    }

    @Test
    void silenceIsDetectedWhenTheWatermarkJumpsPastIt() throws Exception {
        // Catching up on a backlog: the windows after the silence arrive before the watermark
        try (var harness = harness(60_000, 0)) {
            harness.processElement(vector("KILN-01", 10_000, 840), 10_000);
            harness.processElement(vector("KILN-01", 200_000, 840), 200_000);
            harness.processElement(vector("KILN-01", 210_000, 840), 210_000);
            harness.processWatermark(210_000);
            List<Alert> alerts = harness.extractOutputValues();
            assertEquals(1, alerts.size());
            assertEquals("KILN-01#SILENCE#70000", alerts.get(0).getAlertId(), "same id as the timer path");
        }
    }

    @Test
    void timerAndGapDoNotBothAlert() throws Exception {
        try (var harness = harness(60_000, 0)) {
            harness.processElement(vector("KILN-01", 10_000, 840), 10_000);
            harness.processWatermark(100_000);                                 // timer fires
            harness.processElement(vector("KILN-01", 200_000, 840), 200_000);  // station back: no second alert
            assertEquals(1, harness.extractOutputValues().size());
        }
    }

    @Test
    void endOfInputIsNotSilence() throws Exception {
        try (var harness = harness(60_000, 0)) {
            harness.processElement(vector("KILN-01", 10_000, 840), 10_000);
            harness.processWatermark(Long.MAX_VALUE);
            assertTrue(harness.extractOutputValues().isEmpty());
        }
    }

    @Test
    void stuckMetricAlertsOncePerEpisode() throws Exception {
        try (var harness = harness(0, 3)) {
            long t = 0;
            harness.processElement(vector("KILN-01", t += 10_000, 840.1), 0);
            harness.processElement(vector("KILN-01", t += 10_000, 840.7), 0);
            for (int i = 0; i < 6; i++) {
                harness.processElement(vector("KILN-01", t += 10_000, 841.3), 0);
            }
            assertEquals(List.of("KILN-01 temp stuck at 841.30 for 4 windows"), summaries(harness));
            assertEquals("STUCK_SENSOR", harness.extractOutputValues().get(0).getType());

            harness.processElement(vector("KILN-01", t += 10_000, 839.0), 0);   // moves again: re-armed
            for (int i = 0; i < 4; i++) {   // the first of these is a change, then 3 repeats
                harness.processElement(vector("KILN-01", t += 10_000, 839.5), 0);
            }
            assertEquals(2, summaries(harness).size());
        }
    }

    @Test
    void metricsThatNeverVaryAreNotStuck() throws Exception {
        // "setpoint" is 840 in every window: it is a constant, not a frozen sensor
        try (var harness = harness(0, 3)) {
            for (int i = 1; i <= 10; i++) {
                harness.processElement(vector("KILN-01", i * 10_000L, 840 + i), 0);
            }
            assertTrue(harness.extractOutputValues().isEmpty());
        }
    }
}
