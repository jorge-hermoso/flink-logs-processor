package cloud.hermoso.flink_logs_processor.alert;

import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlertFunctionTest {

    private static final String[] NAMES = {"events", "warnings", "errors", "temp"};

    private static ScoredRecord record(String station, long windowEnd, double grade) {
        return record(station, windowEnd, grade, true);
    }

    private static ScoredRecord record(String station, long windowEnd, double grade, boolean modelReady) {
        return new ScoredRecord(station, windowEnd - 10_000, windowEnd, NAMES, new double[]{10, 0, 9, 1100},
                2.5, grade, 1.4, new double[]{10, 0, 0, 840}, new double[]{0.05, 0, 0.35, 0.6}, modelReady);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, ScoredRecord, Alert> harness(double minGrade) throws Exception {
        return harness(minGrade, 0);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, ScoredRecord, Alert> harness(double minGrade, int warmupWindows) throws Exception {
        var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AlertFunction(minGrade, 60_000, warmupWindows)), ScoredRecord::getStationId, Types.STRING);
        harness.open();
        return harness;
    }

    @Test
    void alertsOncePerStationPerCooldown() throws Exception {
        try (var harness = harness(0)) {
            harness.processElement(record("KILN-01", 10_000, 0), 0);        // not an anomaly
            harness.processElement(record("KILN-01", 20_000, 0.9), 0);      // alert
            harness.processElement(record("KILN-01", 30_000, 0.9), 0);      // same incident: suppressed
            harness.processElement(record("PRESS-01", 30_000, 0.7), 0);     // another station: alert
            harness.processElement(record("KILN-01", 79_999, 0.5), 0);      // still cooling down
            harness.processElement(record("KILN-01", 80_000, 0.8), 0);      // cool-down over: alert

            List<String> ids = harness.extractOutputValues().stream().map(Alert::getAlertId).collect(Collectors.toList());
            assertEquals(List.of("KILN-01#20000", "PRESS-01#30000", "KILN-01#80000"), ids);
        }
    }

    @Test
    void aStrongerAnomalyBreaksThroughTheCooldown() throws Exception {
        // A marginal alert must not hide the real incident that follows it
        try (var harness = harness(0)) {
            harness.processElement(record("KILN-01", 10_000, 0.25), 0);
            harness.processElement(record("KILN-01", 40_000, 1.0), 0);
            harness.processElement(record("KILN-01", 50_000, 1.0), 0);
            List<String> ids = harness.extractOutputValues().stream().map(Alert::getAlertId).collect(Collectors.toList());
            assertEquals(List.of("KILN-01#10000", "KILN-01#40000"), ids);
        }
    }

    @Test
    void noAlertsWhileTheModelIsStillSettling() throws Exception {
        try (var harness = harness(0, 3)) {
            harness.processElement(record("KILN-01", 10_000, 0, false), 0);
            harness.processElement(record("KILN-01", 20_000, 1.0, true), 0);   // 1st ready window
            harness.processElement(record("KILN-01", 30_000, 1.0, true), 0);   // 2nd
            harness.processElement(record("KILN-01", 40_000, 0, true), 0);     // 3rd
            harness.processElement(record("KILN-01", 50_000, 1.0, true), 0);   // 4th: alert
            harness.processElement(record("KILN-01", 60_000, 0, false), 0);    // model reset
            harness.processElement(record("KILN-01", 200_000, 1.0, true), 0);  // settling again
            List<String> ids = harness.extractOutputValues().stream().map(Alert::getAlertId).collect(Collectors.toList());
            assertEquals(List.of("KILN-01#50000"), ids);
        }
    }

    @Test
    void gradesBelowTheMinimumDoNotAlert() throws Exception {
        try (var harness = harness(0.5)) {
            harness.processElement(record("KILN-01", 10_000, 0.3), 0);
            harness.processElement(record("KILN-01", 20_000, 0.5), 0);
            assertEquals(1, harness.extractOutputValues().size());
        }
    }

    @Test
    void summaryNamesTheMostSuspiciousFeaturesFirst() {
        assertEquals("KILN-01 anomaly (grade 0.90): temp=1100.00 (expected 840.00), errors=9.00 (expected 0.00), events=10.00 (expected 10.00)",
                AlertFunction.summary(record("KILN-01", 20_000, 0.9)));
    }
}
