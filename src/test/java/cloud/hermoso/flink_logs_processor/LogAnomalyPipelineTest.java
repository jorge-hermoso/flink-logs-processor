package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import cloud.hermoso.flink_logs_processor.operator.RcfModelParams;
import cloud.hermoso.flink_logs_processor.operator.RcfModelsConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the whole topology on an embedded Flink cluster, from raw text to alerts. */
class LogAnomalyPipelineTest {

    private static final long START = Instant.parse("2026-11-17T08:00:00Z").toEpochMilli();
    private static final int NORMAL_WINDOWS = 300;
    private static final int INCIDENT_WINDOWS = 3;

    private static <T> List<T> drain(CloseableIterator<T> iterator) throws Exception {
        List<T> items = new ArrayList<>();
        try (iterator) {
            iterator.forEachRemaining(items::add);
        }
        return items;
    }

    private static <T> List<T> drainUnchecked(CloseableIterator<T> iterator) {
        try {
            return drain(iterator);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<RawLog> logs() {
        Random random = new Random(7);
        List<RawLog> logs = new ArrayList<>();
        for (int window = 0; window < NORMAL_WINDOWS + INCIDENT_WINDOWS; window++) {
            boolean incident = window >= NORMAL_WINDOWS;
            for (int i = 0; i < 5; i++) {
                long time = START + window * 10_000L + i * 2_000L;
                double temp = (incident ? 1150 : 840) + random.nextGaussian();
                double o2 = 3.1 + random.nextGaussian() * 0.05;
                String level = incident ? "<err>" : "<info>";
                String line = String.format(Locale.ROOT, "%s KILN-01 kilnd[412]: %s zone temp reading %.1fC (setpoint 840.0C) o2=%.2f%%",
                        Instant.ofEpochMilli(time), level, temp, o2);
                logs.add(new RawLog(line, time + 500, "shard-0", String.valueOf(logs.size())));
            }
            // Background chatter and garbage, interleaved with the real logs
            long time = START + window * 10_000L;
            logs.add(new RawLog(Instant.ofEpochMilli(time) + " INFO scada-gw heartbeat ok", time, "shard-0", "n"));
            if (window % 100 == 0) {
                logs.add(new RawLog("\u0000\u0001\u0002\u0003 KILN-01", time, "shard-0", "g"));
            }
        }
        return logs;
    }

    @Test
    void detectsAnIncidentInRawLogsAndDeadLettersWhatItCannotUse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        LogAnomalyPipeline.Config config = new LogAnomalyPipeline.Config(
                TestProfiles.profiles(),
                new RcfModelsConfig(RcfModelParams.defaults().toBuilder().shingleSize(2).sampleSize(128).outputAfter(40).build()));
        config.maxOutOfOrdernessMillis = 5_000;

        LogAnomalyPipeline.Outputs outputs = LogAnomalyPipeline.build(
                env.fromData(logs(), TypeInformation.of(RawLog.class))
                        .assignTimestampsAndWatermarks(LogAnomalyPipeline.watermarkStrategy(config)), config);
        CloseableIterator<ScoredRecord> scoresIterator = outputs.scores.collectAsync();
        CloseableIterator<Alert> alertsIterator = outputs.alerts.collectAsync();
        CloseableIterator<DeadLetter> deadLettersIterator = outputs.deadLetters.collectAsync();
        // Collect sinks have bounded buffers: drain the outputs while the job runs
        env.executeAsync("pipeline-test");

        CompletableFuture<List<Alert>> alertsFuture = CompletableFuture.supplyAsync(() -> drainUnchecked(alertsIterator));
        CompletableFuture<List<DeadLetter>> deadLettersFuture = CompletableFuture.supplyAsync(() -> drainUnchecked(deadLettersIterator));
        List<ScoredRecord> scores = drain(scoresIterator);
        List<Alert> alerts = alertsFuture.get();
        List<DeadLetter> deadLetters = deadLettersFuture.get();

        assertEquals(NORMAL_WINDOWS + INCIDENT_WINDOWS, scores.size(), "one scored record per window");
        assertEquals(5.0, scores.get(0).getValues()[0], "events per window");

        long incidentStart = START + NORMAL_WINDOWS * 10_000L;
        List<Alert> incidentAlerts = alerts.stream()
                .filter(a -> a.getWindowEnd() > incidentStart && a.getType().equals("ANOMALY")).collect(Collectors.toList());
        assertEquals(1, incidentAlerts.size(), "one alert for the incident, the rest is suppressed by the cool-down: " + alerts);
        assertEquals("KILN-01", incidentAlerts.get(0).getStationId());
        assertTrue(incidentAlerts.get(0).getSummary().contains("zone_temp"), incidentAlerts.get(0).getSummary());
        assertTrue(alerts.size() - incidentAlerts.size() <= 2, "false alerts on normal data: " + alerts);

        List<String> reasons = deadLetters.stream().map(DeadLetter::getReason).sorted().collect(Collectors.toList());
        assertEquals(List.of("NON_TEXT", "NON_TEXT", "NON_TEXT", "NON_TEXT"), reasons);
        assertFalse(deadLetters.stream().anyMatch(d -> d.getRaw() != null && d.getRaw().contains("scada-gw")),
                "unattributed chatter is counted, not dead-lettered");
    }
}
