package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.config.StationProfiles;
import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.Severity;
import cloud.hermoso.flink_logs_processor.parser.LogParser;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract between the two applications in this repository: the logs written by
 * {@code log-generator} must be understood by the detector with the station profiles shipped in
 * {@code flink-application-properties-dev.json}.
 * <p>
 * {@code generated-plant.log} is real generator output. Regenerate it when the generator changes:
 * <pre>
 * cd log-generator &amp;&amp; python3 -m loggen --seed 11 --start 2026-11-17T08:00:00Z --duration 60 --speed 0 \
 *   --anomaly-rate 0 --garbage-ratio 0.01 --inject KILN-01:error_burst:20:10 &gt; ../src/test/resources/generated-plant.log
 * </pre>
 */
class GeneratedLogsContractTest {

    private static final long START = Instant.parse("2026-11-17T08:00:00Z").toEpochMilli();

    private static Path resource(String name) throws Exception {
        return Path.of(GeneratedLogsContractTest.class.getClassLoader().getResource(name).toURI());
    }

    @Test
    void detectorUnderstandsEveryStationOfTheGenerator() throws Exception {
        Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties(
                resource("flink-application-properties-dev.json").toString());
        LogParser parser = new LogParser(StationProfiles.fromApplicationProperties(properties), 16_384, 300_000);

        List<String> lines = Files.readAllLines(resource("generated-plant.log"), StandardCharsets.UTF_8);
        Map<String, Integer> completeTelemetry = new HashMap<>();
        Map<String, Integer> rejections = new HashMap<>();
        int kilnErrors = 0;

        for (String line : lines) {
            LogParser.Result result = parser.parse(new RawLog(line, START + 70_000, "shard-0", "0"));
            if (!result.isEvent()) {
                rejections.merge(result.rejection.name(), 1, Integer::sum);
                continue;
            }
            LogEvent event = result.event;
            assertTrue(event.getTimestamp() >= START && event.getTimestamp() < START + 60_000,
                    () -> "timestamp not read from the line: " + line);
            if (Arrays.stream(event.getMetrics()).noneMatch(Double::isNaN)) {
                completeTelemetry.merge(event.getStationId(), 1, Integer::sum);
            }
            if (event.getStationId().equals("KILN-01") && event.getSeverity() == Severity.ERROR) {
                kilnErrors++;
            }
        }

        // Telemetry periods are 1 to 2 seconds: at least 25 complete readings per station per minute
        for (String station : List.of("MIXER-01", "EXTRUDER-01", "EXTRUDER-02", "KILN-01", "PRESS-01", "PRESS-02", "PACKER-01")) {
            assertTrue(completeTelemetry.getOrDefault(station, 0) >= 25,
                    () -> station + ": only " + completeTelemetry.get(station) + " lines with all metrics");
        }
        assertEquals(7, completeTelemetry.size(), "no phantom stations: " + completeTelemetry.keySet());
        assertTrue(kilnErrors >= 15, "error burst not recognised as errors: " + kilnErrors);
        assertTrue(rejections.getOrDefault("NO_STATION", 0) > 0, "chatter should be unattributed");
        assertTrue(rejections.keySet().stream().allMatch(r -> r.equals("NO_STATION") || r.equals("NON_TEXT")),
                () -> "unexpected rejections: " + rejections);
    }
}
