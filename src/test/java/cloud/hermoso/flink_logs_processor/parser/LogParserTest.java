package cloud.hermoso.flink_logs_processor.parser;

import cloud.hermoso.flink_logs_processor.TestProfiles;
import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.Severity;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogParserTest {

    private static final long ARRIVAL = Instant.parse("2026-11-17T08:01:05Z").toEpochMilli();
    private static final long EVENT_TIME = Instant.parse("2026-11-17T08:01:02.123Z").toEpochMilli();

    private final LogParser parser = new LogParser(TestProfiles.profiles(), 1000, 60_000);

    private LogParser.Result parse(String line) {
        return parser.parse(new RawLog(line, ARRIVAL, "shard-0", "1"));
    }

    private LogEvent event(String line) {
        LogParser.Result result = parse(line);
        assertTrue(result.isEvent(), () -> "rejected as " + result.rejection);
        return result.event;
    }

    @Test
    void syslogStyleLine() {
        LogEvent event = event("Nov 17 08:01:02 kiln-01 kilnd[412]: <info> zone temp reading 842.3C (setpoint 840.0C) o2=3.1% belt 1.20 m/min");
        assertEquals("KILN-01", event.getStationId());
        assertEquals("KILN", event.getStationType());
        assertEquals(Instant.parse("2026-11-17T08:01:02Z").toEpochMilli(), event.getTimestamp());
        assertEquals(Severity.INFO, event.getSeverity());
        assertArrayEquals(new double[]{3.1, 842.3}, event.getMetrics());
    }

    @Test
    void epochMillisPipeSeparatedLine() {
        LogEvent event = event(EVENT_TIME + "|PRESS-02|W|cycle 18823 done; peak_force=312.4kN cycle_time=4.21s");
        assertEquals("PRESS-02", event.getStationId());
        assertEquals(EVENT_TIME, event.getTimestamp());
        assertEquals(Severity.WARNING, event.getSeverity());
        assertArrayEquals(new double[]{312.4}, event.getMetrics());
    }

    @Test
    void jsonLine() {
        LogEvent event = event("{\"ts\":\"2026-11-17T08:01:02.123Z\",\"unit\":\"PACKER-01\",\"lvl\":\"error\",\"msg\":\"rate 42.0 units/min, rejects 0.8 %\"}");
        assertEquals("PACKER-01", event.getStationId());
        assertEquals(EVENT_TIME, event.getTimestamp());
        assertEquals(Severity.ERROR, event.getSeverity());
        assertArrayEquals(new double[]{42.0, 0.8}, event.getMetrics());
    }

    @Test
    void isoTimestampWithOffsetAndCommaFraction() {
        LogEvent event = event("[2026-11-17 10:01:02,123+02:00] KILN-01 | INFO | idle");
        assertEquals(EVENT_TIME, event.getTimestamp());
    }

    @Test
    void negativeAndScientificNumbers() {
        assertArrayEquals(new double[]{-61.2, 1500.0},
                event("2026-11-17T08:01:02Z KILN-03 o2: -61.2 zone temp reading = 1.5e3").getMetrics());
    }

    @Test
    void metricLabelMustStartAtAWordBoundary() {
        // "co2" must not be read as the "o2" metric
        assertTrue(Double.isNaN(event("2026-11-17T08:01:02Z KILN-01 co2=400").getMetrics()[0]));
    }

    @Test
    void lineWithoutReadingsStillCountsAsAnEvent() {
        LogEvent event = event("2026-11-17T08:01:02Z ERROR KILN-01 burner flame-out detected");
        assertEquals(Severity.ERROR, event.getSeverity());
        assertTrue(Double.isNaN(event.getMetrics()[0]));
        assertTrue(Double.isNaN(event.getMetrics()[1]));
    }

    @Test
    void missingTimestampFallsBackToArrivalTime() {
        LogParser.Result result = parse("KILN-01 zone temp reading 840.0");
        assertEquals(ARRIVAL, result.event.getTimestamp());
        assertTrue(result.timestampFromArrival);
    }

    @Test
    void invalidTimestampFallsBackToArrivalTime() {
        assertEquals(ARRIVAL, event("2026-13-45T08:01:02Z KILN-01 zone temp reading 840.0").getTimestamp());
    }

    @Test
    void timestampFarInTheFutureIsNotTrusted() {
        // A controller with a broken clock must not be able to drag the watermark forward
        LogParser.Result result = parse("2031-01-01T00:00:00Z KILN-01 zone temp reading 840.0");
        assertEquals(ARRIVAL, result.event.getTimestamp());
        assertTrue(result.timestampFromArrival);
    }

    @Test
    void syslogDecemberLineArrivingInJanuaryBelongsToThePreviousYear() {
        long arrival = Instant.parse("2027-01-01T00:00:03Z").toEpochMilli();
        LogParser.Result result = parser.parse(new RawLog("Dec 31 23:59:59 kiln-01 kilnd: ok", arrival, "s", "1"));
        assertEquals(Instant.parse("2026-12-31T23:59:59Z").toEpochMilli(), result.event.getTimestamp());
    }

    @Test
    void unknownStationIsRejectedAsNoStation() {
        assertEquals(LogParser.Rejection.NO_STATION, parse("2026-11-17T08:01:02Z INFO scada-gw heartbeat ok seq=1").rejection);
        assertEquals(LogParser.Rejection.NO_STATION, parse("2026-11-17T08:01:02Z INFO LATHE-07 spindle 1200 rpm").rejection);
    }

    @Test
    void garbageIsRejectedNotThrown() {
        assertEquals(LogParser.Rejection.NON_TEXT, parse("\u0000\u0001\u0002�� KILN-01").rejection);
        assertEquals(LogParser.Rejection.OVERSIZED, parse("KILN-01 " + "x".repeat(2000)).rejection);
        assertEquals(LogParser.Rejection.OVERSIZED, parse(null).rejection);
        assertFalse(parse("").isEvent());
    }

    @Test
    void lineWithoutTimestampAndWithoutArrivalTimeIsRejected() {
        LogParser.Result result = parser.parse(new RawLog("KILN-01 zone temp reading 840.0", 0, null, null));
        assertEquals(LogParser.Rejection.NO_TIMESTAMP, result.rejection);
    }
}
