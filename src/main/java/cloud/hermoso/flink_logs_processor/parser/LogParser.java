package cloud.hermoso.flink_logs_processor.parser;

import cloud.hermoso.flink_logs_processor.config.StationProfile;
import cloud.hermoso.flink_logs_processor.config.StationProfiles;
import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.Severity;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns one unstructured log line into a {@link LogEvent}. Format-agnostic on purpose: the line
 * layouts of the plant's controllers differ per vendor and change with firmware updates, so
 * nothing here depends on field positions. The parser only looks for
 * <ul>
 *   <li>a station id ({@code <letters>-<digits>}) whose type has a configured profile,</li>
 *   <li>a timestamp in any of the supported notations, falling back to the stream arrival time,</li>
 *   <li>a severity keyword,</li>
 *   <li>the numeric readings listed in the station profile, located by their label.</li>
 * </ul>
 * Never throws on bad input: every outcome is reported through {@link Result}.
 */
public final class LogParser implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Why a line was not turned into an event. */
    public enum Rejection {
        /** Longer than the configured maximum: almost certainly not a log line. */
        OVERSIZED,
        /** Mostly control or undecodable characters. */
        NON_TEXT,
        /** No timestamp in the line and no arrival time to fall back on. */
        NO_TIMESTAMP,
        /** Valid text, but not attributable to a known station (heartbeats, gateway chatter...). */
        NO_STATION
    }

    /** Outcome of parsing one line: exactly one of event / rejection is set. */
    public static final class Result {
        public final LogEvent event;
        public final Rejection rejection;
        /** True when the line's own timestamp was unusable and the arrival time was used instead. */
        public final boolean timestampFromArrival;

        private Result(LogEvent event, Rejection rejection, boolean timestampFromArrival) {
            this.event = event;
            this.rejection = rejection;
            this.timestampFromArrival = timestampFromArrival;
        }

        public boolean isEvent() {
            return event != null;
        }
    }

    private static final Pattern STATION = Pattern.compile("(?<![A-Za-z0-9])([A-Za-z]{2,16})[-_](\\d{1,4})(?![A-Za-z0-9])");

    private static final Pattern EPOCH_MILLIS = Pattern.compile("^\\s*(\\d{13})(?!\\d)");
    private static final Pattern EPOCH_SECONDS = Pattern.compile("^\\s*(\\d{10})(?![\\d])");
    private static final Pattern ISO = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2})[T ](\\d{2}:\\d{2}:\\d{2})(?:[.,](\\d{1,9}))?\\s?(Z|[+-]\\d{2}:?\\d{2})?");
    private static final Pattern SYSLOG = Pattern.compile("^\\s*([A-Z][a-z]{2})\\s+(\\d{1,2})\\s+(\\d{2}:\\d{2}:\\d{2})");

    /** Single letter level between pipes, e.g. {@code ...|W|...}. */
    private static final Pattern PIPE_LEVEL = Pattern.compile("\\|\\s*([TDIWEFC])\\s*\\|");
    private static final Pattern WORD_LEVEL = Pattern.compile(
            "(?<![A-Za-z])(trace|debug|info|notice|warn|warning|err|error|crit|critical|fatal|alarm|alert|emerg)(?![A-Za-z])",
            Pattern.CASE_INSENSITIVE);

    private static final Map<String, Month> MONTHS = Map.ofEntries(
            Map.entry("Jan", Month.JANUARY), Map.entry("Feb", Month.FEBRUARY), Map.entry("Mar", Month.MARCH),
            Map.entry("Apr", Month.APRIL), Map.entry("May", Month.MAY), Map.entry("Jun", Month.JUNE),
            Map.entry("Jul", Month.JULY), Map.entry("Aug", Month.AUGUST), Map.entry("Sep", Month.SEPTEMBER),
            Map.entry("Oct", Month.OCTOBER), Map.entry("Nov", Month.NOVEMBER), Map.entry("Dec", Month.DECEMBER));

    private static final long ONE_DAY_MS = 86_400_000L;
    /** Returned by {@link #eventTime} when a line has no usable time. */
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    private final StationProfiles profiles;
    private final int maxLineLength;
    private final long maxFutureSkewMillis;

    /**
     * @param maxLineLength       lines longer than this are rejected as {@link Rejection#OVERSIZED}
     * @param maxFutureSkewMillis a line timestamp further than this ahead of the arrival time is
     *                            not trusted. A single clock gone wrong would otherwise push the
     *                            watermark into the future and make every other station's data late.
     */
    public LogParser(StationProfiles profiles, int maxLineLength, long maxFutureSkewMillis) {
        this.profiles = profiles;
        this.maxLineLength = maxLineLength;
        this.maxFutureSkewMillis = maxFutureSkewMillis;
    }

    public Result parse(RawLog raw) {
        String line = raw.getLine();
        if (line == null || line.length() > maxLineLength) {
            return rejected(Rejection.OVERSIZED);
        }
        if (isNonText(line)) {
            return rejected(Rejection.NON_TEXT);
        }

        String[] station = findStation(line);
        if (station == null) {
            return rejected(Rejection.NO_STATION);
        }
        StationProfile profile = profiles.get(station[0]);

        long arrival = raw.getArrivalTimestamp();
        long lineTimestamp = findTimestamp(line, arrival);
        long timestamp = resolve(lineTimestamp, arrival, maxFutureSkewMillis);
        if (timestamp == NO_TIMESTAMP) {
            return rejected(Rejection.NO_TIMESTAMP);
        }
        boolean fromArrival = timestamp != lineTimestamp;

        double[] metrics = new double[profile.metricCount()];
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = findMetric(profile.metricPattern(i), line);
        }

        LogEvent event = new LogEvent(timestamp, station[0] + "-" + station[1], station[0], findSeverity(line), metrics);
        return new Result(event, null, fromArrival);
    }

    /**
     * Event time of a line: the timestamp printed in it, or the arrival time when the line has
     * none or when it is more than {@code maxFutureSkewMillis} ahead of the arrival time.
     * Returns {@link #NO_TIMESTAMP} when neither is available.
     * <p>
     * Used both by the parser and by the source's watermark strategy, so that the element
     * timestamp and {@link LogEvent#getTimestamp()} always agree.
     */
    public static long eventTime(String line, long arrival, long maxFutureSkewMillis) {
        return resolve(line == null ? NO_TIMESTAMP : findTimestamp(line, arrival), arrival, maxFutureSkewMillis);
    }

    private static long resolve(long lineTimestamp, long arrival, long maxFutureSkewMillis) {
        if (lineTimestamp == NO_TIMESTAMP || (arrival > 0 && lineTimestamp > arrival + maxFutureSkewMillis)) {
            return arrival > 0 ? arrival : NO_TIMESTAMP;
        }
        return lineTimestamp;
    }

    private static Result rejected(Rejection rejection) {
        return new Result(null, rejection, false);
    }

    /** More than 10% control / replacement characters. */
    private static boolean isNonText(String line) {
        int suspicious = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if ((c < 0x20 && c != '\t') || c == '�') {
                suspicious++;
            }
        }
        return suspicious * 10 > line.length();
    }

    /** First {@code <letters>-<digits>} token whose type has a profile: [type, number], or null. */
    private String[] findStation(String line) {
        Matcher matcher = STATION.matcher(line);
        while (matcher.find()) {
            String type = matcher.group(1).toUpperCase(Locale.ROOT);
            if (profiles.get(type) != null) {
                return new String[]{type, matcher.group(2)};
            }
        }
        return null;
    }

    private static long findTimestamp(String line, long arrival) {
        try {
            Matcher matcher = EPOCH_MILLIS.matcher(line);
            if (matcher.find()) {
                return Long.parseLong(matcher.group(1));
            }
            matcher = EPOCH_SECONDS.matcher(line);
            if (matcher.find()) {
                return Long.parseLong(matcher.group(1)) * 1000L;
            }
            matcher = ISO.matcher(line);
            if (matcher.find()) {
                LocalDateTime dateTime = LocalDateTime.of(LocalDate.parse(matcher.group(1)), LocalTime.parse(matcher.group(2)));
                String fraction = matcher.group(3);
                if (fraction != null) {
                    dateTime = dateTime.plusNanos(Long.parseLong((fraction + "000000000").substring(0, 9)));
                }
                // Controllers that do not print a zone are assumed to log in UTC
                String zone = matcher.group(4);
                ZoneOffset offset = zone == null || zone.equals("Z") ? ZoneOffset.UTC : ZoneOffset.of(zone);
                return dateTime.toInstant(offset).toEpochMilli();
            }
            matcher = SYSLOG.matcher(line);
            if (matcher.find() && arrival > 0 && MONTHS.containsKey(matcher.group(1))) {
                // Syslog timestamps have no year: take it from the arrival time
                int year = Instant.ofEpochMilli(arrival).atOffset(ZoneOffset.UTC).getYear();
                LocalDateTime dateTime = LocalDateTime.of(
                        LocalDate.of(year, MONTHS.get(matcher.group(1)), Integer.parseInt(matcher.group(2))),
                        LocalTime.parse(matcher.group(3)));
                long millis = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
                // "Dec 31 23:59:59" arriving on Jan 1st belongs to the previous year
                return millis > arrival + ONE_DAY_MS ? dateTime.minusYears(1).toInstant(ZoneOffset.UTC).toEpochMilli() : millis;
            }
        } catch (DateTimeException | NumberFormatException e) {
            // Looked like a timestamp but is not a valid one (e.g. month 13): treat as absent
        }
        return NO_TIMESTAMP;
    }

    private static int findSeverity(String line) {
        Matcher pipe = PIPE_LEVEL.matcher(line);
        if (pipe.find()) {
            switch (pipe.group(1).charAt(0)) {
                case 'W':
                    return Severity.WARNING;
                case 'E':
                case 'F':
                case 'C':
                    return Severity.ERROR;
                default:
                    return Severity.INFO;
            }
        }
        Matcher word = WORD_LEVEL.matcher(line);
        if (word.find()) {
            String level = word.group(1).toLowerCase(Locale.ROOT);
            if (level.startsWith("warn")) {
                return Severity.WARNING;
            }
            if (level.startsWith("err") || level.startsWith("crit") || level.equals("fatal")
                    || level.equals("alarm") || level.equals("alert") || level.equals("emerg")) {
                return Severity.ERROR;
            }
        }
        return Severity.INFO;
    }

    private static double findMetric(Pattern pattern, String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            try {
                double value = Double.parseDouble(matcher.group(1));
                return Double.isFinite(value) ? value : Double.NaN;
            } catch (NumberFormatException e) {
                return Double.NaN;
            }
        }
        return Double.NaN;
    }
}
