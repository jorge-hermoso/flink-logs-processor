package cloud.hermoso.flink_logs_processor.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * What to read from the logs of one type of station: a list of named numeric readings, each found
 * by the text label that precedes the number in the log line.
 * <p>
 * Example: metric {@code zone_temp} with label {@code zone temp reading} matches
 * {@code "... zone temp reading 842.3C (setpoint 840.0C) ..."} and yields 842.3.
 */
public final class StationProfile implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Features every station has, before its own metrics. */
    public static final List<String> COMMON_FEATURES = List.of("events", "warnings", "errors");

    private static final String NUMBER = "(-?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?)";

    private final String stationType;
    private final String[] metricNames;
    private final Pattern[] metricPatterns;

    /**
     * @param metricNames  metric names, in feature order
     * @param metricLabels the label of each metric, as it appears in the log text (case-insensitive)
     */
    public StationProfile(String stationType, List<String> metricNames, List<String> metricLabels) {
        if (metricNames.size() != metricLabels.size()) {
            throw new IllegalArgumentException("Metric names and labels must have the same size");
        }
        this.stationType = stationType;
        this.metricNames = metricNames.toArray(new String[0]);
        this.metricPatterns = metricLabels.stream().map(StationProfile::labelPattern).toArray(Pattern[]::new);
    }

    /**
     * Label, not preceded by a word character, then an optional quote / separator, then a number.
     * Whitespace inside the label matches any run of whitespace.
     */
    private static Pattern labelPattern(String label) {
        String[] words = label.trim().split("\\s+");
        StringBuilder regex = new StringBuilder("(?<![A-Za-z0-9_])");
        for (int i = 0; i < words.length; i++) {
            if (i > 0) {
                regex.append("\\s+");
            }
            regex.append(Pattern.quote(words[i]));
        }
        regex.append("[\"']?\\s*[=:]?\\s*").append(NUMBER);
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }

    public String getStationType() {
        return stationType;
    }

    public int metricCount() {
        return metricNames.length;
    }

    public Pattern metricPattern(int index) {
        return metricPatterns[index];
    }

    /** Names of the full feature vector: the common features followed by this profile's metrics. */
    public String[] featureNames() {
        List<String> names = new ArrayList<>(COMMON_FEATURES);
        names.addAll(List.of(metricNames));
        return names.toArray(new String[0]);
    }
}
