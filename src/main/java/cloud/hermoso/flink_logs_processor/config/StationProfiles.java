package cloud.hermoso.flink_logs_processor.config;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * All station profiles, by station type. Built from the property groups named
 * {@code Station-<TYPE>}, whose properties are {@code metric.<name> = <label in the log text>}.
 */
public final class StationProfiles implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String GROUP_PREFIX = "Station-";
    public static final String METRIC_PREFIX = "metric.";

    private final Map<String, StationProfile> byType = new HashMap<>();

    public static StationProfiles fromApplicationProperties(Map<String, Properties> applicationProperties) {
        StationProfiles profiles = new StationProfiles();
        for (Map.Entry<String, Properties> group : applicationProperties.entrySet()) {
            if (!group.getKey().startsWith(GROUP_PREFIX)) {
                continue;
            }
            String type = group.getKey().substring(GROUP_PREFIX.length()).toUpperCase(Locale.ROOT);
            Preconditions.checkArgument(type.matches("[A-Z]{2,16}"),
                    "Invalid station type in property group '%s': expected 2 to 16 letters", group.getKey());

            // Properties are unordered: sort metrics by name so that the feature order is stable
            // across restarts. The model state depends on it.
            Map<String, String> metrics = new TreeMap<>();
            for (String name : group.getValue().stringPropertyNames()) {
                if (name.startsWith(METRIC_PREFIX)) {
                    String label = group.getValue().getProperty(name).trim();
                    Preconditions.checkArgument(!label.isEmpty(), "Empty label for '%s' in group '%s'", name, group.getKey());
                    metrics.put(name.substring(METRIC_PREFIX.length()), label);
                }
            }
            profiles.add(new StationProfile(type, new ArrayList<>(metrics.keySet()), new ArrayList<>(metrics.values())));
        }
        Preconditions.checkArgument(!profiles.byType.isEmpty(),
                "No station profile configured: at least one '%s<TYPE>' property group is required", GROUP_PREFIX);
        return profiles;
    }

    public StationProfiles add(StationProfile profile) {
        byType.put(profile.getStationType(), profile);
        return this;
    }

    /** Profile for an upper-case station type, or null when the type is not configured. */
    public StationProfile get(String stationType) {
        return byType.get(stationType);
    }

    public List<String> stationTypes() {
        return new ArrayList<>(byType.keySet());
    }
}
