package cloud.hermoso.flink_logs_processor;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Typed access to the runtime properties. Every failure names the offending property, so a
 * misconfigured deployment fails at start-up with an actionable message instead of at the first record.
 */
public final class ApplicationConfigUtils {

    private ApplicationConfigUtils() {
    }

    /** Returns the property group, failing with a clear message when it is absent. */
    public static Properties mandatoryGroup(Map<String, Properties> applicationProperties, String groupId) {
        Properties group = applicationProperties.get(groupId);
        Preconditions.checkArgument(group != null, "Missing application property group '%s'", groupId);
        return group;
    }

    /** Returns the property group, or an empty one when it is absent (all its properties have defaults). */
    public static Properties optionalGroup(Map<String, Properties> applicationProperties, String groupId) {
        Properties group = applicationProperties.get(groupId);
        return group != null ? group : new Properties();
    }

    public static String parseMandatoryString(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(value),
                "Missing application parameter '%s'", propertyName);
        return value.trim();
    }

    public static String parseString(Properties properties, String propertyName, String defaultValue) {
        String value = properties.getProperty(propertyName);
        return StringUtils.isNullOrWhitespaceOnly(value) ? defaultValue : value.trim();
    }

    public static int parseInt(Properties properties, String propertyName, int defaultValue) {
        return parse(properties, propertyName, defaultValue, Integer::parseInt);
    }

    public static long parseLong(Properties properties, String propertyName, long defaultValue) {
        return parse(properties, propertyName, defaultValue, Long::parseLong);
    }

    public static double parseDouble(Properties properties, String propertyName, double defaultValue) {
        return parse(properties, propertyName, defaultValue, Double::parseDouble);
    }

    public static boolean parseBoolean(Properties properties, String propertyName, boolean defaultValue) {
        return parse(properties, propertyName, defaultValue, value -> {
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return Boolean.parseBoolean(value);
            }
            throw new IllegalArgumentException("expected true or false");
        });
    }

    private static <T> T parse(Properties properties, String propertyName, T defaultValue, Function<String, T> parser) {
        String value = properties.getProperty(propertyName);
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return defaultValue;
        }
        try {
            return parser.apply(value.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid value '" + value + "' for application parameter '" + propertyName + "'", e);
        }
    }
}
