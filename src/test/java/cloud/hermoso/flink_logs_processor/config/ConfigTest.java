package cloud.hermoso.flink_logs_processor.config;

import cloud.hermoso.flink_logs_processor.LogAnomalyPipeline;
import cloud.hermoso.flink_logs_processor.operator.RcfModelParams;
import cloud.hermoso.flink_logs_processor.operator.RcfModelsConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigTest {

    private static Properties props(String... keyValues) {
        Properties properties = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    private static Map<String, Properties> minimal() {
        Map<String, Properties> groups = new HashMap<>();
        groups.put("Station-kiln", props("metric.zone_temp", "zone temp reading", "metric.o2", "o2"));
        return groups;
    }

    @Test
    void stationProfileMetricsAreOrderedByName() {
        StationProfiles profiles = StationProfiles.fromApplicationProperties(minimal());
        assertArrayEquals(new String[]{"events", "warnings", "errors", "o2", "zone_temp"}, profiles.get("KILN").featureNames());
        assertNull(profiles.get("PRESS"));
    }

    @Test
    void atLeastOneStationProfileIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> StationProfiles.fromApplicationProperties(new HashMap<>()));
    }

    @Test
    void modelParametersResolveByStationThenTypeThenDefault() {
        Map<String, Properties> groups = minimal();
        groups.put("ModelParameters-DEFAULT", props("sample.size", "128", "output.after", "32"));
        groups.put("ModelParameters-KILN", props("number.of.trees", "30"));
        groups.put("ModelParameters-KILN-02", props("shingle.size", "8"));
        RcfModelsConfig config = RcfModelsConfig.fromApplicationProperties(groups);

        RcfModelParams byType = config.getModelParams("KILN-01");
        assertEquals(30, byType.getNumberOfTrees());
        assertEquals(128, byType.getSampleSize(), "a partial override keeps the configured defaults");

        assertEquals(8, config.getModelParams("KILN-02").getShingleSize());
        assertEquals(50, config.getModelParams("KILN-02").getNumberOfTrees());
        assertEquals(50, config.getModelParams("PRESS-01").getNumberOfTrees());
        assertEquals(32, config.getModelParams("PRESS-01").getOutputAfter());
    }

    @Test
    void invalidValuesNameTheProperty() {
        Map<String, Properties> groups = minimal();
        groups.put("ModelParameters-DEFAULT", props("sample.size", "lots"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> RcfModelsConfig.fromApplicationProperties(groups));
        assertTrue(e.getMessage().contains("sample.size"));

        groups.put("ModelParameters-DEFAULT", props("sample.size", "64", "output.after", "500"));
        assertThrows(IllegalArgumentException.class, () -> RcfModelsConfig.fromApplicationProperties(groups));
    }

    @Test
    void pipelineConfigHasDefaultsAndValidates() {
        LogAnomalyPipeline.Config config = LogAnomalyPipeline.Config.fromApplicationProperties(minimal());
        assertEquals(10_000, config.windowSizeMillis);

        Map<String, Properties> groups = minimal();
        groups.put("Features", props("window.size.ms", "0"));
        assertThrows(IllegalArgumentException.class, () -> LogAnomalyPipeline.Config.fromApplicationProperties(groups));

        groups.put("Features", props("window.size.ms", "5000"));
        groups.put("RcfOperator", props("model.state.ttl.hours", "2"));
        LogAnomalyPipeline.Config tuned = LogAnomalyPipeline.Config.fromApplicationProperties(groups);
        assertEquals(5_000, tuned.windowSizeMillis);
        assertEquals(7_200_000, tuned.modelStateTtlMillis);
    }
}
