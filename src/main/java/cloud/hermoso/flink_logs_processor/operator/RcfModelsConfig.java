package cloud.hermoso.flink_logs_processor.operator;

import cloud.hermoso.flink_logs_processor.ApplicationConfigUtils;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Model parameters by model key. Built from the property groups {@code ModelParameters-DEFAULT}
 * and {@code ModelParameters-<KEY>}, where the key is either a station id ({@code KILN-01}) or a
 * station type ({@code KILN}). A specific group only needs the properties it overrides.
 */
@ToString
public class RcfModelsConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String GROUP_PREFIX = "ModelParameters-";
    public static final String DEFAULT_KEY = "DEFAULT";

    @Getter
    private final RcfModelParams defaultModelParams;
    private final Map<String, RcfModelParams> overrides = new HashMap<>();

    public RcfModelsConfig(RcfModelParams defaultModelParams) {
        this.defaultModelParams = defaultModelParams;
    }

    public static RcfModelsConfig fromApplicationProperties(Map<String, Properties> applicationProperties) {
        RcfModelParams defaults = RcfModelParams.fromProperties(
                ApplicationConfigUtils.optionalGroup(applicationProperties, GROUP_PREFIX + DEFAULT_KEY),
                RcfModelParams.defaults());
        RcfModelsConfig config = new RcfModelsConfig(defaults);
        for (Map.Entry<String, Properties> group : applicationProperties.entrySet()) {
            String groupId = group.getKey();
            if (groupId.startsWith(GROUP_PREFIX) && !groupId.equals(GROUP_PREFIX + DEFAULT_KEY)) {
                config.addOverride(groupId.substring(GROUP_PREFIX.length()), RcfModelParams.fromProperties(group.getValue(), defaults));
            }
        }
        return config;
    }

    public RcfModelsConfig addOverride(String key, RcfModelParams params) {
        overrides.put(key, params);
        return this;
    }

    /** Most specific parameters for a station: by station id, then by station type, then the defaults. */
    public RcfModelParams getModelParams(String stationId) {
        RcfModelParams params = overrides.get(stationId);
        if (params == null) {
            int dash = stationId.indexOf('-');
            params = dash > 0 ? overrides.get(stationId.substring(0, dash)) : null;
        }
        return params != null ? params : defaultModelParams;
    }
}
