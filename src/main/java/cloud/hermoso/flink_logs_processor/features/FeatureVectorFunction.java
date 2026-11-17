package cloud.hermoso.flink_logs_processor.features;

import cloud.hermoso.flink_logs_processor.config.StationProfile;
import cloud.hermoso.flink_logs_processor.config.StationProfiles;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import cloud.hermoso.flink_logs_processor.monitor.Metrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Closes a window: turns its totals into the feature vector
 * {@code [events, warnings, errors, mean(metric 1), ..., mean(metric n)]}.
 * <p>
 * A metric that was not logged during the window keeps its last known value (the reading did not
 * change just because nobody printed it). Until every metric of the station has been seen at least
 * once there is no meaningful vector, so the window is skipped and counted in {@code incompleteWindows}.
 */
public class FeatureVectorFunction extends ProcessWindowFunction<WindowAccumulator, FeatureVector, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<double[]> LAST_METRICS =
            new ValueStateDescriptor<>("last-metrics", double[].class);

    private final StationProfiles profiles;
    private transient Counter incompleteWindows;

    public FeatureVectorFunction(StationProfiles profiles) {
        this.profiles = profiles;
    }

    @Override
    public void open(OpenContext openContext) {
        incompleteWindows = Metrics.counter(getRuntimeContext(), "incompleteWindows");
    }

    @Override
    public void process(String stationId, Context context, Iterable<WindowAccumulator> elements, Collector<FeatureVector> out) throws Exception {
        WindowAccumulator acc = elements.iterator().next();
        StationProfile profile = profiles.get(stationId.substring(0, stationId.indexOf('-')));
        if (profile == null) {
            // Profile removed by a configuration change while events were in flight
            return;
        }
        int metricCount = profile.metricCount();

        ValueState<double[]> lastState = context.globalState().getState(LAST_METRICS);
        double[] last = lastState.value();
        if (last == null || last.length != metricCount) {
            // First window of this station, or its profile changed
            last = new double[metricCount];
            java.util.Arrays.fill(last, Double.NaN);
        }

        boolean complete = true;
        for (int i = 0; i < metricCount; i++) {
            if (i < acc.counts.length && acc.counts[i] > 0) {
                last[i] = acc.sums[i] / acc.counts[i];
            }
            complete &= !Double.isNaN(last[i]);
        }
        lastState.update(last);

        if (!complete) {
            incompleteWindows.inc();
            return;
        }

        double[] values = new double[StationProfile.COMMON_FEATURES.size() + metricCount];
        values[0] = acc.events;
        values[1] = acc.warnings;
        values[2] = acc.errors;
        System.arraycopy(last, 0, values, StationProfile.COMMON_FEATURES.size(), metricCount);

        out.collect(new FeatureVector(
                stationId, context.window().getStart(), context.window().getEnd(), profile.featureNames(), values));
    }
}
