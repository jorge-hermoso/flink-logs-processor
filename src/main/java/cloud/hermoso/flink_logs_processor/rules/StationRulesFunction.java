package cloud.hermoso.flink_logs_processor.rules;

import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import cloud.hermoso.flink_logs_processor.monitor.Metrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Locale;

/**
 * Explicit rules for failure modes the Random Cut Forest cannot see, because the values involved
 * look perfectly normal:
 * <ul>
 *   <li><b>Silence:</b> a station that was logging produces no line for {@code silenceAfterMillis}
 *       of event time. Detected two ways, which produce the same alert id and never both alert:
 *       <ul>
 *         <li>while the station is still silent, by an event-time timer that fires as the other
 *             stations' logs advance the watermark;</li>
 *         <li>when it logs again, by the gap before its new window. Needed when the job catches
 *             up on a backlog: watermarks then advance in big steps, and the windows after the
 *             silence reach this operator before the watermark that would fire the timer.</li>
 *       </ul>
 *       If every station goes silent the watermark stops too; the "no scores" CloudWatch alarm
 *       covers that case. The end-of-input watermark (bounded replays, stop with drain) does
 *       not count as silence.</li>
 *   <li><b>Stuck sensor:</b> a metric that has varied before reports exactly the same window mean
 *       for {@code stuckWindows} consecutive windows. Real readings carry noise; a frozen value
 *       means a frozen sensor or a reading that is no longer logged.</li>
 * </ul>
 * Each rule alerts once per episode and re-arms when the station recovers.
 */
public class StationRulesFunction extends KeyedProcessFunction<String, FeatureVector, Alert> {
    private static final long serialVersionUID = 1L;

    public static final String SILENCE = "SILENCE";
    public static final String STUCK_SENSOR = "STUCK_SENSOR";

    /** Index of the first metric in the feature vector; the ones before are event counts. */
    private static final int FIRST_METRIC = 3;

    private final long silenceAfterMillis;
    private final int stuckWindows;

    private transient ValueState<Long> lastWindowEnd;
    private transient ValueState<Long> silenceTimer;
    private transient ValueState<Boolean> silenceAlerted;
    private transient ValueState<double[]> lastValues;
    private transient ValueState<int[]> repeats;
    private transient ValueState<boolean[]> varied;
    private transient Counter emitted;

    /**
     * @param silenceAfterMillis event time without any line before a station counts as silent; 0 disables the rule
     * @param stuckWindows       consecutive identical windows before a metric counts as stuck; 0 disables the rule
     */
    public StationRulesFunction(long silenceAfterMillis, int stuckWindows) {
        this.silenceAfterMillis = silenceAfterMillis;
        this.stuckWindows = stuckWindows;
    }

    @Override
    public void open(OpenContext openContext) {
        lastWindowEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-last-window-end", Long.class));
        silenceTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-silence-timer", Long.class));
        silenceAlerted = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-silence-alerted", Boolean.class));
        lastValues = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-last-values", double[].class));
        repeats = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-repeats", int[].class));
        varied = getRuntimeContext().getState(new ValueStateDescriptor<>("rules-varied", boolean[].class));
        emitted = Metrics.counter(getRuntimeContext(), "ruleAlertsEmitted");
    }

    @Override
    public void processElement(FeatureVector vector, Context ctx, Collector<Alert> out) throws Exception {
        Long last = lastWindowEnd.value();
        if (silenceAfterMillis > 0 && last != null && vector.getWindowStart() - last >= silenceAfterMillis
                && !Boolean.TRUE.equals(silenceAlerted.value())) {
            // The station was silent, and the timer did not get the chance to say so
            out.collect(silenceAlert(vector.getStationId(), last));
        }
        silenceAlerted.clear();
        lastWindowEnd.update(vector.getWindowEnd());
        if (silenceAfterMillis > 0) {
            Long timer = silenceTimer.value();
            if (timer != null) {
                ctx.timerService().deleteEventTimeTimer(timer);
            }
            long next = vector.getWindowEnd() + silenceAfterMillis;
            ctx.timerService().registerEventTimeTimer(next);
            silenceTimer.update(next);
        }
        if (stuckWindows > 0) {
            checkStuck(vector, out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        silenceTimer.clear();
        Long last = lastWindowEnd.value();
        if (last == null || ctx.timerService().currentWatermark() == Long.MAX_VALUE) {
            // End of input, not silence
            return;
        }
        silenceAlerted.update(true);
        out.collect(silenceAlert(ctx.getCurrentKey(), last));
        // Re-armed by the next feature vector, i.e. when the station logs again
    }

    private Alert silenceAlert(String station, long lastWindowEnd) {
        emitted.inc();
        return alert(station, SILENCE, lastWindowEnd + silenceAfterMillis,
                String.format(Locale.ROOT, "%s silent: no log line since %s", station, Instant.ofEpochMilli(lastWindowEnd)),
                new String[0], new double[0]);
    }

    private void checkStuck(FeatureVector vector, Collector<Alert> out) throws Exception {
        double[] values = vector.getValues();
        int metrics = values.length - FIRST_METRIC;
        double[] previous = lastValues.value();
        int[] count = repeats.value();
        boolean[] hasVaried = varied.value();
        if (previous == null || previous.length != values.length) {
            // First window of this station, or its profile changed
            lastValues.update(values.clone());
            repeats.update(new int[metrics]);
            varied.update(new boolean[metrics]);
            return;
        }
        for (int m = 0; m < metrics; m++) {
            int i = FIRST_METRIC + m;
            if (values[i] == previous[i]) {
                count[m]++;
                // Exactly at the threshold: one alert per episode. Constant metrics never alert.
                if (count[m] == stuckWindows && hasVaried[m]) {
                    String name = vector.getFeatureNames()[i];
                    emitted.inc();
                    out.collect(alert(vector.getStationId(), STUCK_SENSOR, vector.getWindowEnd(),
                            String.format(Locale.ROOT, "%s %s stuck at %.2f for %d windows", vector.getStationId(), name, values[i], count[m] + 1),
                            new String[]{name}, new double[]{values[i]}));
                }
            } else {
                count[m] = 0;
                hasVaried[m] = true;
            }
        }
        lastValues.update(values.clone());
        repeats.update(count);
        varied.update(hasVaried);
    }

    private static Alert alert(String station, String type, long windowEnd, String summary, String[] names, double[] values) {
        return new Alert(station + "#" + type + "#" + windowEnd, type, station, Instant.ofEpochMilli(windowEnd).toString(),
                windowEnd, 0, 1.0, 0, summary, names, values, new double[0]);
    }
}
