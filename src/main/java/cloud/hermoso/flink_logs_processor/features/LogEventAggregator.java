package cloud.hermoso.flink_logs_processor.features;

import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.Severity;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Arrays;

/** Incrementally folds log events into a {@link WindowAccumulator}, so windows keep O(1) state. */
public class LogEventAggregator implements AggregateFunction<LogEvent, WindowAccumulator, WindowAccumulator> {
    private static final long serialVersionUID = 1L;

    @Override
    public WindowAccumulator createAccumulator() {
        return new WindowAccumulator();
    }

    @Override
    public WindowAccumulator add(LogEvent event, WindowAccumulator acc) {
        acc.events++;
        if (event.getSeverity() == Severity.WARNING) {
            acc.warnings++;
        } else if (event.getSeverity() == Severity.ERROR) {
            acc.errors++;
        }
        double[] metrics = event.getMetrics();
        if (acc.sums.length < metrics.length) {
            acc.sums = Arrays.copyOf(acc.sums, metrics.length);
            acc.counts = Arrays.copyOf(acc.counts, metrics.length);
        }
        for (int i = 0; i < metrics.length; i++) {
            if (!Double.isNaN(metrics[i])) {
                acc.sums[i] += metrics[i];
                acc.counts[i]++;
            }
        }
        return acc;
    }

    @Override
    public WindowAccumulator getResult(WindowAccumulator acc) {
        return acc;
    }

    @Override
    public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
        int length = Math.max(a.sums.length, b.sums.length);
        WindowAccumulator merged = new WindowAccumulator();
        merged.events = a.events + b.events;
        merged.warnings = a.warnings + b.warnings;
        merged.errors = a.errors + b.errors;
        merged.sums = new double[length];
        merged.counts = new long[length];
        for (WindowAccumulator acc : new WindowAccumulator[]{a, b}) {
            for (int i = 0; i < acc.sums.length; i++) {
                merged.sums[i] += acc.sums[i];
                merged.counts[i] += acc.counts[i];
            }
        }
        return merged;
    }
}
