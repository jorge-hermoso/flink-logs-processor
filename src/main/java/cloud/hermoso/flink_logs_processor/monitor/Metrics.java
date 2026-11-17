package cloud.hermoso.flink_logs_processor.monitor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

/** Custom metrics, all registered in the group Amazon Managed Service for Apache Flink exports to CloudWatch. */
public final class Metrics {

    /** Metrics in this group are published to CloudWatch by Managed Service for Apache Flink. */
    public static final String GROUP = "kinesisAnalytics";

    private Metrics() {
    }

    public static Counter counter(RuntimeContext context, String name) {
        return context.getMetricGroup().addGroup(GROUP).counter(name);
    }
}
