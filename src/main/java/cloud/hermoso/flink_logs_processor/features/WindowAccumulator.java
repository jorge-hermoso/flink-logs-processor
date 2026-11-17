package cloud.hermoso.flink_logs_processor.features;

/** Running totals of one station's log events within one window. Flink POJO. */
public class WindowAccumulator {
    public long events;
    public long warnings;
    public long errors;
    /** Sum and count of each metric of the station profile; sized on the first event. */
    public double[] sums = new double[0];
    public long[] counts = new long[0];
}
