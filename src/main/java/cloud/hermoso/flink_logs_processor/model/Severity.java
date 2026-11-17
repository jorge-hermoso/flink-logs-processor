package cloud.hermoso.flink_logs_processor.model;

/** Coarse log severity buckets used as features. */
public final class Severity {
    public static final int INFO = 0;
    public static final int WARNING = 1;
    public static final int ERROR = 2;

    private Severity() {
    }
}
