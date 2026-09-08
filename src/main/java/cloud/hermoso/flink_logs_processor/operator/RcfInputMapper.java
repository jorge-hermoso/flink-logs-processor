package cloud.hermoso.flink_logs_processor.operator;

import org.apache.flink.api.common.functions.Function;

/**
 * Maps the model input to a float array
 */
@FunctionalInterface
public interface RcfInputMapper<IN> extends Function, java.util.function.Function<IN, float[]> {
}
