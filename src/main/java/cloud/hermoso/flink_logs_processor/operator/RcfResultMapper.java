package cloud.hermoso.flink_logs_processor.operator;

@FunctionalInterface
public interface RcfResultMapper<IN, OUT> extends org.apache.flink.api.common.functions.Function, java.util.function.BiFunction<IN, Double, OUT>{
}