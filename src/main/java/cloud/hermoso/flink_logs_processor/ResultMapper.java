package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.model.InputData;
import cloud.hermoso.flink_logs_processor.model.OutputData;
import cloud.hermoso.flink_logs_processor.operator.RcfResultMapper;

import java.util.Arrays;

public class ResultMapper implements RcfResultMapper<InputData, OutputData> {
    @Override
    public OutputData apply(InputData inputData, Double score) {
        return new OutputData(
                inputData.getTimestamp(),
                inputData.getKey(),
                Arrays.copyOf(inputData.getValues(), inputData.getValues().length),
                score);
    }
}
