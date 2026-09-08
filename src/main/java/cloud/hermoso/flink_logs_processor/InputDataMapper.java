package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.model.InputData;
import cloud.hermoso.flink_logs_processor.operator.RcfInputMapper;

import java.util.Arrays;

public class InputDataMapper implements RcfInputMapper<InputData> {
    @Override
    public float[] apply(InputData inputData) {
        // Return a copy of the values
        return Arrays.copyOf(inputData.getValues(), inputData.getValues().length);
    }
}
