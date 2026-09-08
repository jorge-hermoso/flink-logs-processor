package cloud.hermoso.flink_logs_processor.model;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class InputData {

    private long timestamp;
    private String key;
    private float[] values;
}


