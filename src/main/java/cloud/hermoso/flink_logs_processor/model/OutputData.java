package cloud.hermoso.flink_logs_processor.model;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class OutputData {
    private long timestamp;
    private String key;
    private float[] values;
    private double score;
}
