package cloud.hermoso.flink_logs_processor.source;

import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.parser.LogParser;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

/**
 * Event time of a raw line, assigned in the source so that Flink tracks watermarks per Kinesis
 * shard. Watermarks generated after the source are a single stream: while catching up, the reader
 * may read far ahead in one shard and every record of the other shards would then be late.
 */
public class RawLogTimestampAssigner implements SerializableTimestampAssigner<RawLog> {
    private static final long serialVersionUID = 1L;

    private final long maxFutureSkewMillis;

    public RawLogTimestampAssigner(long maxFutureSkewMillis) {
        this.maxFutureSkewMillis = maxFutureSkewMillis;
    }

    @Override
    public long extractTimestamp(RawLog raw, long recordTimestamp) {
        long timestamp = LogParser.eventTime(raw.getLine(), raw.getArrivalTimestamp(), maxFutureSkewMillis);
        // Lines without any time are rejected by the parser; give them a neutral timestamp
        return timestamp == LogParser.NO_TIMESTAMP ? Math.max(recordTimestamp, 0L) : timestamp;
    }
}
