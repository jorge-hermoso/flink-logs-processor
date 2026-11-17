package cloud.hermoso.flink_logs_processor.parser;

import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.monitor.Metrics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Parses raw lines. Lines that cannot be used go to the dead-letter side output instead of
 * failing the job: one malformed record must never put the application in a restart loop.
 */
public class LogParseFunction extends ProcessFunction<RawLog, LogEvent> {
    private static final long serialVersionUID = 1L;

    /** Maximum number of characters of the offending line kept in a dead letter. */
    static final int MAX_RAW_LENGTH = 2048;

    private final LogParser parser;
    private final OutputTag<DeadLetter> deadLetterTag;
    private final boolean deadLetterUnattributed;

    private transient Counter parsed;
    private transient Counter deadLettered;
    private transient Counter unattributed;
    private transient Counter arrivalTimestamps;

    /**
     * @param deadLetterUnattributed whether lines without a known station are dead-lettered. They
     *                               are normally just background chatter, so by default they are
     *                               only counted.
     */
    public LogParseFunction(LogParser parser, OutputTag<DeadLetter> deadLetterTag, boolean deadLetterUnattributed) {
        this.parser = parser;
        this.deadLetterTag = deadLetterTag;
        this.deadLetterUnattributed = deadLetterUnattributed;
    }

    @Override
    public void open(OpenContext openContext) {
        parsed = Metrics.counter(getRuntimeContext(), "parsedRecords");
        deadLettered = Metrics.counter(getRuntimeContext(), "deadLetterRecords");
        unattributed = Metrics.counter(getRuntimeContext(), "unattributedRecords");
        arrivalTimestamps = Metrics.counter(getRuntimeContext(), "arrivalTimestampRecords");
    }

    @Override
    public void processElement(RawLog raw, Context ctx, Collector<LogEvent> out) {
        LogParser.Result result;
        try {
            result = parser.parse(raw);
        } catch (RuntimeException e) {
            // The parser is written not to throw; this is the safety net for what we did not foresee
            deadLetter(ctx, raw, "PARSER_ERROR", e.toString());
            return;
        }

        if (result.isEvent()) {
            parsed.inc();
            if (result.timestampFromArrival) {
                arrivalTimestamps.inc();
            }
            out.collect(result.event);
        } else if (result.rejection == LogParser.Rejection.NO_STATION) {
            unattributed.inc();
            if (deadLetterUnattributed) {
                deadLetter(ctx, raw, result.rejection.name(), null);
            }
        } else {
            deadLetter(ctx, raw, result.rejection.name(), null);
        }
    }

    private void deadLetter(Context ctx, RawLog raw, String reason, String detail) {
        deadLettered.inc();
        String line = raw.getLine();
        String truncated = line == null ? null : line.substring(0, Math.min(line.length(), MAX_RAW_LENGTH));
        ctx.output(deadLetterTag, new DeadLetter(
                reason, detail, truncated, raw.getShardId(), raw.getSequenceNumber(), raw.getArrivalTimestamp()));
    }
}
