package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.alert.AlertFunction;
import cloud.hermoso.flink_logs_processor.config.StationProfiles;
import cloud.hermoso.flink_logs_processor.features.FeatureVectorFunction;
import cloud.hermoso.flink_logs_processor.features.LogEventAggregator;
import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.FeatureVector;
import cloud.hermoso.flink_logs_processor.model.LogEvent;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import cloud.hermoso.flink_logs_processor.operator.RcfModelsConfig;
import cloud.hermoso.flink_logs_processor.operator.RcfScoringFunction;
import cloud.hermoso.flink_logs_processor.parser.LogParseFunction;
import cloud.hermoso.flink_logs_processor.parser.LogParser;
import cloud.hermoso.flink_logs_processor.rules.StationRulesFunction;
import cloud.hermoso.flink_logs_processor.source.RawLogTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * The processing topology, independent of where the data comes from and goes to, so that it can
 * be run unchanged in tests:
 * <pre>
 * raw lines (event time + watermarks per shard) -> parse -> event-time windows per station -> feature vector -> RCF score -> alerts
 *                |                |                                           |
 *                +----------------+------------- dead letters ----------------+
 * </pre>
 * Operator uids are part of the savepoint contract: changing one drops that operator's state.
 */
public final class LogAnomalyPipeline {

    private static final OutputTag<DeadLetter> DEAD_LETTERS = new OutputTag<>("dead-letters") {
    };
    private static final OutputTag<LogEvent> LATE_EVENTS = new OutputTag<>("late-events") {
    };

    private LogAnomalyPipeline() {
    }

    /** Tunables of the topology. See docs/CONFIGURATION.md for the property reference. */
    public static final class Config implements Serializable {
        private static final long serialVersionUID = 1L;

        public final StationProfiles stationProfiles;
        public final RcfModelsConfig modelsConfig;
        public int maxLineLength = 16_384;
        public long maxFutureSkewMillis = 300_000;
        public boolean deadLetterUnattributed = false;
        public long windowSizeMillis = 10_000;
        public long maxOutOfOrdernessMillis = 30_000;
        public long idleTimeoutMillis = 60_000;
        public int maxCachedModels = 1_000;
        public long modelStateTtlMillis = 0;
        public double alertMinGrade = 0.5;
        public long alertCooldownMillis = 300_000;
        public int alertWarmupWindows = 60;
        public long silenceAfterMillis = 60_000;
        public int stuckWindows = 12;

        public Config(StationProfiles stationProfiles, RcfModelsConfig modelsConfig) {
            this.stationProfiles = stationProfiles;
            this.modelsConfig = modelsConfig;
        }

        public static Config fromApplicationProperties(Map<String, Properties> applicationProperties) {
            Config config = new Config(
                    StationProfiles.fromApplicationProperties(applicationProperties),
                    RcfModelsConfig.fromApplicationProperties(applicationProperties));

            Properties parser = ApplicationConfigUtils.optionalGroup(applicationProperties, "Parser");
            config.maxLineLength = ApplicationConfigUtils.parseInt(parser, "max.line.length", config.maxLineLength);
            config.maxFutureSkewMillis = ApplicationConfigUtils.parseLong(parser, "max.future.skew.ms", config.maxFutureSkewMillis);
            config.deadLetterUnattributed = ApplicationConfigUtils.parseBoolean(parser, "dead.letter.unattributed", config.deadLetterUnattributed);

            Properties features = ApplicationConfigUtils.optionalGroup(applicationProperties, "Features");
            config.windowSizeMillis = ApplicationConfigUtils.parseLong(features, "window.size.ms", config.windowSizeMillis);
            config.maxOutOfOrdernessMillis = ApplicationConfigUtils.parseLong(features, "max.out.of.orderness.ms", config.maxOutOfOrdernessMillis);
            config.idleTimeoutMillis = ApplicationConfigUtils.parseLong(features, "idle.timeout.ms", config.idleTimeoutMillis);

            Properties rcf = ApplicationConfigUtils.optionalGroup(applicationProperties, "RcfOperator");
            config.maxCachedModels = ApplicationConfigUtils.parseInt(rcf, "model.cache.max.size", config.maxCachedModels);
            config.modelStateTtlMillis = ApplicationConfigUtils.parseLong(rcf, "model.state.ttl.hours", 0) * 3_600_000L;

            Properties alerting = ApplicationConfigUtils.optionalGroup(applicationProperties, "Alerting");
            config.alertMinGrade = ApplicationConfigUtils.parseDouble(alerting, "min.grade", config.alertMinGrade);
            config.alertCooldownMillis = ApplicationConfigUtils.parseLong(alerting, "cooldown.ms", config.alertCooldownMillis);
            config.alertWarmupWindows = ApplicationConfigUtils.parseInt(alerting, "warmup.windows", config.alertWarmupWindows);

            Properties rules = ApplicationConfigUtils.optionalGroup(applicationProperties, "Rules");
            config.silenceAfterMillis = ApplicationConfigUtils.parseLong(rules, "silence.after.ms", config.silenceAfterMillis);
            config.stuckWindows = ApplicationConfigUtils.parseInt(rules, "stuck.windows", config.stuckWindows);

            config.validate();
            return config;
        }

        void validate() {
            Preconditions.checkArgument(maxLineLength > 0, "Parser max.line.length must be > 0");
            Preconditions.checkArgument(maxFutureSkewMillis >= 0, "Parser max.future.skew.ms must be >= 0");
            Preconditions.checkArgument(windowSizeMillis > 0, "Features window.size.ms must be > 0");
            Preconditions.checkArgument(maxOutOfOrdernessMillis >= 0, "Features max.out.of.orderness.ms must be >= 0");
            Preconditions.checkArgument(idleTimeoutMillis > 0, "Features idle.timeout.ms must be > 0");
            Preconditions.checkArgument(maxCachedModels >= 1, "RcfOperator model.cache.max.size must be >= 1");
            Preconditions.checkArgument(modelStateTtlMillis >= 0, "RcfOperator model.state.ttl.hours must be >= 0");
            Preconditions.checkArgument(alertMinGrade >= 0 && alertMinGrade <= 1, "Alerting min.grade must be in [0, 1]");
            Preconditions.checkArgument(alertCooldownMillis >= 0, "Alerting cooldown.ms must be >= 0");
            Preconditions.checkArgument(alertWarmupWindows >= 0, "Alerting warmup.windows must be >= 0");
            Preconditions.checkArgument(silenceAfterMillis >= 0, "Rules silence.after.ms must be >= 0");
            Preconditions.checkArgument(stuckWindows >= 0, "Rules stuck.windows must be >= 0");
        }
    }

    /** The three outputs of the topology. */
    public static final class Outputs {
        /** Every scored window: for dashboards and history. */
        public final DataStream<ScoredRecord> scores;
        /** Anomalies worth notifying someone about. */
        public final DataStream<Alert> alerts;
        /** Everything that could not be processed. */
        public final DataStream<DeadLetter> deadLetters;

        Outputs(DataStream<ScoredRecord> scores, DataStream<Alert> alerts, DataStream<DeadLetter> deadLetters) {
            this.scores = scores;
            this.alerts = alerts;
            this.deadLetters = deadLetters;
        }
    }

    /**
     * Watermark strategy for the raw log source. It must be applied in the source itself
     * ({@code env.fromSource(source, watermarkStrategy(config), ...)}) so that watermarks are
     * tracked per shard. Idleness keeps windows closing when some shards stop receiving data.
     */
    public static WatermarkStrategy<RawLog> watermarkStrategy(Config config) {
        return WatermarkStrategy
                .<RawLog>forBoundedOutOfOrderness(Duration.ofMillis(config.maxOutOfOrdernessMillis))
                .withTimestampAssigner(new RawLogTimestampAssigner(config.maxFutureSkewMillis))
                .withIdleness(Duration.ofMillis(config.idleTimeoutMillis));
    }

    /**
     * @param rawLogs raw lines with event-time timestamps and watermarks, see {@link #watermarkStrategy}
     */
    public static Outputs build(DataStream<RawLog> rawLogs, Config config) {
        LogParser parser = new LogParser(config.stationProfiles, config.maxLineLength, config.maxFutureSkewMillis);

        SingleOutputStreamOperator<LogEvent> events = rawLogs
                .process(new LogParseFunction(parser, DEAD_LETTERS, config.deadLetterUnattributed))
                .uid("parse").name("Parse log lines");

        SingleOutputStreamOperator<FeatureVector> features = events
                .keyBy(LogEvent::getStationId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(config.windowSizeMillis)))
                .sideOutputLateData(LATE_EVENTS)
                .aggregate(new LogEventAggregator(), new FeatureVectorFunction(config.stationProfiles))
                .uid("features").name("Window features");

        SingleOutputStreamOperator<ScoredRecord> scores = features
                .keyBy(FeatureVector::getStationId)
                .process(new RcfScoringFunction(config.modelsConfig, config.maxCachedModels, config.modelStateTtlMillis, DEAD_LETTERS))
                .uid("rcf").name("RCF scoring");

        DataStream<Alert> anomalyAlerts = scores
                .keyBy(ScoredRecord::getStationId)
                .process(new AlertFunction(config.alertMinGrade, config.alertCooldownMillis, config.alertWarmupWindows))
                .uid("alerts").name("Alert de-duplication");

        // Failure modes invisible to the model: silent stations, stuck sensors
        DataStream<Alert> ruleAlerts = features
                .keyBy(FeatureVector::getStationId)
                .process(new StationRulesFunction(config.silenceAfterMillis, config.stuckWindows))
                .uid("rules").name("Station rules");

        DataStream<Alert> alerts = anomalyAlerts.union(ruleAlerts);

        DataStream<DeadLetter> lateEvents = features.getSideOutput(LATE_EVENTS)
                .map(event -> new DeadLetter("LATE_EVENT", "arrived after its window closed", event.toString(), null, null, event.getTimestamp()))
                .returns(DeadLetter.class)
                .uid("late-events").name("Late events");

        DataStream<DeadLetter> deadLetters = events.getSideOutput(DEAD_LETTERS)
                .union(scores.getSideOutput(DEAD_LETTERS), lateEvents);

        return new Outputs(scores, alerts, deadLetters);
    }
}
