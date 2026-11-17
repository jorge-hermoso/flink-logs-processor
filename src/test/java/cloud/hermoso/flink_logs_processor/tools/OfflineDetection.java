package cloud.hermoso.flink_logs_processor.tools;

import cloud.hermoso.flink_logs_processor.LogAnomalyPipeline;
import cloud.hermoso.flink_logs_processor.model.Alert;
import cloud.hermoso.flink_logs_processor.model.DeadLetter;
import cloud.hermoso.flink_logs_processor.model.RawLog;
import cloud.hermoso.flink_logs_processor.model.ScoredRecord;
import cloud.hermoso.flink_logs_processor.parser.LogParser;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs the production topology over a log file, without Kinesis, and writes the alerts as JSON
 * lines. Together with {@code python -m loggen.evaluate} it measures detection quality in
 * seconds, for tuning. See docs/DETECTION_QUALITY.md.
 * <pre>
 * OfflineDetection &lt;log file&gt; &lt;alerts output&gt; [properties JSON file] [Group.property=value ...]
 * </pre>
 * Properties default to the dev configuration on the classpath. Extra arguments override single
 * properties, e.g. {@code Alerting.min.grade=0.5 ModelParameters-DEFAULT.anomaly.rate=0.002}.
 */
public final class OfflineDetection {

    private OfflineDetection() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: OfflineDetection <log file> <alerts output> [properties.json] [Group.property=value ...]");
            System.exit(2);
        }
        Path logFile = Path.of(args[0]);
        Path alertsFile = Path.of(args[1]);
        int next = 2;
        String propertiesPath;
        if (args.length > 2 && args[2].endsWith(".json")) {
            propertiesPath = args[2];
            next = 3;
        } else {
            propertiesPath = OfflineDetection.class.getClassLoader().getResource("flink-application-properties-dev.json").getPath();
        }
        Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties(propertiesPath);
        for (int i = next; i < args.length; i++) {
            // Group ids contain dashes, property names contain dots: split at the first dot
            int dot = args[i].indexOf('.');
            int equals = args[i].indexOf('=');
            String group = args[i].substring(0, dot);
            properties.computeIfAbsent(group, g -> new Properties())
                    .setProperty(args[i].substring(dot + 1, equals), args[i].substring(equals + 1));
        }

        LogAnomalyPipeline.Config config = LogAnomalyPipeline.Config.fromApplicationProperties(properties);
        // No Kinesis here. Emulate its arrival time: the file is in delivery order, so a line
        // "arrives" at the latest time seen so far. Lines without their own time get that.
        config.maxFutureSkewMillis = Long.MAX_VALUE / 4;
        long arrival = 0;

        List<RawLog> lines = new ArrayList<>();
        for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                long guess = arrival > 0 ? arrival : System.currentTimeMillis();
                long time = LogParser.eventTime(line, guess, config.maxFutureSkewMillis);
                if (time != LogParser.NO_TIMESTAMP && time != guess) {
                    arrival = Math.max(arrival, time);
                }
                lines.add(new RawLog(line.strip(), arrival > 0 ? arrival : guess, "file", String.valueOf(lines.size())));
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        LogAnomalyPipeline.Outputs outputs = LogAnomalyPipeline.build(
                env.fromData(lines, TypeInformation.of(RawLog.class))
                        .assignTimestampsAndWatermarks(LogAnomalyPipeline.watermarkStrategy(config)),
                config);
        CloseableIterator<Alert> alerts = outputs.alerts.collectAsync();
        CloseableIterator<ScoredRecord> scores = outputs.scores.collectAsync();
        CloseableIterator<DeadLetter> deadLetters = outputs.deadLetters.collectAsync();
        // Collect sinks have bounded buffers: drain all outputs while the job runs, or it blocks
        env.executeAsync("offline-detection");

        ObjectMapper json = new ObjectMapper();
        AtomicLong alertCount = new AtomicLong();
        AtomicLong scored = new AtomicLong();
        AtomicLong graded = new AtomicLong();
        AtomicLong dead = new AtomicLong();
        Map<String, Long> deadReasons = new java.util.concurrent.ConcurrentSkipListMap<>();
        ExecutorService drainers = Executors.newFixedThreadPool(3);
        Future<?> alertsDone = drainers.submit(() -> {
            try (BufferedWriter out = Files.newBufferedWriter(alertsFile, StandardCharsets.UTF_8); alerts) {
                while (alerts.hasNext()) {
                    out.write(json.writeValueAsString(alerts.next()));
                    out.newLine();
                    alertCount.incrementAndGet();
                }
            }
            return null;
        });
        Future<?> scoresDone = drainers.submit(() -> {
            try (scores) {
                while (scores.hasNext()) {
                    scored.incrementAndGet();
                    if (scores.next().getGrade() > 0) {
                        graded.incrementAndGet();
                    }
                }
            }
            return null;
        });
        Future<?> deadDone = drainers.submit(() -> {
            try (deadLetters) {
                while (deadLetters.hasNext()) {
                    deadReasons.merge(deadLetters.next().getReason(), 1L, Long::sum);
                    dead.incrementAndGet();
                }
            }
            return null;
        });
        alertsDone.get();
        scoresDone.get();
        deadDone.get();
        drainers.shutdown();
        System.out.printf("lines=%d scoredWindows=%d gradedWindows=%d alerts=%d deadLetters=%d %s%n",
                lines.size(), scored.get(), graded.get(), alertCount.get(), dead.get(), deadReasons);
    }
}
