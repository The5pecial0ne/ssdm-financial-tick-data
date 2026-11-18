package ssdm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Detecting trading trends in the DEBS 2022 tick data.
 *
 * Pipeline: ticks -> sort by event kind -> key by symbol -> 5-minute tumbling event-time
 * windows (last trade = close) -> EMA38/EMA100 per symbol -> crossover advisories.
 *
 * Two ways to run it:
 *   Kafka mode (default): reads the topic the producer fills and writes results,
 *     advisories and late trades back to Kafka.
 *   File mode (--input-file): reads DEBS CSV files directly and writes JSON lines to
 *     --output-file. Handy for checking correctness without Kafka; see run-local.sh.
 *
 * Main options (all optional):
 *   --bootstrap kafka:29092   --input-topic ticks    --group <consumer group>
 *   --bounded                 stop once everything in the topic when the job started is read
 *   --ooo-ms 5000             how far out of order (in event time) trades may arrive
 *   --idle-ms 10000           partitions quiet for this long stop holding back the watermark
 *   --subscriptions all|A,B|@file   symbols whose results get published
 *   --parallelism N           operator parallelism (file mode; on a cluster pass -p to flink run)
 *   --buffer-timeout-ms 100   network buffer flush timeout (lower = less latency, less throughput)
 *   --watermark-interval-ms 200   how often watermarks are emitted
 *   --checkpoint-ms 0         turn on checkpointing (exactly-once state) every N ms
 */
public class TradingJob {

    static final OutputTag<TickEvent> LATE = new OutputTag<>("late-trades") {};
    static final long WINDOW_MS = Duration.ofMinutes(5).toMillis();

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        if (p.has("parallelism")) {
            env.setParallelism(p.getInt("parallelism"));   // mainly for file mode; on a cluster use flink run -p
        }
        // Latency knobs: how long network buffers may wait to fill up, and how often watermarks are emitted.
        // Smaller values cut latency at some cost in throughput (an experiment in run_latency.sh).
        if (p.has("buffer-timeout-ms")) {
            env.setBufferTimeout(p.getLong("buffer-timeout-ms"));
        }
        if (p.has("watermark-interval-ms")) {
            env.getConfig().setAutoWatermarkInterval(p.getLong("watermark-interval-ms"));
        }
        if (p.getLong("checkpoint-ms", 0) > 0) {
            env.enableCheckpointing(p.getLong("checkpoint-ms"));
        }

        Duration outOfOrder = Duration.ofMillis(p.getLong("ooo-ms", 5_000));
        Duration idle = Duration.ofMillis(p.getLong("idle-ms", 10_000));
        Subscriptions subs = Subscriptions.parse(p.get("subscriptions", "all"));

        WatermarkStrategy<TickEvent> watermarks = WatermarkStrategy
                .<TickEvent>forBoundedOutOfOrderness(outOfOrder)
                .withTimestampAssigner((e, ts) -> e.eventTs)
                .withIdleness(idle);

        boolean fileMode = p.has("input-file");
        DataStream<TickEvent> ticks = fileMode
                ? fileSource(env, p, watermarks)
                : kafkaSource(env, p, watermarks);

        // ---- Query 1 and 2 ----
        SingleOutputStreamOperator<TickEvent> trades = ticks
                .process(new EventRouter()).name("Route by event kind").uid("router");

        SingleOutputStreamOperator<WindowClose> closes = trades
                .keyBy(e -> e.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(WINDOW_MS)))
                .sideOutputLateData(LATE)
                .aggregate(new LastTradeAggregate(), new WindowCloseFunction())
                .name("5-min window close").uid("window-close");

        DataStream<EmaResult> results = closes
                .keyBy(w -> w.symbol)
                .process(new EmaProcessor()).name("EMA38/EMA100 + crossover").uid("ema");

        DataStream<TickEvent> late = closes.getSideOutput(LATE);

        System.out.printf("Trading job: %s mode, out-of-order bound %d ms, publishing for %s%n",
                fileMode ? "file" : "Kafka", outOfOrder.toMillis(), subs.describe());

        if (fileMode) {
            runFileMode(env, p, results, late);
        } else {
            publishToKafka(p, results, late, subs);
            JobExecutionResult res = env.execute(p.get("job-name", "Trading trends"));
            try {
                // Only available when the client waited for the job (bounded local runs)
                System.out.printf("Net runtime ms: %d%n", res.getNetRuntime());
            } catch (RuntimeException detached) {
                // submitted with flink run -d: the client doesn't know when the job ends
            }
        }
    }

    // ---------------------------------------------------------------- sources

    private static DataStream<TickEvent> kafkaSource(StreamExecutionEnvironment env, ParameterTool p,
                                                     WatermarkStrategy<TickEvent> watermarks) {
        var builder = KafkaSource.<TickEvent>builder()
                .setBootstrapServers(p.get("bootstrap", "kafka:29092"))
                .setTopics(p.get("input-topic", "ticks"))
                .setGroupId(p.get("group", "trading-job-" + System.currentTimeMillis()))
                .setStartingOffsets(p.get("start", "earliest").equals("latest")
                        ? OffsetsInitializer.latest() : OffsetsInitializer.earliest())
                .setDeserializer(new TickDeserializer())
                .setProperty("fetch.min.bytes", p.get("fetch-min-bytes", "1"));
        if (p.has("bounded")) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        // Watermarks are generated per Kafka partition inside the source, so one slow
        // partition can't let another run ahead and make its trades look late
        return env.fromSource(builder.build(), watermarks, "Kafka ticks").uid("kafka-source");
    }

    private static DataStream<TickEvent> fileSource(StreamExecutionEnvironment env, ParameterTool p,
                                                    WatermarkStrategy<TickEvent> watermarks) {
        Path[] paths = Arrays.stream(p.get("input-file").split(",")).map(Path::new).toArray(Path[]::new);
        FileSource<String> src = FileSource.forRecordStreamFormat(new TextLineInputFormat(), paths).build();
        // One reader so files and lines stay in order; the line number becomes the tie-break seq
        return env.fromSource(src, WatermarkStrategy.noWatermarks(), "CSV files").setParallelism(1)
                .map(new RichMapFunction<String, TickEvent>() {
                    private transient TickParser parser;
                    private long line;

                    @Override
                    public void open(Configuration c) {
                        parser = new TickParser();
                    }

                    @Override
                    public TickEvent map(String s) {
                        TickEvent e = parser.parse(s, System.currentTimeMillis(), line++);
                        if (e == null) {
                            e = new TickEvent();
                            e.kind = TickEvent.MALFORMED;   // header/comment; dropped by the router
                            e.eventTs = Long.MIN_VALUE;
                            e.symbol = "";
                        }
                        return e;
                    }
                }).setParallelism(1).name("Parse CSV")
                .assignTimestampsAndWatermarks(watermarks).setParallelism(1);
    }

    // ---------------------------------------------------------------- sinks

    private static void publishToKafka(ParameterTool p, DataStream<EmaResult> results,
                                       DataStream<TickEvent> late, Subscriptions subs) {
        String bootstrap = p.get("bootstrap", "kafka:29092");

        DataStream<EmaResult> subscribed = results
                .filter((FilterFunction<EmaResult>) r -> subs.contains(r.symbol))
                .name("Subscribed symbols only");

        subscribed.map(new ToJson()).name("to JSON")
                .sinkTo(kafkaSink(bootstrap, p.get("results-topic", "ema-results"))).name("ema-results");

        subscribed.filter(r -> !r.signal.isEmpty()).name("Advisories only")
                .map(new ToJson()).name("to JSON")
                .sinkTo(kafkaSink(bootstrap, p.get("advisories-topic", "advisories"))).name("advisories");

        late.map(TradingJob::lateCsv).name("late to CSV")
                .sinkTo(kafkaSink(bootstrap, p.get("late-topic", "late-trades"))).name("late-trades");
    }

    private static KafkaSink<String> kafkaSink(String bootstrap, String topic) {
        Properties props = new Properties();
        props.setProperty("linger.ms", "5");
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(props)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static void runFileMode(StreamExecutionEnvironment env, ParameterTool p,
                                    DataStream<EmaResult> results, DataStream<TickEvent> late) throws Exception {
        String out = p.get("output-file", "flink-output.jsonl");
        String lateOut = out.replaceAll("\\.jsonl$", "") + ".late.csv";

        DataStream<String> tagged = results.map(new ToJson()).map(s -> "R" + s)
                .union(late.map(TradingJob::lateCsv).map(s -> "L" + s));

        long nResults = 0, nLate = 0;
        try (CloseableIterator<String> it = tagged.executeAndCollect("Trading trends (file mode)");
             BufferedWriter r = Files.newBufferedWriter(Paths.get(out), StandardCharsets.UTF_8);
             BufferedWriter l = Files.newBufferedWriter(Paths.get(lateOut), StandardCharsets.UTF_8)) {
            l.write("symbol,event_ts,price,seq\n");
            while (it.hasNext()) {
                String s = it.next();
                if (s.charAt(0) == 'R') {
                    r.write(s, 1, s.length() - 1);
                    r.newLine();
                    nResults++;
                } else {
                    l.write(s, 1, s.length() - 1);
                    l.newLine();
                    nLate++;
                }
            }
        }
        System.out.printf("Wrote %,d window results to %s and %,d late trades to %s%n", nResults, out, nLate, lateOut);
    }

    static String lateCsv(TickEvent e) {
        return e.symbol + "," + e.eventTs + "," + e.price + "," + e.seq;
    }

    static class ToJson extends RichMapFunction<EmaResult, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration c) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(EmaResult r) throws Exception {
            return mapper.writeValueAsString(r);
        }
    }
}
