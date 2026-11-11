package ssdm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Kafka in, Kafka out.
 *
 * <pre>
 *   trading-events  ->  [ parse | 5-min windows per symbol | EMA + crossover ]  ->  ema-results   (Query 1)
 *                                                                               ->  advisories    (Query 2)
 *                                                                               ->  late-trades   (dropped as too late)
 * </pre>
 *
 * Settings (all optional, "--name value" or the matching environment variable):
 * <ul>
 *   <li>--bootstrap kafka:29092</li>
 *   <li>--input-topic trading-events</li>
 *   <li>--bounded false: true reads what is in the topic right now and then stops.
 *       Used for benchmarks, where "how long did the job take" is the measurement.</li>
 *   <li>--max-out-of-order-ms 5000: how far the watermark trails the newest trade</li>
 *   <li>--subscribe ALL: or a comma-separated list of symbols to publish</li>
 *   <li>--run-id live: copied into every output line so runs can be told apart</li>
 *   <li>--checkpoint-ms 0: turn on checkpoints at this interval (0 = off)</li>
 * </ul>
 */
public class TradingJob {

    public static void main(String[] rawArgs) throws Exception {
        Args args = Args.parse(rawArgs);
        String bootstrap = args.get("bootstrap", "kafka:29092");
        String inputTopic = args.get("input-topic", "trading-events");
        String runId = args.get("run-id", "live");
        boolean bounded = args.getBoolean("bounded", false);
        long maxOutOfOrderMs = args.getLong("max-out-of-order-ms", 5000);
        long checkpointMs = args.getLong("checkpoint-ms", 0);
        Subscriptions subscriptions = new Subscriptions(args.get("subscribe", "ALL"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (checkpointMs > 0) {
            env.enableCheckpointing(checkpointMs);
        }

        KafkaSourceBuilder<TradeEvent> source = KafkaSource.<TradeEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId("trading-job-" + runId)
                // Always from the start of the topic, so it doesn't matter
                // whether the producer or the job is started first.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TradeEventSchema());
        if (bounded) {
            source.setBounded(OffsetsInitializer.latest());
        }

        // Giving the watermark strategy to the source (not to a later step)
        // makes Flink track one watermark per Kafka partition.
        DataStream<TradeEvent> events =
                env.fromSource(source.build(), Pipeline.watermarks(maxOutOfOrderMs), "kafka: " + inputTopic);

        SingleOutputStreamOperator<EmaResult> results = Pipeline.build(events);

        DataStream<EmaResult> subscribed = results.filter(subscriptions).name("subscribed: " + subscriptions);

        subscribed
                .map(result -> result.toJson(runId))
                .name("EMA to JSON")
                .sinkTo(sink(bootstrap, args.get("ema-topic", "ema-results")))
                .name("kafka: ema-results");

        subscribed
                .filter(EmaResult::isAdvisory)
                .name("crossovers only")
                .map(result -> result.toJson(runId))
                .name("advisory to JSON")
                .sinkTo(sink(bootstrap, args.get("advisory-topic", "advisories")))
                .name("kafka: advisories");

        results.getSideOutput(Pipeline.LATE)
                .map(late -> Pipeline.lateJson(late, runId))
                .name("late trade to JSON")
                .sinkTo(sink(bootstrap, args.get("late-topic", "late-trades")))
                .name("kafka: late-trades");

        env.execute("Trading trends (" + runId + ")");
    }

    private static KafkaSink<String> sink(String bootstrap, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();
    }
}
