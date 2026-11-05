import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.core.JsonProcessingException;

public class TradingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:29092")
                        .setTopics("trading-events")
                        .setGroupId("trading-job")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        DataStream<String> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                );
				ObjectMapper mapper = new ObjectMapper();

DataStream<TradeEvent> events =
		stream.map(json ->{
			return  mapper.readValue(json, TradeEvent.class);
		});

		events = events.map(event -> {

    String dateTime =
            event.tradingDate + " " + event.tradingTime;

    DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern(
                    "dd-MM-yyyy HH:mm:ss.SSS"
            );

    LocalDateTime ts =
            LocalDateTime.parse(dateTime, formatter);

    event.eventTimestamp =
            ts.toInstant(ZoneOffset.UTC)
              .toEpochMilli();

    return event;
});

DataStream<TradeEvent> withTimestamps =
        events.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5)
                        )
                        .withTimestampAssigner(
                                (event, ts) -> event.eventTimestamp
                        )
        );

DataStream<WindowClose> windowCloses =
        withTimestamps
    .keyBy(event -> event.symbol)
	.window(TumblingEventTimeWindows.of(Time.minutes(5)))
	.reduce((event1, event2) -> 
	                 event1.eventTimestamp > event2.eventTimestamp ? event1 : event2)
	.map(event -> new WindowClose(event.symbol, event.SecType, event.eventTimestamp, event.price));

	DataStream<EmaResult> emaResults = windowCloses
		.keyBy(windowClose -> windowClose.ID)
		.process(new EmaProcessor());

		ObjectMapper outputMapper = new ObjectMapper();

/*DataStream<String> emaJson =
    emaResults.map(result ->
        outputMapper.writeValueAsString(result)
    );*/
	DataStream<String> emaJson =
    emaResults.map(result ->
        "{"
        + "\"ID\":\"" + result.ID + "\","
        + "\"SecType\":\"" + result.SecType + "\","
        + "\"windowStart\":" + result.windowStart + ","
        + "\"close\":" + result.close + ","
        + "\"ema38\":" + result.ema38 + ","
        + "\"ema100\":" + result.ema100 + ","
        + "\"firstWindow\":" + result.firstWindow + ","
        + "\"signal\":\"" + result.signal + "\""
        + "}"
    );
	KafkaSink<String> emaSink =
    KafkaSink.<String>builder()
        .setBootstrapServers("kafka:29092")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic("ema-results")
                .setValueSerializationSchema(
                    new SimpleStringSchema()
                )
                .build()
        )
        //.setDeliveryGuarantee(
        //    DeliveryGuarantee.AT_LEAST_ONCE
        //)
        .build();
	emaJson.sinkTo(emaSink);

		emaResults.print();
		emaJson.print();
        //stream.print();

        env.execute("Trading Job");
    }
}
