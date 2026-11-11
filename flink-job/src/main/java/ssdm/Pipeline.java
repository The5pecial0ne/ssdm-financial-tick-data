package ssdm;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

/**
 * The part of the job that does not care where events come from.
 *
 * {@link TradingJob} feeds it from Kafka and {@link LocalFileJob} from a text
 * file, so the exact same logic can be checked without a cluster.
 */
public final class Pipeline {

    /** Trades that showed up after their window had already closed. */
    public static final OutputTag<TradeEvent> LATE = new OutputTag<TradeEvent>("late-trades") {};

    private Pipeline() {}

    /**
     * Event time comes from the trade itself. The watermark trails the newest
     * trade seen by {@code maxOutOfOrderMs}: a window closes once the watermark
     * passes its end, so that is how long we wait for stragglers.
     * Lines that are not trades carry no time and leave the watermark alone.
     */
    public static WatermarkStrategy<TradeEvent> watermarks(long maxOutOfOrderMs) {
        return WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofMillis(maxOutOfOrderMs))
                .withTimestampAssigner((event, previous) -> event.ts);
    }

    /** events (already timestamped) -> one result per symbol per 5-minute window that had a trade. */
    public static SingleOutputStreamOperator<EmaResult> build(DataStream<TradeEvent> events) {
        return events
                // Drop bids, asks and housekeeping before the shuffle by symbol:
                // about 4 in 5 events never need to cross the network.
                .filter(event -> event.trade)
                .name("trades only")
                .keyBy(event -> event.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .sideOutputLateData(LATE)
                .reduce(new LastTrade(), new EmaWindowFunction())
                .name("5-min close -> EMA38/EMA100 -> crossover");
    }

    static String lateJson(TradeEvent late, String runId) {
        return "{\"symbol\":\"" + late.symbol + "\",\"ts\":" + late.ts
                + ",\"price\":" + late.price + ",\"run_id\":\"" + runId + "\"}";
    }
}
