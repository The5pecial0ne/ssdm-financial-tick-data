package ssdm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Sorts incoming events by kind. Trades go on to the windows; everything else is counted
 * and dropped. The counters show up in the Flink UI and REST API (metric names below),
 * which is how the experiments report how many quotes, placeholders and bad lines were seen.
 */
public class EventRouter extends ProcessFunction<TickEvent, TickEvent> {

    private transient Counter trades, nonTrades, placeholders, malformed, endMarkers;

    @Override
    public void open(Configuration parameters) {
        trades = getRuntimeContext().getMetricGroup().counter("ticksTrade");
        nonTrades = getRuntimeContext().getMetricGroup().counter("ticksNonTrade");
        placeholders = getRuntimeContext().getMetricGroup().counter("ticksPlaceholder");
        malformed = getRuntimeContext().getMetricGroup().counter("ticksMalformed");
        endMarkers = getRuntimeContext().getMetricGroup().counter("ticksEndOfStream");
    }

    @Override
    public void processElement(TickEvent e, Context ctx, Collector<TickEvent> out) {
        switch (e.kind) {
            case TickEvent.TRADE:
                trades.inc();
                out.collect(e);
                break;
            case TickEvent.NON_TRADE:
                nonTrades.inc();
                break;
            case TickEvent.PLACEHOLDER:
                placeholders.inc();
                break;
            case TickEvent.END_OF_STREAM:
                // Its only job was to push the watermark forward, which already happened at the source
                endMarkers.inc();
                break;
            default:
                malformed.inc();
        }
    }
}
