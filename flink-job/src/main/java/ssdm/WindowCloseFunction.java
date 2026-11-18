package ssdm;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Adds the window's boundaries to the aggregated last trade. */
public class WindowCloseFunction
        extends ProcessWindowFunction<LastTradeAggregate.Acc, WindowClose, String, TimeWindow> {

    @Override
    public void process(String symbol, Context ctx, Iterable<LastTradeAggregate.Acc> accs,
                        Collector<WindowClose> out) {
        LastTradeAggregate.Acc acc = accs.iterator().next();
        WindowClose w = new WindowClose();
        w.symbol = symbol;
        w.secType = acc.secType;
        w.windowStart = ctx.window().getStart();
        w.windowEnd = ctx.window().getEnd();
        w.close = acc.lastPrice;
        w.trades = acc.trades;
        w.lastTradeTs = acc.lastTs;
        w.maxIngestTs = acc.maxIngestTs;
        w.firedTs = System.currentTimeMillis();
        out.collect(w);
    }
}
