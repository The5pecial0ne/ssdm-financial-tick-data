package ssdm;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Query 1 and Query 2 for one symbol.
 *
 * Keyed state holds the symbol's two EMAs from its latest window, nothing more, so
 * state size grows with the number of symbols, not with the number of events.
 * Windows of one symbol arrive here in order, because they fire as the watermark moves forward.
 * A window with no trades never fires, so the EMA simply carries over, like in the answer key.
 */
public class EmaProcessor extends KeyedProcessFunction<String, WindowClose, EmaResult> {

    private transient ValueState<double[]> emas;   // [EMA38, EMA100]

    @Override
    public void open(Configuration parameters) {
        emas = getRuntimeContext().getState(new ValueStateDescriptor<>("emas", double[].class));
    }

    @Override
    public void processElement(WindowClose w, Context ctx, Collector<EmaResult> out) throws Exception {
        double[] prev = emas.value();
        boolean first = prev == null;
        double prev38 = first ? 0.0 : prev[0];     // EMA of window 0 is defined as 0
        double prev100 = first ? 0.0 : prev[1];

        double ema38 = Ema.next(w.close, prev38, Ema.ALPHA_38);
        double ema100 = Ema.next(w.close, prev100, Ema.ALPHA_100);
        emas.update(new double[] {ema38, ema100});

        EmaResult r = new EmaResult();
        r.symbol = w.symbol;
        r.secType = w.secType;
        r.windowStart = w.windowStart;
        r.windowEnd = w.windowEnd;
        r.close = w.close;
        r.trades = w.trades;
        r.ema38 = ema38;
        r.ema100 = ema100;
        r.firstWindow = first;
        r.signal = Ema.signal(prev38, prev100, ema38, ema100);
        r.maxIngestTs = w.maxIngestTs;
        r.firedTs = w.firedTs;
        r.emitTs = System.currentTimeMillis();
        out.collect(r);
    }
}
