package ssdm;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Keeps only the latest trade of a symbol's window, so a window holds one
 * event in memory no matter how many trades arrive.
 *
 * On equal timestamps the one that arrived later wins. The producer keys by
 * symbol, so a symbol's trades stay in file order inside their partition and
 * "arrived later" means "later row in the file", the answer key's tie-break.
 */
public class LastTrade implements ReduceFunction<TradeEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public TradeEvent reduce(TradeEvent soFar, TradeEvent incoming) {
        return incoming.ts >= soFar.ts ? incoming : soFar;
    }
}
