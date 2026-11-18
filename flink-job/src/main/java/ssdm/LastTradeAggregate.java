package ssdm;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Keeps only what a 5-minute window needs: the last trade in it (its close price),
 * how many trades it had, and when its newest event entered the system.
 *
 * "Last" means latest event time, and for equal timestamps the one further down the
 * source file. That's exactly the tie-break the pandas answer key uses (sort by ts, seq).
 * Arrival order doesn't matter, so out-of-order trades within the bound land correctly.
 */
public class LastTradeAggregate
        implements AggregateFunction<TickEvent, LastTradeAggregate.Acc, LastTradeAggregate.Acc> {

    public static class Acc {
        public String secType;
        public long lastTs = Long.MIN_VALUE;
        public long lastSeq = Long.MIN_VALUE;
        public double lastPrice;
        public long trades;
        public long maxIngestTs = Long.MIN_VALUE;

        public Acc() {}
    }

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(TickEvent e, Acc acc) {
        if (e.eventTs > acc.lastTs || (e.eventTs == acc.lastTs && e.seq >= acc.lastSeq)) {
            acc.lastTs = e.eventTs;
            acc.lastSeq = e.seq;
            acc.lastPrice = e.price;
            acc.secType = e.secType;
        }
        acc.trades++;
        acc.maxIngestTs = Math.max(acc.maxIngestTs, e.ingestTs);
        return acc;
    }

    @Override
    public Acc getResult(Acc acc) {
        return acc;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        Acc later = (b.lastTs > a.lastTs || (b.lastTs == a.lastTs && b.lastSeq >= a.lastSeq)) ? b : a;
        Acc out = new Acc();
        out.secType = later.secType;
        out.lastTs = later.lastTs;
        out.lastSeq = later.lastSeq;
        out.lastPrice = later.lastPrice;
        out.trades = a.trades + b.trades;
        out.maxIngestTs = Math.max(a.maxIngestTs, b.maxIngestTs);
        return out;
    }
}
