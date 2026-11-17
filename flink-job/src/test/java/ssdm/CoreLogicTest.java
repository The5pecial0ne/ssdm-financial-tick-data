package ssdm;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CoreLogicTest {

    private static final String RAW_TRADE =
            "A169S8.ETR,I,08-11-2021,08:00:00.000,,,,,,,,,,,,,,,,,,16022.6,,08:00:00.010,,,,,,,,,,,,,,,,";
    private static final String RAW_QUOTE =
            "ABC.FR,E,08-11-2021,08:00:00.000,10.5,100,10.4,200,,,,,,,,,,,,,,,,08:00:00.000,,,,,,,,,,,,,,,,";

    @Test
    void parsesRawTrade() {
        TickEvent e = new TickParser().parse(RAW_TRADE, 1L, 7L);
        assertEquals(TickEvent.TRADE, e.kind);
        assertEquals("A169S8.ETR", e.symbol);
        assertEquals("I", e.secType);
        assertEquals(16022.6, e.price);
        assertEquals(7L, e.seq);
        // 2021-11-08 is day 18939 since the epoch; 08:00:00.010 is 28,800,010 ms after midnight
        assertEquals(18939L * 86_400_000L + 28_800_010L, e.eventTs);
    }

    @Test
    void quotesAreNotTrades() {
        assertEquals(TickEvent.NON_TRADE, new TickParser().parse(RAW_QUOTE, 0, 0).kind);
    }

    @Test
    void parsesCompactFormatAndPlaceholder() {
        TickParser p = new TickParser();
        TickEvent e = p.parse("X.NL,E,09-11-2021,09:00:01.000,09:00:00.5,12.25,42", 5L, 0L);
        assertEquals(TickEvent.TRADE, e.kind);
        assertEquals(42L, e.seq);
        assertEquals(500L, e.eventTs % 1000);
        assertEquals(TickEvent.PLACEHOLDER,
                p.parse("X.NL,E,09-11-2021,09:00:01.000,01:00:00.000,12.25,43", 5L, 0L).kind);
        assertEquals(TickEvent.MALFORMED, p.parse("garbage", 0, 0).kind);
        assertNull(p.parse("# licence line", 0, 0));
        assertEquals(TickEvent.END_OF_STREAM, p.parse("#EOS,123", 0, 0).kind);
    }

    @Test
    void emaMatchesTheReferenceExample() {
        // First A169S8.ETR window from analysis/test-slice-expected.jsonl
        double e38 = Ema.next(16027.61, 0.0, Ema.ALPHA_38);
        double e100 = Ema.next(16027.61, 0.0, Ema.ALPHA_100);
        assertEquals(821.9287179487179, e38, 1e-9);
        assertEquals(317.3784158415842, e100, 1e-9);
        assertEquals("buy", Ema.signal(0, 0, e38, e100));
    }

    @Test
    void crossoverRules() {
        assertEquals("buy", Ema.signal(1, 2, 3, 2));    // crosses from below
        assertEquals("buy", Ema.signal(2, 2, 3, 2));    // equality counts as "below" before
        assertEquals("sell", Ema.signal(3, 2, 1, 2));   // crosses from above
        assertEquals("sell", Ema.signal(2, 2, 1, 2));
        assertEquals("", Ema.signal(3, 2, 4, 2));       // stays above
        assertEquals("", Ema.signal(1, 2, 1, 2));       // stays below
    }

    @Test
    void lastTradeUsesEventTimeThenFilePosition() {
        LastTradeAggregate agg = new LastTradeAggregate();
        LastTradeAggregate.Acc acc = agg.createAccumulator();
        acc = agg.add(trade(100, 1, 1.0), acc);
        acc = agg.add(trade(300, 2, 3.0), acc);
        acc = agg.add(trade(200, 3, 2.0), acc);   // arrives late but is older: not the close
        acc = agg.add(trade(300, 4, 4.0), acc);   // same time, later in the file: wins
        assertEquals(4.0, acc.lastPrice);
        assertEquals(4, acc.trades);
    }

    private static TickEvent trade(long ts, long seq, double price) {
        TickEvent e = new TickEvent();
        e.eventTs = ts;
        e.seq = seq;
        e.price = price;
        e.kind = TickEvent.TRADE;
        return e;
    }
}
