package ssdm;

import java.time.LocalDate;

/**
 * Turns one producer line into a {@link TradeEvent}.
 *
 * Line format, five comma-separated fields taken straight from the CSV columns:
 * <pre>ID,SecType,Date,Trading time,Last</pre>
 * for example {@code A169S8.ETR,I,08-11-2021,08:00:00.010,16021.6}.
 *
 * A line counts as a trade only when all of these hold. They are the same
 * rules the pandas answer key uses, so both sides see the same trades:
 * <ul>
 *   <li>Last is a number greater than 0 (zeros are placeholders, not prices)</li>
 *   <li>Trading time is present</li>
 *   <li>Trading time is not 01:00:00.000, the fake stamp the feed uses when the real time is missing</li>
 * </ul>
 *
 * Timestamps: the data is in CET but we read it as if it were UTC. Only window
 * boundaries matter, and 00:00 CET maps to 00:00 here, so the 5-minute windows
 * start at local midnight exactly as the project description asks.
 */
public final class TradeEventParser {

    /** Missing trade time, shown by the feed as midnight UTC in CET. */
    public static final String PLACEHOLDER_TIME = "01:00:00.000";

    /** The producer sends this symbol once per partition when it has nothing more to send. */
    public static final String FLUSH_SYMBOL = "__FLUSH__";

    /**
     * The producer sends this symbol to every partition about once a second during a paced
     * (live) replay. It carries the newest trade time sent so far and no price. A partition
     * whose own symbols are quiet then still knows how far the replay has got, so it does
     * not hold back the windows of the busy partitions.
     */
    public static final String HEARTBEAT_SYMBOL = "__HEARTBEAT__";

    /** Event time of the flush marker: far in the future, so every open window closes. */
    public static final long FLUSH_TS = 4102444800000L; // 2100-01-01

    private static final long MS_PER_DAY = 86_400_000L;

    private TradeEventParser() {}

    /** Returns null for lines that are not in the expected format at all. */
    public static TradeEvent parse(String line) {
        int a = line.indexOf(',');
        int b = a < 0 ? -1 : line.indexOf(',', a + 1);
        int c = b < 0 ? -1 : line.indexOf(',', b + 1);
        int d = c < 0 ? -1 : line.indexOf(',', c + 1);
        if (a <= 0 || d < 0) {
            return null;
        }

        TradeEvent e = new TradeEvent();
        e.symbol = line.substring(0, a);
        e.secType = line.substring(a + 1, b);
        e.ts = TradeEvent.NO_TIMESTAMP;

        if (FLUSH_SYMBOL.equals(e.symbol)) {
            e.ts = FLUSH_TS;
            return e;
        }
        if (HEARTBEAT_SYMBOL.equals(e.symbol)) {
            long hbDay = epochDay(line, b + 1, c);
            long hbMs = millisOfDay(line, c + 1, d);
            if (hbDay != Long.MIN_VALUE && hbMs >= 0) {
                e.ts = hbDay * MS_PER_DAY + hbMs; // moves the watermark, is never a trade
            }
            return e;
        }

        // Cheapest check first: four out of five lines have no price and stop here.
        int lastEnd = line.length();
        while (lastEnd > d + 1 && line.charAt(lastEnd - 1) <= ' ') {
            lastEnd--; // tolerate a trailing \r or space
        }
        if (lastEnd == d + 1) {
            return e;
        }
        double price;
        try {
            price = Double.parseDouble(line.substring(d + 1, lastEnd));
        } catch (NumberFormatException ex) {
            return e;
        }
        if (!(price > 0)) {
            return e;
        }

        if (line.regionMatches(c + 1, PLACEHOLDER_TIME, 0, PLACEHOLDER_TIME.length())
                && d - c - 1 == PLACEHOLDER_TIME.length()) {
            return e;
        }
        long day = epochDay(line, b + 1, c);
        long ms = millisOfDay(line, c + 1, d);
        if (day == Long.MIN_VALUE || ms < 0) {
            return e;
        }

        e.ts = day * MS_PER_DAY + ms;
        e.price = price;
        e.trade = true;
        return e;
    }

    /** dd-MM-yyyy to days since 1970, or Long.MIN_VALUE if it isn't a date. */
    static long epochDay(String s, int from, int to) {
        if (to - from != 10 || s.charAt(from + 2) != '-' || s.charAt(from + 5) != '-') {
            return Long.MIN_VALUE;
        }
        int dd = digits(s, from, 2);
        int mm = digits(s, from + 3, 2);
        int yyyy = digits(s, from + 6, 4);
        if (dd < 1 || dd > 31 || mm < 1 || mm > 12 || yyyy < 0) {
            return Long.MIN_VALUE;
        }
        try {
            return LocalDate.of(yyyy, mm, dd).toEpochDay();
        } catch (RuntimeException ex) {
            return Long.MIN_VALUE; // e.g. 31-02-2021
        }
    }

    /** HH:mm:ss with an optional fraction to ms since midnight, or -1 if it isn't a time. */
    static long millisOfDay(String s, int from, int to) {
        if (to - from < 8 || s.charAt(from + 2) != ':' || s.charAt(from + 5) != ':') {
            return -1;
        }
        int hh = digits(s, from, 2);
        int mi = digits(s, from + 3, 2);
        int ss = digits(s, from + 6, 2);
        if (hh < 0 || hh > 23 || mi < 0 || mi > 59 || ss < 0 || ss > 59) {
            return -1;
        }
        int millis = 0;
        if (to - from > 8) {
            if (s.charAt(from + 8) != '.') {
                return -1;
            }
            // Keep the first three fraction digits; the spec allows four, the files have three.
            int scale = 100;
            for (int i = from + 9; i < to; i++) {
                char ch = s.charAt(i);
                if (ch < '0' || ch > '9') {
                    return -1;
                }
                millis += (ch - '0') * scale;
                scale /= 10;
            }
        }
        return ((hh * 60L + mi) * 60L + ss) * 1000L + millis;
    }

    private static int digits(String s, int from, int count) {
        int value = 0;
        for (int i = from; i < from + count; i++) {
            char ch = s.charAt(i);
            if (ch < '0' || ch > '9') {
                return -1;
            }
            value = value * 10 + (ch - '0');
        }
        return value;
    }
}
