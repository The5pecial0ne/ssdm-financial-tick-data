package ssdm;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * Turns one line of text into a {@link TickEvent}.
 *
 * Two line formats are understood:
 * <ul>
 *   <li>the raw DEBS 2022 CSV row (39 columns), so the job can read the original day files directly;</li>
 *   <li>the compact row our producer sends to Kafka: {@code ID,SecType,Date,Time,TradingTime,Last,seq}.</li>
 * </ul>
 * The producer marks the start and end of a replay with {@code #SOS,<epoch ms>} and {@code #EOS,<epoch ms>}.
 *
 * Timestamps are parsed by hand rather than with DateTimeFormatter, because this runs
 * once per event and the formatter is surprisingly slow at hundreds of thousands of events a second.
 */
public class TickParser implements Serializable {

    public static final String PLACEHOLDER_TIME = "01:00:00.000";
    private static final int RAW_COLUMNS = 27;

    // The date only changes once a day, so remember the last one instead of re-parsing it every time
    private String lastDate = null;
    private long lastDateMs = 0L;

    /** Parses a line. Returns null for header and comment lines. */
    public TickEvent parse(String line, long ingestTs, long fallbackSeq) {
        if (line == null || line.isEmpty() || line.startsWith("ID,")) {
            return null;
        }
        TickEvent e = new TickEvent();
        e.ingestTs = ingestTs;

        if (line.startsWith("#EOS") || line.startsWith("#SOS")) {
            boolean end = line.startsWith("#EOS");
            e.kind = end ? TickEvent.END_OF_STREAM : TickEvent.START_OF_STREAM;
            e.symbol = end ? "#EOS" : "#SOS";
            int comma = line.indexOf(',');
            e.eventTs = comma > 0 ? Long.parseLong(line.substring(comma + 1).trim()) : Long.MAX_VALUE / 2;
            return e;
        }
        if (line.startsWith("#")) {
            return null;
        }

        String[] f = line.split(",", -1);
        String date, tradingTime, last;
        try {
            if (f.length >= RAW_COLUMNS) {
                e.symbol = f[0];
                e.secType = f[1];
                date = f[2];
                last = f[21];
                tradingTime = f[23];
                e.seq = fallbackSeq;
            } else if (f.length >= 7) {
                e.symbol = f[0];
                e.secType = f[1];
                date = f[2];
                tradingTime = f[4];
                last = f[5];
                e.seq = f[6].isEmpty() ? fallbackSeq : Long.parseLong(f[6]);
            } else {
                return malformed(e);
            }

            e.price = last.isEmpty() ? 0.0 : Double.parseDouble(last);
            if (e.price <= 0.0) {
                // A quote or housekeeping event. It never advances event time, so watermarks
                // are driven by trades only, the same events the answer key looks at.
                e.kind = TickEvent.NON_TRADE;
                e.eventTs = Long.MIN_VALUE;
                return e;
            }
            if (tradingTime.isEmpty() || date.isEmpty()) {
                return malformed(e);
            }
            if (PLACEHOLDER_TIME.equals(tradingTime)) {
                e.kind = TickEvent.PLACEHOLDER;
                e.eventTs = Long.MIN_VALUE;
                return e;
            }
            e.eventTs = dateMs(date) + timeMs(tradingTime);
            e.kind = TickEvent.TRADE;
            return e;
        } catch (RuntimeException ex) {
            return malformed(e);
        }
    }

    private static TickEvent malformed(TickEvent e) {
        e.kind = TickEvent.MALFORMED;
        e.eventTs = Long.MIN_VALUE;
        return e;
    }

    /** "dd-MM-yyyy" to epoch ms at midnight. */
    long dateMs(String date) {
        if (!date.equals(lastDate)) {
            int d = Integer.parseInt(date.substring(0, 2));
            int m = Integer.parseInt(date.substring(3, 5));
            int y = Integer.parseInt(date.substring(6, 10));
            lastDateMs = LocalDate.of(y, m, d).toEpochDay() * 86_400_000L;
            lastDate = date;
        }
        return lastDateMs;
    }

    /** "HH:mm:ss.SSS" (milliseconds optional, any number of fraction digits) to ms since midnight. */
    static long timeMs(String t) {
        int h = (t.charAt(0) - '0') * 10 + (t.charAt(1) - '0');
        int m = (t.charAt(3) - '0') * 10 + (t.charAt(4) - '0');
        int s = (t.charAt(6) - '0') * 10 + (t.charAt(7) - '0');
        int ms = 0;
        if (t.length() > 9) {
            // Read up to three fraction digits, padding short ones (".5" means 500 ms)
            int digits = 0;
            for (int i = 9; i < t.length() && digits < 3; i++, digits++) {
                ms = ms * 10 + (t.charAt(i) - '0');
            }
            for (; digits < 3; digits++) {
                ms *= 10;
            }
        }
        return ((h * 60L + m) * 60L + s) * 1000L + ms;
    }
}
