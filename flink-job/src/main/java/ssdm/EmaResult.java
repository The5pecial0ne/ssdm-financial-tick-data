package ssdm;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Query 1 and Query 2 output for one symbol and one 5-minute window. */
public class EmaResult {

    private static final DateTimeFormatter ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    public String symbol;
    public String secType;
    /** Start of the 5-minute window, ms. */
    public long windowStart;
    /** Last trade price seen in the window. */
    public double close;
    public double ema38;
    public double ema100;
    /** True for a symbol's very first window, where both averages start from 0. */
    public boolean firstWindow;
    /** "buy", "sell" or "" when the averages did not cross. */
    public String signal;
    /** Wall-clock ms at which Flink computed this result. Used to measure latency. */
    public long detectedAt;

    public EmaResult() {}

    /**
     * A crossover a trader should hear about. A symbol's first window always
     * looks like a buy, only because both averages start at 0, so it is not one.
     */
    public boolean isAdvisory() {
        return !firstWindow && !signal.isEmpty();
    }

    /** One JSON line, with the same field names as the answer key. */
    public String toJson(String runId) {
        return "{\"symbol\":\"" + symbol + "\""
                + ",\"SecType\":\"" + secType + "\""
                + ",\"window_start\":\"" + ISO.format(Instant.ofEpochMilli(windowStart)) + "\""
                + ",\"close\":" + close
                + ",\"ema38\":" + ema38
                + ",\"ema100\":" + ema100
                + ",\"first_window\":" + firstWindow
                + ",\"signal\":\"" + signal + "\""
                + ",\"detected_at\":" + detectedAt
                + ",\"run_id\":\"" + runId + "\"}";
    }
}
