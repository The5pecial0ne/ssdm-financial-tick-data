package ssdm;

/**
 * Output of Query 1 (both EMAs for a symbol and window) and Query 2 (the advisory, if any).
 * {@code signal} is "buy", "sell" or "".
 */
public class EmaResult {

    public String symbol;
    public String secType;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long trades;
    public double ema38;
    public double ema100;
    public boolean firstWindow;
    public String signal;

    // Timing fields for the latency evaluation (all wall-clock ms)
    public long maxIngestTs;
    public long firedTs;
    public long emitTs;

    public EmaResult() {}
}
