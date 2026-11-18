package ssdm;

/** The closing price of one symbol in one 5-minute window: the input to the EMA step. */
public class WindowClose {

    public String symbol;
    public String secType;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long trades;
    public long lastTradeTs;
    public long maxIngestTs;
    /** Wall-clock time the window fired (the watermark passed its end). */
    public long firedTs;

    public WindowClose() {}
}
