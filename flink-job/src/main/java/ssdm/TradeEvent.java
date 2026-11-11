package ssdm;

/**
 * One line from the tick data, after parsing.
 *
 * Most lines in the dataset are not trades (bids, asks, housekeeping), so
 * {@code trade} tells the rest of the pipeline whether this one carries a
 * usable price. Public fields plus an empty constructor let Flink treat it as
 * a POJO, which is the fast path for sending it between operators.
 */
public class TradeEvent {

    /** Event time given to lines that are not trades. They never move the watermark. */
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    public String symbol;
    public String secType;
    /** When the trade happened (Date + Trading time), in ms. Read as UTC, see TradeEventParser. */
    public long ts;
    public double price;
    public boolean trade;

    public TradeEvent() {}
}
