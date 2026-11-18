package ssdm;

/**
 * One event from the DEBS 2022 tick data, after parsing.
 *
 * Only a handful of the 39 columns matter for the queries, so that's all we keep.
 * The {@code kind} field says what sort of event it was, because the raw data
 * doesn't mark event types explicitly: a trade is an event with a positive Last price.
 */
public class TickEvent {

    /** A real trade: Last > 0 and a usable trading time. The only kind the queries use. */
    public static final int TRADE = 0;
    /** Quote updates (bid/ask) and housekeeping events with no trade price. */
    public static final int NON_TRADE = 1;
    /** Trades stamped with the fake 01:00:00.000 trading time. Their real time is unknown. */
    public static final int PLACEHOLDER = 2;
    /** Lines we couldn't parse at all. */
    public static final int MALFORMED = 3;
    /** End-of-stream marker sent by the producer, so the last windows get evaluated. */
    public static final int END_OF_STREAM = 4;
    /**
     * Start-of-stream marker the producer sends to every partition before the data. It wakes up
     * partitions that were idle while the job waited for input, so none of them is left out of the
     * watermark when the first trades arrive (see producer.py, announce_start).
     */
    public static final int START_OF_STREAM = 5;

    public String symbol;
    public String secType;
    /** Event time in ms: Date + Trading time, read as if it were UTC so windows line up with midnight. */
    public long eventTs;
    /** Position in the source file. Breaks ties between trades with the same timestamp. */
    public long seq;
    public double price;
    /** Wall-clock time the event entered the system (Kafka record timestamp set by the producer). */
    public long ingestTs;
    public int kind;

    public TickEvent() {}

    @Override
    public String toString() {
        return "TickEvent{" + symbol + " " + secType + " ts=" + eventTs + " seq=" + seq
                + " price=" + price + " kind=" + kind + "}";
    }
}
