public class TradeEvent {

    public String symbol;
    public Double price;
    public String tradingTime;
    public String tradingDate;
    public Long eventTimestamp;
    public String SecType;

    public TradeEvent() {}

    @Override
    public String toString() {
        return "TradeEvent{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                ", tradingTime='" + tradingTime + '\'' +
                ", tradingDate='" + tradingDate + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                ", SecType='" + SecType + '\'' +
                '}';
    }
}