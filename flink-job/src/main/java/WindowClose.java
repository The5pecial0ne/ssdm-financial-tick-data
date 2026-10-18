public class WindowClose {

    public String symbol;
    public Double closePrice;
    public Long windowEnd;

    public WindowClose() {}

    public WindowClose(
            String symbol,
            Double closePrice,
            Long windowEnd) {

        this.symbol = symbol;
        this.closePrice = closePrice;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "WindowClose{" +
                "symbol='" + symbol + '\'' +
                ", closePrice=" + closePrice +
                ", windowEnd=" + windowEnd +
                '}';
    }
}