public class EmaResult {

    public String symbol;

    public Double closePrice;

    public Double ema38;

    public Double ema100;

    public Long windowEnd;

    public EmaResult() {
    }

    public EmaResult(
            String symbol,
            Double closePrice,
            Double ema38,
            Double ema100,
            Long windowEnd) {

        this.symbol = symbol;
        this.closePrice = closePrice;
        this.ema38 = ema38;
        this.ema100 = ema100;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {

        return "EmaResult{" +
                "symbol='" + symbol + '\'' +
                ", closePrice=" + closePrice +
                ", ema38=" + ema38 +
                ", ema100=" + ema100 +
                ", windowEnd=" + windowEnd +
                '}';
    }
}