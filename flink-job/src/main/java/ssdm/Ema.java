package ssdm;

/**
 * The maths for Query 1 and Query 2, kept apart from Flink so it is easy to unit test.
 */
public final class Ema {

    public static final double ALPHA_38 = 2.0 / (1 + 38);
    public static final double ALPHA_100 = 2.0 / (1 + 100);

    private Ema() {}

    /** EMA of the current window, given the last close in it and the previous window's EMA. */
    public static double next(double close, double previous, double alpha) {
        return close * alpha + previous * (1.0 - alpha);
    }

    /**
     * Breakout detection (project spec equations 1 to 4).
     * Returns "buy" for a bullish crossover, "sell" for a bearish one, and "" otherwise.
     */
    public static String signal(double prev38, double prev100, double ema38, double ema100) {
        if (ema38 > ema100 && prev38 <= prev100) {
            return "buy";
        }
        if (ema38 < ema100 && prev38 >= prev100) {
            return "sell";
        }
        return "";
    }
}
