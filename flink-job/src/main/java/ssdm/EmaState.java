package ssdm;

/** The two averages of one symbol after its latest window. This is all Flink remembers per symbol. */
public class EmaState {

    public double ema38;
    public double ema100;

    public EmaState() {}
}
