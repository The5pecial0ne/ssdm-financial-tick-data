public class EmaResult {

    public String ID;

    public String SecType;

    public Long windowStart;

    public Double close;

    public Double ema38;

    public Double ema100;

    public Boolean firstWindow;

    public String signal;

    public EmaResult() {
    }
    @Override
public String toString() {
    return "EmaResult{" +
            "ID='" + ID + '\'' +
            ", SecType='" + SecType + '\'' +
            ", windowStart=" + windowStart +
            ", close=" + close +
            ", ema38=" + ema38 +
            ", ema100=" + ema100 +
            ", firstWindow=" + firstWindow +
            ", signal='" + signal + '\'' +
            '}';
}
}