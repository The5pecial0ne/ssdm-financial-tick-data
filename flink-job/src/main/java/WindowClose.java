public class WindowClose {

    public String ID;

    public String SecType;

    public Long windowStart;

    public Double close;

    public WindowClose() {}

    public WindowClose(String ID, String SecType, Long windowStart, Double close) {
        this.ID = ID;
        this.SecType = SecType;
        this.windowStart = windowStart;
        this.close = close;
    }

    @Override
    public String toString() {
        return "WindowClose{" +
                "ID='" + ID + '\'' +
                ", SecType='" + SecType + '\'' +
                ", windowStart=" + windowStart +
                ", close=" + close +
                '}';
    }
}