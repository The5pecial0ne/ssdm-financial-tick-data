import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EmaProcessor
    extends KeyedProcessFunction<
        String,
        WindowClose,
        EmaResult> {

    private ValueState<Double> ema38State;
    private ValueState<Double> ema100State;
    private ValueState<Double> prev38State;
    private ValueState<Double> prev100State;

    @Override
    public void open(Configuration parameters) {

        ema38State =
            getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                    "ema38",
                    Double.class
                )
            );

        ema100State =
            getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                    "ema100",
                    Double.class
                )
            );
    }

    @Override
    public void processElement(
        WindowClose windowClose,
        Context context,
        Collector<EmaResult> collector) throws Exception {
        
        Double prev38 = ema38State.value();
        Double prev100 = ema100State.value();

    if (prev38 == null) {
        prev38 = 0.0;
    }

    if (prev100 == null) {
        prev100 = 0.0;
    }

    String signal = "";
   
  
    double A38 = 2.0 / 39.0;
    double A100 = 2.0 / 101.0;

    double ema38 =
            windowClose.close * A38
            + prev38 * (1.0 - A38);

    double ema100 =
            windowClose.close * A100
            + prev100 * (1.0 - A100);
  if (
    ema38 > ema100 &&
    prev38 <= prev100
) {
    signal = "buy";
}
if (
    ema38 < ema100 &&
    prev38 >= prev100
) {
    signal = "sell";
}

    ema38State.update(ema38);
    ema100State.update(ema100);

    EmaResult result = new EmaResult();

    result.ID = windowClose.ID;
    result.SecType = windowClose.SecType;
    result.windowStart = windowClose.windowStart;
    result.close = windowClose.close;

    result.ema38 = ema38;
    result.ema100 = ema100;

    result.firstWindow = (prev38 == 0.0 && prev100 == 0.0);

    result.signal = signal;

    collector.collect(result);

    System.out.println("EMA Result: " + windowClose.ID
            + " " + windowClose.SecType
            + " " + windowClose.windowStart
            + " close=" + windowClose.close
            + " ema38=" + ema38
            + " ema100=" + ema100
            + " firstWindow=" + result.firstWindow);
        }
}