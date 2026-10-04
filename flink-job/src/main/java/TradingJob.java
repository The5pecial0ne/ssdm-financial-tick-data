import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TradingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("Trading Job Started")
           .print();

        env.execute("Trading Job");
    }
}
