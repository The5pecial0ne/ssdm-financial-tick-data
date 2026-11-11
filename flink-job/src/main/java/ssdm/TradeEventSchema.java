package ssdm;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

/**
 * Lets the Kafka source hand out {@link TradeEvent}s directly.
 *
 * Parsing inside the source (rather than in a map step after it) matters:
 * the source can then read each trade's time and keep one watermark per Kafka
 * partition. A slow partition can no longer make another one's trades look late.
 */
public class TradeEventSchema extends AbstractDeserializationSchema<TradeEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public TradeEvent deserialize(byte[] message) {
        // Returning null skips the record, which is what we want for garbage lines.
        return TradeEventParser.parse(new String(message, StandardCharsets.UTF_8));
    }
}
