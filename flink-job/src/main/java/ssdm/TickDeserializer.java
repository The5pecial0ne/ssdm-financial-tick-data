package ssdm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Reads a Kafka record into a TickEvent. The record's own timestamp (set by the producer
 * when it sent the event) becomes the ingest time used for latency measurements.
 */
public class TickDeserializer implements KafkaRecordDeserializationSchema<TickEvent> {

    private transient TickParser parser;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TickEvent> out) {
        if (parser == null) {
            parser = new TickParser();
        }
        if (record.value() == null) {
            return;
        }
        String line = new String(record.value(), StandardCharsets.UTF_8);
        TickEvent e = parser.parse(line, record.timestamp(), record.offset());
        if (e != null) {
            out.collect(e);
        }
    }

    @Override
    public TypeInformation<TickEvent> getProducedType() {
        return TypeInformation.of(TickEvent.class);
    }
}
