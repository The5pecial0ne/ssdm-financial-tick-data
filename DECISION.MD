#Project Decisions

29-09-2026

-Using Docker for all infrastructure.
-Using kafka for event streaming.
-Using Flink for stream proessing.

04-10-2026

-Python virtual environment created
-pandas successfully installed
-DEBS Sample data successfully loaded
-Required columns identified:
ID, Last , Trading time , Trading date.
- Convert rows to JSON events, and publishes them to Kafka topic "trading-events".

Successfully verified all 10 records through Kafka consumer.

17-10-2026
 Kafka -> Flink integration completed.
Producer events successfully consumed by Flink KafkaSource.
Running job remains active and process incoming Kafka messages.
