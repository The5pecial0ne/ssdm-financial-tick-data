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


04-11-2026
The project was developed using a single TaskManager Docker deployment. Attempts to execute the job using higher parallelism values resulted in NoResourceAvailableException, indicating insufficient available task slots. Consequently, the evaluation was performed using the available execution resources.


11-11-2026
Branch pipeline-fixes-and-scaling: reworked the pipeline after checking it against the pandas answer key.

- Event time now comes from the Date column. Trading date is empty in the data, and the producer used to
  stamp every row as 08-11-2021, which made Flink drop every later day as late.
- window_start is the real start of the 5-minute window (it was the last trade's timestamp).
- The last window of each symbol now closes: bounded mode for experiments, and a flush marker that the
  producer sends at the end of a live replay.
- Trade rules are the same as in answer key v3: Last > 0, Trading time present and not 01:00:00.000.
- The producer keys every message by symbol, so a symbol's events stay in order inside one partition.
  The input topic has 8 partitions.
- Watermarks are generated inside the Kafka source, one per partition. In a live replay the producer also
  sends a heartbeat with the newest trade time to every partition, so quiet partitions don't hold back windows.
- The EMAs are kept in per-symbol state of the window operator; one object with two numbers per symbol.
- Three output topics: ema-results (Query 1), advisories (Query 2), late-trades (dropped as late).
  A symbol's first window is not an advisory, because both averages start at 0.
- All symbols are computed, only the ones in config/subscriptions.txt are published.
- The TaskManager has 8 task slots (it had 1, which caused NoResourceAvailableException above parallelism 1).
  The benchmark limits its CPU cores to the number of workers.
- Everything runs in Docker: the Flink image builds the jar itself, a second image holds the Python tools.
  Kafka is pinned to 3.9.0.
- Latency is measured from detected_at (set by Flink) to the broker's LogAppendTime on the output topic.
