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

17-11-2026
The single-slot limit above is solved: the TaskManager now gets as many task slots as CPU cores
(TM_SLOTS / TM_CPUS in docker-compose.yml), and the job runs with parallelism 1 to 8. The input topic
is now `ticks` (8 partitions, keyed by symbol) instead of `trading-events`. The full list of design
decisions, with alternatives and evidence, is in report/materials/design-decisions.md.

25-11-2026
Final measurements: 1 to 8 cores with 3 repeats each (results/scalability/20261125-223400-cores-1-8-r3).
The report is in report/report.pdf.
