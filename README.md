Prerequisites
  Docker
  Java 11+
  Maven
  Python 3

1. Clone repository

2. Python environment
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt

3. Start infrastructure
   docker compose up -d

4. Create Kafka topics
   ./create-topics.sh

5. Build Flink application
   cd flink-job
   mvn clean package

6. Open Flink
   http://localhost:8081

7. Upload
   flink-job-1.0.jar

8. Run with parallelism 1

9. From repository root run producer
   python3 producer/producer.py

10. Run dashboard
    streamlit run dashboard/dashboard.py

    Host applications:
    localhost:9092

    Docker/Flink:
    kafka:29092

