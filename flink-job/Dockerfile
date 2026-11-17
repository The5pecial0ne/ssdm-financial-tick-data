# Flink image with the trading job baked in. Used for the JobManager and the TaskManager,
# so `docker compose up` gives a cluster that can run the job straight away.
# Build context is the repo root (see docker-compose.yml), so config/ can be copied in.

# ---- build the jar
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY flink-job/pom.xml .
RUN mvn -B -q dependency:go-offline
COPY flink-job/src ./src
RUN mvn -B -q package -DskipTests

# ---- runtime
FROM flink:1.20.0-java17
COPY --from=build /src/target/flink-job-1.0.jar /opt/ssdm/trading-job.jar
COPY config/ /opt/ssdm/config/
