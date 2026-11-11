# Stage 1: build the job jar with Maven, so nobody needs Java or Maven installed.
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -q clean package -DskipTests

# Stage 2: a normal Flink image with our jar inside.
# The same image runs as JobManager, as TaskManager and as the job submitter.
FROM flink:1.20-java11
COPY --from=build /build/target/trading-job.jar /opt/flink/job/trading-job.jar
