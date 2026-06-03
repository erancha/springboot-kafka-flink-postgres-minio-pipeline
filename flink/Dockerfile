FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM apache/flink:2.2-java21
USER root
RUN mkdir -p /opt/job
COPY --from=build /app/target/flink-job.jar /opt/job/app.jar
USER flink
