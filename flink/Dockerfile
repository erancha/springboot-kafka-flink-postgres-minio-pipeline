FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM apache/flink:2.2-java21
USER root
RUN mkdir -p /opt/job
COPY --from=build /app/target/flink-job.jar /opt/job/app.jar
# JDBC driver staged into Flink's lib; see flink/pom.xml.
COPY --from=build /app/target/flink-lib/ /opt/flink/lib/
# The stock config.yaml ships --add-exports=jdk.compiler/... in env.java.opts.all, but this image
# is JRE-based and has no jdk.compiler module, so every Flink JVM prints "Unknown module" at startup
# (a pre-logging stderr line log4j cannot filter). Strip only those tokens; all other current and
# future flags survive, so this does not drift when the base image's opts change.
RUN sed -i -E 's#--add-exports=jdk\.compiler/[^ ]+ ##g' /opt/flink/conf/config.yaml
USER flink
