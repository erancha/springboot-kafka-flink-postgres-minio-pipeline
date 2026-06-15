# syntax=docker/dockerfile:1
# Build context is the repo root so the shared contract module (contract-eventtype) is reachable.
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY contract-eventtype ./contract-eventtype
COPY flink ./flink

# Cache mount keeps the Maven repo across builds, so an evicted layer recompiles without
# re-downloading the dependency tree; .m2 stays out of the image since the mount is build-only.
# Install the contract-eventtype module first, then build the flink job against it; this avoids dragging the
# reactor aggregator (and the unrelated backend module) into the image.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -f contract-eventtype/pom.xml install && \
    mvn -q -DskipTests -f flink/pom.xml package

FROM apache/flink:2.2-java21
USER root
RUN mkdir -p /opt/job
COPY --from=build /app/flink/target/flink-job.jar /opt/job/app.jar
# JDBC driver staged into Flink's lib; see flink/pom.xml.
COPY --from=build /app/flink/target/flink-lib/ /opt/flink/lib/
# The stock config.yaml ships --add-exports=jdk.compiler/... in env.java.opts.all, but this image
# is JRE-based and has no jdk.compiler module, so every Flink JVM prints "Unknown module" at startup
# (a pre-logging stderr line log4j cannot filter). Strip only those tokens; all other current and
# future flags survive, so this does not drift when the base image's opts change.
RUN sed -i -E 's#--add-exports=jdk\.compiler/[^ ]+ ##g' /opt/flink/conf/config.yaml
USER flink
