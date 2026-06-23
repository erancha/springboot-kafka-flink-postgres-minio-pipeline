# syntax=docker/dockerfile:1
# Build context is the repo root so the shared contract modules (contract-eventtype, contract-userkeys)
# are reachable.
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY contract-eventtype ./contract-eventtype
COPY contract-userkeys ./contract-userkeys
COPY backend ./backend

# Cache mount keeps the Maven repo across builds, so an evicted layer recompiles without
# re-downloading the dependency tree; .m2 stays out of the image since the mount is build-only.
# Install the contract modules the backend depends on first, then build the backend against them; this
# avoids dragging the reactor aggregator (and the unrelated flink module) into the image.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -f contract-eventtype/pom.xml install && \
    mvn -q -DskipTests -f contract-userkeys/pom.xml install && \
    mvn -q -DskipTests -f backend/pom.xml package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/backend/target/backend.jar /app/backend.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/backend.jar"]
