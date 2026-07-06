FROM gradle:8.9-jdk21 AS build
WORKDIR /app
COPY settings.gradle build.gradle ./
COPY src ./src
RUN gradle --no-daemon clean installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/install/largegraph-pagerank ./
ENTRYPOINT ["/app/bin/largegraph-pagerank"]
