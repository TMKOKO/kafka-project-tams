# Builds one service: docker build --build-arg MODULE=payment-service -t payments/payment-service:0.1.0 .
# Unit tests for the module and common run during the build, so a failing test fails the image.
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml ./
COPY common ./common
COPY payment-service ./payment-service
COPY fraud-service ./fraud-service
COPY notification-service ./notification-service
COPY audit-service ./audit-service
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl ${MODULE} -am package \
    && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

FROM eclipse-temurin:21-jre
# curl lets the demo scripts read each pod's endpoints through kubectl exec.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /app.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
