FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY order-api/pom.xml order-api/pom.xml
COPY fulfillment-service/pom.xml fulfillment-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
RUN mvn -pl fulfillment-service -am -ntp -DskipTests dependency:go-offline
COPY common/src common/src
COPY fulfillment-service/src fulfillment-service/src
RUN mvn -pl fulfillment-service -am -ntp -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/fulfillment-service/target/fulfillment-service.jar /app/fulfillment-service.jar
ENTRYPOINT ["java", "-jar", "/app/fulfillment-service.jar"]
