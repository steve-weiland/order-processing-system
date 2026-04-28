FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY order-api/pom.xml order-api/pom.xml
COPY fulfillment-service/pom.xml fulfillment-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
RUN mvn -pl order-api -am -ntp -DskipTests dependency:go-offline
COPY common/src common/src
COPY order-api/src order-api/src
RUN mvn -pl order-api -am -ntp -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /build/order-api/target/order-api.jar /app/order-api.jar
EXPOSE 6080
ENTRYPOINT ["java", "-jar", "/app/order-api.jar"]
