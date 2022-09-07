FROM maven:3.8-jdk-8-slim AS builder
COPY ./pom.xml /opt/pom.xml
COPY ./connector/pom.xml /opt/connector/pom.xml
COPY ./connector/src /opt/connector/src
RUN cd /opt/connector; mvn clean install -Dmaven.test.skip

FROM apache/flink:1.15.2-scala_2.12-java8
COPY --from=builder /opt/connector/target/flink-questdb-connector-*.jar /opt/flink/lib/flink-questdb-connector.jar