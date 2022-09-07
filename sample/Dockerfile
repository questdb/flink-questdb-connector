FROM maven:3.8-jdk-8-slim AS builder
COPY ./pom.xml /opt/pom.xml
COPY ./src /opt/src

RUN cd /opt; mvn clean install -Dmaven.test.skip

FROM --platform=linux/amd64 samples/flink-questdb-connector
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.14.4/flink-sql-connector-kafka_2.12-1.14.4.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.12/1.14.4/flink-connector-jdbc_2.12-1.14.4.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.14.4/flink-csv-1.14.4.jar;

COPY --from=builder /opt/target/sample-*.jar /opt/flink/usrlib/sample.jar

RUN echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
