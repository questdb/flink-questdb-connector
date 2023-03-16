FROM maven:3.8-jdk-11-slim AS dep-cache
WORKDIR /opt
COPY pom.xml /opt/pom.xml
RUN mvn -B -f ./pom.xml dependency:go-offline

FROM dep-cache AS builder
WORKDIR /opt
COPY src /opt/src
RUN mvn install -Dmaven.test.skip
RUN ls -l target

FROM flink:1.16.1-scala_2.12
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.1/flink-sql-connector-kafka-1.16.1.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/questdb/flink-questdb-connector/0.2/flink-questdb-connector-0.2.jar
COPY --from=builder /opt/target/flink-questdb-connector-samples-coinbase-to-kafka-to-questdb-*.jar /opt/flink/usrlib/coinbase-to-kafka-to-questdb.jar
RUN ls -l /opt/flink/usrlib/

RUN echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
