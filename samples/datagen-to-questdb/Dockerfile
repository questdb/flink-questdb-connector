FROM maven:3.8-jdk-11-slim AS dep-cache
WORKDIR /opt
COPY pom.xml /opt/pom.xml
RUN mvn -B -f ./pom.xml dependency:go-offline

FROM dep-cache AS builder
WORKDIR /opt
COPY src /opt/src
RUN mvn install -Dmaven.test.skip

FROM flink:1.16.1-scala_2.12
COPY --from=builder /opt/target/flink-questdb-connector-samples-datagen-to-questdb-*.jar /opt/flink/usrlib/datagen-to-questdb.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/questdb/flink-questdb-connector/0.2/flink-questdb-connector-0.2.jar

RUN echo "pipeline.time-characteristic: EventTime" >> /opt/flink/conf/flink-conf.yaml; \
    echo "taskmanager.memory.jvm-metaspace.size: 256m" >> /opt/flink/conf/flink-conf.yaml;
