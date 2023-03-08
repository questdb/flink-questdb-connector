package io.questdb.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.STRING;


public class KafkaToQuestDB {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Define Kafka source
        tEnv.executeSql("CREATE TABLE Orders (\n"
                + "  type STRING,\n"
                + "  product_id STRING,\n"
                + "  changes ARRAY<ARRAY<STRING>>,\n"
                + "  `time` STRING,\n"
                + "  ts as TO_TIMESTAMP(`time`, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSSVV'),\n"
                + "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic' = 'Orders',\n"
                + "  'properties.bootstrap.servers' = 'kafka:9092',\n"
                + "  'value.format' = 'json',\n"
                + "  'properties.group.id' = 'flinkGroup',\n"
                + "  'scan.startup.mode' = 'latest-offset'\n"
                + ");");

        // Define QuestDB sink. It's using Table API instead of SQL.
        // Table API and SQL are equivalent and mostly inter-changeable.
        // You can see Table API as a typed SQL.
        tEnv.createTable("Quest", TableDescriptor.forConnector("questdb")
                .schema(Schema.newBuilder()
                        .column("product_id", STRING())
                        .column("side", STRING())
                        .column("price", DOUBLE())
                        .column("volume", DOUBLE())
                        .build())
                .option("host", "questdb")
                .option("buffer.size.kb", "1")
                .option("table", "orders")
                .build());

        // Data processing pipeline.
        // In reads from Kafka source, filter to get only `l2update` messages, expands the outer array
        // and finally insert the result into QuestDB sink.
        tEnv.executeSql("INSERT INTO Quest "
                + "SELECT product_id, "
                    + "changeTable.change[1] as side, "
                    + "cast(changeTable.change[2] as double) as price, "
                    + "cast(changeTable.change[3] as double) as volume\n"
                + "FROM Orders\n"
                + "CROSS JOIN UNNEST(Orders.changes) AS changeTable (change)\n"
                + "WHERE type = 'l2update'");
    }
}