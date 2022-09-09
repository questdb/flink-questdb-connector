package org.questdb.flink;

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

        tEnv.createTable("Quest", TableDescriptor.forConnector("questdb")
                .schema(Schema.newBuilder()
                        .column("product_id", STRING())
                        .column("side", STRING())
                        .column("price", DOUBLE())
                        .column("volume", DOUBLE())
                        .build())
                .option(QuestDBConfiguration.HOST, "questdb")
                .option(QuestDBConfiguration.BUFFER_SIZE_KB, 1)
                .option(QuestDBConfiguration.TABLE, "orders")
                .build());

        tEnv.executeSql("insert into Quest "
                + "select product_id as instrument, "
                    + "changeTable.change[1] as side, "
                    + "cast(changeTable.change[2] as double) as price, "
                    + "cast(changeTable.change[3] as double) as volume\n"
                + "from Orders\n"
                + "CROSS JOIN UNNEST(Orders.changes) AS changeTable (change)\n"
                + "where type = 'l2update'");
    }
}