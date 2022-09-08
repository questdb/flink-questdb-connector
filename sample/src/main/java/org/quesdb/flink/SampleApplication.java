package org.quesdb.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.questdb.flink.QuestDBConfiguration;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;


public class SampleApplication {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.createTable("Orders", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("order_number", STRING())
                        .column("price", BIGINT())
                        .column("buyer", STRING())
                        .build())
                .option("rows-per-second", "100")
                .build());

        tEnv.createTable("Quest", TableDescriptor.forConnector("questdb")
                .schema(Schema.newBuilder()
                        .column("order_number", STRING())
                        .column("price", BIGINT())
                        .column("buyer", STRING())
                        .build())
                .option(QuestDBConfiguration.HOST, "questdb")
                .option(QuestDBConfiguration.BUFFER_SIZE_KB, 1)
                .option(QuestDBConfiguration.TABLE, "from_flink")
                .build());

        tEnv.from("Orders").executeInsert("Quest");
    }
}