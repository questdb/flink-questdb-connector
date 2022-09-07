package org.quesdb.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


public class SampleApplication {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE Orders (\n"
                + "     order_number BIGINT,\n"
                + "     price        BIGINT,\n"
                + "     buyer        STRING"
                + " ) WITH (\n"
                + "   'connector' = 'datagen',\n"
                + "   'rows-per-second' = '100'\n"
                + " )");

        tEnv.executeSql("CREATE TABLE Quest (\n"
                + "     order_number BIGINT,\n"
                + "     price        BIGINT,\n"
                + "     buyer        STRING\n"
                + " ) WITH (\n"
                + "   'connector'='questdb',\n"
                + "   'host'='questdb',\n"
                + "   'buffer.size.kb'='1',\n"
                + "   'table'='from_flink'\n"
                + ");");

        tEnv.from("Orders").executeInsert("Quest");
    }
}
