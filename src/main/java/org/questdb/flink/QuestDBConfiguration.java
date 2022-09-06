package org.questdb.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.Optional;

public final class QuestDBConfiguration implements Serializable {
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("QuestDB server hostname and port");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .defaultValue("admin")
                    .withDescription("QuestDB server authentication username");

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("QuestDB server authentication token");

    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("QuestDB target table");

    public static final ConfigOption<Boolean> TLS =
            ConfigOptions.key("tls")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional enable TLS/SSL encryption");

    public static final ConfigOption<Integer> BUFFER_SIZE_BYTES =
            ConfigOptions.key("buffer.size.kb")
                    .intType()
                    .noDefaultValue()
                    .withDescription("ILP client buffer size in KB");


    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    private final ReadableConfig readableConfig;
    private final String internalCatalogName;


    public QuestDBConfiguration(ReadableConfig readableConfig, String internalCatalogName) {
        this.readableConfig = readableConfig;
        this.internalCatalogName = internalCatalogName;
    }

    public String getTable() {
        return readableConfig.getOptional(TABLE).orElse(internalCatalogName);
    }

    public String getHost() {
        return readableConfig.get(HOST);
    }

    public boolean isTlsEnabled() {
        return readableConfig.get(TLS);
    }

    public String getUserId() {
        return readableConfig.get(USERNAME);
    }

    public Optional<String> getToken() {
        return readableConfig.getOptional(TOKEN);
    }

    public Integer getParallelism() {
        return readableConfig.getOptional(SINK_PARALLELISM).orElse(null);
    }

    public Optional<Integer> getBufferSize() {
        return readableConfig.getOptional(BUFFER_SIZE_BYTES);
    }
}
