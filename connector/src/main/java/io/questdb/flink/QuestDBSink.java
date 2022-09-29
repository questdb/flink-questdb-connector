package io.questdb.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import io.questdb.client.Sender;

public final class QuestDBSink implements Sink<RowData> {

    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBSink(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) {
        Sender.LineSenderBuilder builder = Sender
                .builder()
                .address(questDBConfiguration.getHost());
        if (questDBConfiguration.isTlsEnabled()) {
            builder.enableTls();
        }
        questDBConfiguration.getToken().ifPresent(t -> {
            String username = questDBConfiguration.getUserId();
            builder.enableAuth(username).authToken(t);
        });
        questDBConfiguration.getBufferSize().ifPresent(buffer -> builder.bufferCapacity(buffer * 1024));
        Sender sender = builder.build();
        return new QuestDBSinkWriter(physicalRowDataType, questDBConfiguration.getTable(), sender, context.metricGroup());
    }
}
