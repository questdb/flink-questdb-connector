package org.questdb.flink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;

import java.util.Objects;

public final class QuestDBDynamicTableSink implements DynamicTableSink {
    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBDynamicTableSink(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        Objects.requireNonNull(physicalRowDataType, "physical row data type cannot be null");
        Objects.requireNonNull(physicalRowDataType, "questdb configuration cannot be null");

        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new QuestDBSink(physicalRowDataType, questDBConfiguration), questDBConfiguration.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new QuestDBDynamicTableSink(physicalRowDataType, questDBConfiguration);
    }

    @Override
    public String asSummaryString() {
        return QuestDBDynamicSinkFactory.FACTORY_IDENTIFIER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuestDBDynamicTableSink that = (QuestDBDynamicTableSink) o;
        return physicalRowDataType.equals(that.physicalRowDataType) && questDBConfiguration.equals(that.questDBConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalRowDataType, questDBConfiguration);
    }
}
