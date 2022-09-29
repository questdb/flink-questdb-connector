package io.questdb.flink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;

import io.questdb.client.Sender;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class QuestDBSinkWriter implements SinkWriter<RowData> {
    private final Sender sender;
    private final DataType physicalRowDataType;
    private final String targetTable;
    private final Counter numRecordsOut;

    public QuestDBSinkWriter(DataType physicalRowDataType, String targetTable, Sender sender, SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.physicalRowDataType = physicalRowDataType;
        this.targetTable = targetTable;
        this.sender = sender;
        this.numRecordsOut = sinkWriterMetricGroup.getNumRecordsSendCounter();
    }

    @Override
    public void write(RowData element, Context context) {
        Long timestamp = context.timestamp();
        write(element, timestamp);
    }

    private void write(RowData rw, Long ts) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        sender.table(targetTable);
        try {
            for (int i = 0; i < fields.size(); i++) {
                RowType.RowField rowField = fields.get(i);
                LogicalType type = rowField.getType();
                String name = rowField.getName();
                LogicalTypeRoot logicalTypeRoot = type.getTypeRoot();
                switch (logicalTypeRoot) {
                    case CHAR:
                    case VARCHAR:
                        sender.stringColumn(name, rw.getString(i).toString());
                        break;
                    case BOOLEAN:
                        sender.boolColumn(name, rw.getBoolean(i));
                        break;
                    case DECIMAL:
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        sender.longColumn(name, rw.getLong(i));
                        break;
                    case FLOAT:
                        sender.doubleColumn(name, rw.getFloat(i));
                        break;
                    case DOUBLE:
                        sender.doubleColumn(name, rw.getDouble(i));
                        break;
                    case DATE:
                        LocalDate localDate = LocalDate.ofEpochDay(rw.getInt(i));
                        ZonedDateTime utc = localDate.atStartOfDay(ZoneId.of("UTC"));
                        long micros = TimeUnit.SECONDS.toMicros(utc.toEpochSecond());
                        sender.timestampColumn(name, micros);
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        int timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                        TimestampData timestamp = rw.getTimestamp(i, timestampPrecision);
                        micros = TimeUnit.MILLISECONDS.toMicros(timestamp.getMillisecond());
                        long microsInMillis = TimeUnit.NANOSECONDS.toMicros(timestamp.getNanoOfMillisecond());
                        sender.timestampColumn(name, micros + microsInMillis);
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        timestampPrecision = ((TimestampType) type).getPrecision();
                        timestamp = rw.getTimestamp(i, timestampPrecision);
                        micros = TimeUnit.MILLISECONDS.toMicros(timestamp.getMillisecond());
                        microsInMillis = TimeUnit.NANOSECONDS.toMicros(timestamp.getNanoOfMillisecond());
                        sender.timestampColumn(name, micros + microsInMillis);
                        break;
                    case TIMESTAMP_WITH_TIME_ZONE:
                        timestampPrecision = ((ZonedTimestampType) type).getPrecision();
                        timestamp = rw.getTimestamp(i, timestampPrecision);
                        micros = TimeUnit.MILLISECONDS.toMicros(timestamp.getMillisecond());
                        microsInMillis = TimeUnit.NANOSECONDS.toMicros(timestamp.getNanoOfMillisecond());
                        sender.timestampColumn(name, micros + microsInMillis);
                        break;
                    case TIME_WITHOUT_TIME_ZONE:
                        long l = rw.getInt(i);
                        sender.longColumn(name, l);
                        break;
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_DAY_TIME:
                    case ARRAY:
                    case MULTISET:
                    case MAP:
                    case ROW:
                    case DISTINCT_TYPE:
                    case STRUCTURED_TYPE:
                    case NULL:
                    case RAW:
                    case SYMBOL:
                    case UNRESOLVED:
                    case BINARY:
                    case VARBINARY:
                    default:
                        throw new UnsupportedOperationException(logicalTypeRoot + " type not supported");
                }
            }
        } finally {
            if (ts == null) {
                sender.atNow();
            } else {
                sender.at(TimeUnit.MILLISECONDS.toNanos(ts));
            }
            numRecordsOut.inc();
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        sender.flush();
    }

    @Override
    public void close() {
        sender.close();
    }
}
