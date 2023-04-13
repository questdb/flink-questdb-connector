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
    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    private static final TimeUnitConverter TO_MICROS = (duration, timeUnit) -> timeUnit.toMicros(duration);
    private static final TimeUnitConverter TO_NANOS = (duration, timeUnit) -> timeUnit.toNanos(duration);

    private final Sender sender;
    private final DataType physicalRowDataType;
    private final String targetTable;
    private final Counter numRecordsOut;
    private final String timestampColumn;

    public QuestDBSinkWriter(DataType physicalRowDataType, String targetTable, Sender sender, SinkWriterMetricGroup sinkWriterMetricGroup, String timestampColumn) {
        this.physicalRowDataType = physicalRowDataType;
        this.targetTable = targetTable;
        this.sender = sender;
        this.numRecordsOut = sinkWriterMetricGroup.getNumRecordsSendCounter();
        this.timestampColumn = timestampColumn;
    }

    @Override
    public void write(RowData element, Context context) {
        Long timestamp = context.timestamp();
        write(element, timestamp);
    }

    private static long extractDesignatedTimestamp(LogicalType type, RowData rw, int pos) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                int timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                TimestampData timestamp = rw.getTimestamp(pos, timestampPrecision);
                return convertTimestamp(TO_NANOS, timestamp);
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                int timestampPrecision = ((TimestampType) type).getPrecision();
                TimestampData timestamp = rw.getTimestamp(pos, timestampPrecision);
                return convertTimestamp(TO_NANOS, timestamp);
            }
            case TIMESTAMP_WITH_TIME_ZONE: {
                int timestampPrecision = ((ZonedTimestampType) type).getPrecision();
                TimestampData timestamp = rw.getTimestamp(pos, timestampPrecision);
                return convertTimestamp(TO_NANOS, timestamp);
            }
            default:
                throw new UnsupportedOperationException(type + " type is not supported as a designated timestamp column");
        }
    }

    private void write(RowData rw, final Long eventTimeMillis) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        sender.table(targetTable);
        long extractedTimestamp = -1;
        try {
            for (int i = 0; i < fields.size(); i++) {
                RowType.RowField rowField = fields.get(i);
                LogicalType type = rowField.getType();
                String name = rowField.getName();
                LogicalTypeRoot logicalTypeRoot = type.getTypeRoot();
                if (name.equals(timestampColumn)) {
                    extractedTimestamp = extractDesignatedTimestamp(type, rw, i);
                } else {
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
                        case DATE: {
                            LocalDate localDate = LocalDate.ofEpochDay(rw.getInt(i));
                            ZonedDateTime utc = localDate.atStartOfDay(UTC_ZONE_ID);
                            long value = TO_MICROS.convert(utc.toEpochSecond(), TimeUnit.SECONDS);
                            sender.timestampColumn(name, value);
                            break;
                        }
                        case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                            int timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                            TimestampData timestamp = rw.getTimestamp(i, timestampPrecision);
                            sender.timestampColumn(name, convertTimestamp(TO_MICROS, timestamp));
                            break;
                        }
                        case TIMESTAMP_WITHOUT_TIME_ZONE: {
                            int timestampPrecision = ((TimestampType) type).getPrecision();
                            TimestampData timestamp = rw.getTimestamp(i, timestampPrecision);
                            sender.timestampColumn(name, convertTimestamp(TO_MICROS, timestamp));
                            break;
                        }
                        case TIMESTAMP_WITH_TIME_ZONE: {
                            int timestampPrecision = ((ZonedTimestampType) type).getPrecision();
                            TimestampData timestamp = rw.getTimestamp(i, timestampPrecision);
                            sender.timestampColumn(name, convertTimestamp(TO_MICROS, timestamp));
                            break;
                        }
                        case TIME_WITHOUT_TIME_ZONE: {
                            long l = rw.getInt(i);
                            sender.longColumn(name, l);
                            break;
                        }
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
            }
        } finally {
            if (extractedTimestamp != -1) {
                sender.at(extractedTimestamp);
            } else if (eventTimeMillis != null) {
                sender.at(TimeUnit.MILLISECONDS.toNanos(eventTimeMillis));
            } else {
                sender.atNow();
            }
            numRecordsOut.inc();
        }
    }

    private static long convertTimestamp(TimeUnitConverter timeUnitConverter, TimestampData timestamp) {
        return timeUnitConverter.convert(timestamp.getMillisecond(), TimeUnit.MILLISECONDS)
                + timeUnitConverter.convert(timestamp.getNanoOfMillisecond(), TimeUnit.NANOSECONDS);
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
