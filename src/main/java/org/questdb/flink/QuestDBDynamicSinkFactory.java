package org.questdb.flink;

import io.questdb.cairo.TableUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 *
 */
public final class QuestDBDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final String FACTORY_IDENTIFIER = "questdb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig readableConfig = helper.getOptions();
        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions(), readableConfig);
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        validateSchema(physicalRowDataType);

        QuestDBConfiguration configuration = new QuestDBConfiguration(readableConfig, context.getObjectIdentifier().getObjectName());
        if (!TableUtils.isValidTableName(configuration.getTable(), Integer.MAX_VALUE)) {
            throw new ValidationException(configuration.getTable() + " is not a valid table name");
        }
        return new QuestDBDynamicTableSink(physicalRowDataType, configuration);
    }

    private static void validateSchema(DataType physicalRowDataType) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        for (RowType.RowField field : fields) {
            String fieldName = field.getName();
            if (!TableUtils.isValidColumnName(fieldName, Integer.MAX_VALUE)) {
                throw new ValidationException(fieldName + " is not a valid column name");
            }

            LogicalTypeRoot typeRoot = field.getType().getTypeRoot();
            switch (typeRoot) {
                // supported types
                case CHAR:
                case VARCHAR:
                case BOOLEAN:
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    break;

                // unsupported types
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
                    throw new ValidationException(typeRoot + " type not supported");
            }
        }
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(QuestDBConfiguration.HOST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(QuestDBConfiguration.USERNAME);
        options.add(QuestDBConfiguration.TOKEN);
        options.add(QuestDBConfiguration.TLS);
        options.add(QuestDBConfiguration.TABLE);
        options.add(QuestDBConfiguration.SINK_PARALLELISM);
        options.add(QuestDBConfiguration.BUFFER_SIZE_BYTES);
        return options;
    }
}
