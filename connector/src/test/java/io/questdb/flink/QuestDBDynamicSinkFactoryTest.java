package io.questdb.flink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.time.LocalDateTime;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.row;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(TestLoggerExtension.class)
@Testcontainers
public class QuestDBDynamicSinkFactoryTest {
    private static final boolean USE_LOCAL_QUEST = false;
    private static final int QUERY_WAITING_TIME_SECONDS = 30;

    private static final int ILP_PORT = 9009;
    private static final int HTTP_PORT = 9000;

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();
    private String testName;

    @AfterAll
    public static void classTearDown() throws IOException {
        HTTP_CLIENT.close();
    }

    @Container
    public static GenericContainer<?> questdb = new GenericContainer<>(DockerImageName.parse("questdb/questdb:7.1.1"))
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI") // this makes QuestDB container much faster to start
            .withExposedPorts(ILP_PORT, HTTP_PORT)
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));

    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        this.testName = testInfo.getTestMethod().orElseThrow(() -> new IllegalStateException("test name unknown")).getName();
        if (questdb.isRunning()) {
            executeQuery("drop table " + testName).close();
        }
    }

    @Test
    public void testSmoke() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL,"
                        + "h as a + 2\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
                        + String.format("'%s'='%s'\n", "table", testName)
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalDateTime.of(2022, 6, 1, 10, 10, 10, 10_000),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("questTable")
                .await();

        assertSqlEventually("{\"query\":\"select a, b, c, d, e, f, g from testSmoke\",\"columns\":[{\"name\":\"a\",\"type\":\"LONG\"},{\"name\":\"b\",\"type\":\"TIMESTAMP\"},{\"name\":\"c\",\"type\":\"STRING\"},{\"name\":\"d\",\"type\":\"DOUBLE\"},{\"name\":\"e\",\"type\":\"LONG\"},{\"name\":\"f\",\"type\":\"TIMESTAMP\"},{\"name\":\"g\",\"type\":\"TIMESTAMP\"}],\"dataset\":[[1,\"2022-06-01T10:10:10.000010Z\",\"ABCDE\",12.119999885559,2,\"2003-10-20T00:00:00.000000Z\",\"2012-12-12T12:12:12.000000Z\"]],\"timestamp\":-1,\"count\":1}",
                "select a, b, c, d, e, f, g from " + testName);
    }

    private static Map<String, String> getSinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "questdb");
        options.put("host", "whatever");
        return options;
    }

    @Test
    public void testUnsupportedTypes() {
        assertTypeNotSupported(DataTypes.BINARY(10));
        assertTypeNotSupported(DataTypes.INTERVAL(DataTypes.SECOND(3)));
        assertTypeNotSupported(DataTypes.ARRAY(DataTypes.INT()));
        assertTypeNotSupported(DataTypes.ROW());
        assertTypeNotSupported(DataTypes.MULTISET(DataTypes.INT()));
        assertTypeNotSupported(DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        assertTypeNotSupported(DataTypes.VARBINARY(100));
    }

    @Test
    public void testInvalidColumnName() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a*", DataTypes.INT()));
        try {
            FactoryMocks.createTableSink(schema, getSinkOptions());
            fail("not supported");
        } catch (ValidationException expected) { }
    }

    @Test
    public void testInvalidTableName() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", DataTypes.INT()));
        Map<String, String> sinkOptions = getSinkOptions();
        sinkOptions.put(QuestDBConfiguration.TABLE.key(), "invalid*table*name");
        try {
            FactoryMocks.createTableSink(schema, sinkOptions);
            fail("not supported");
        } catch (ValidationException expected) { }
    }

    @Test
    public void testSinkIsSerializable() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", DataTypes.BIGINT()));
        DynamicTableSink dynamicTableSink = FactoryMocks.createTableSink(schema, getSinkOptions());
        DynamicTableSink.SinkRuntimeProvider provider =
                dynamicTableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        SinkV2Provider sinkProvider = (SinkV2Provider) provider;
        Sink<RowData> sink = sinkProvider.createSink();
        InstantiationUtil.isSerializable(sink);
    }

    @Test
    public void isSerializable() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", DataTypes.BIGINT()));
        DynamicTableSink dynamicTableSink = FactoryMocks.createTableSink(schema, getSinkOptions());
        DynamicTableSink.SinkRuntimeProvider provider =
                dynamicTableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        SinkV2Provider sinkProvider = (SinkV2Provider) provider;
        Sink<RowData> sink = sinkProvider.createSink();
        InstantiationUtil.isSerializable(sink);
    }

    private static void assertTypeNotSupported(DataType dataType) {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("a", dataType));
        try {
            FactoryMocks.createTableSink(schema, getSinkOptions());
            fail("not supported");
        } catch (ValidationException expected) { }
    }

    @Test
    public void testEventTime() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnvironment.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");
        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "col_varchar VARCHAR,\n"
                        + "col_integer INTEGER,\n"
                        + "col_timestamp TIMESTAMP(3)\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
                        + String.format("'%s'='%s'\n", "table", testName)
                        + ")").await();

        tableEnvironment.executeSql(
                "CREATE TABLE datagen ("
                        + "col_varchar VARCHAR,\n"
                        + "col_integer INTEGER,\n"
                        + "col_timestamp TIMESTAMP(3)\n,"
                        + "WATERMARK FOR col_timestamp AS col_timestamp\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "datagen")
                        + String.format("'%s'='%s'\n", "number-of-rows", "10")
                        + ")").await();

        tableEnvironment.executeSql(
                "INSERT INTO questTable select * from datagen").await();

        assertSqlEventually("{\"query\":\"select count(*) from testEventTime where col_timestamp = timestamp\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[10]],\"timestamp\":-1,\"count\":1}",
                "select count(*) from " + testName + " where col_timestamp = timestamp");
    }

    @Test
    public void testPrecreatedTableDesignatedTimestamp() throws Exception {
        assertSql("{\"ddl\":\"OK\"}", "CREATE TABLE " + testName + " (timestamp timestamp, name string) timestamp(timestamp) partition by DAY");

        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnvironment.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");
        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "`timestamp` TIMESTAMP(3),\n"
                        + "name VARCHAR\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
                        + String.format("'%s'='%s',\n", "timestamp.field.name", "timestamp")
                        + String.format("'%s'='%s'\n", "table", testName)
                        + ")").await();

        tableEnvironment.executeSql(
                "CREATE TABLE datagen ("
                        + "`timestamp` timestamp(3),\n"
                        + "name varchar\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "datagen")
                        + String.format("'%s'='%s'\n", "number-of-rows", "10")
                        + ")").await();

        tableEnvironment.executeSql(
                "INSERT INTO questTable select * from datagen").await();

        assertSqlEventually("{\"query\":\"select count(*) from testPrecreatedTableDesignatedTimestamp where timestamp > '1980-01'\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[10]],\"timestamp\":-1,\"count\":1}",
                "select count(*) from " + testName + " where timestamp > '1980-01'");
    }

    @Test
    public void testAllSupportedTypes() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");

        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "a CHAR,\n"
                        + "b VARCHAR,\n"
                        + "c STRING,\n"
                        + "d BOOLEAN,\n"
                        + "h DECIMAL,\n"
                        + "i TINYINT,\n"
                        + "j SMALLINT,\n"
                        + "k INTEGER,\n"
                        + "l BIGINT,\n"
                        + "m FLOAT,\n"
                        + "n DOUBLE,\n"
                        + "o DATE,"
                        + "p TIME,"
                        + "q TIMESTAMP,"
                        + "r TIMESTAMP_LTZ"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
                        + String.format("'%s'='%s'\n", "table", testName)
                        + ")");

        tableEnvironment
                .fromValues(
                        row("c",
                                "varchar",
                                "string",
                                true,
                                42,
                                (byte)42,
                                (short)42,
                                42,
                                10_000_000_000L,
                                42.42f,
                                42.42,
                                LocalDate.of(2022, 6, 6),
                                LocalTime.of(12, 12),
                                LocalDateTime.of(2022, 9, 3, 12, 12, 12),
                                LocalDateTime.of(2022, 9, 3, 12, 12, 12)
                        )
                ).executeInsert("questTable")
                .await();

        assertSqlEventually("{\"query\":\"select a, b, c, d, h, i, j, k, l, m, n, o, p, q, r from testAllSupportedTypes\"," +
                        "\"columns\":[" +
                            "{\"name\":\"a\",\"type\":\"STRING\"}," +
                            "{\"name\":\"b\",\"type\":\"STRING\"}," +
                            "{\"name\":\"c\",\"type\":\"STRING\"}," +
                            "{\"name\":\"d\",\"type\":\"BOOLEAN\"}," +
                            "{\"name\":\"h\",\"type\":\"LONG\"}," +
                            "{\"name\":\"i\",\"type\":\"LONG\"}," +
                            "{\"name\":\"j\",\"type\":\"LONG\"}," +
                            "{\"name\":\"k\",\"type\":\"LONG\"}," +
                            "{\"name\":\"l\",\"type\":\"LONG\"}," +
                            "{\"name\":\"m\",\"type\":\"DOUBLE\"}," +
                            "{\"name\":\"n\",\"type\":\"DOUBLE\"}," +
                            "{\"name\":\"o\",\"type\":\"TIMESTAMP\"}," +
                            "{\"name\":\"p\",\"type\":\"LONG\"}," +
                            "{\"name\":\"q\",\"type\":\"TIMESTAMP\"}," +
                            "{\"name\":\"r\",\"type\":\"TIMESTAMP\"}" +
                        "],\"dataset\":[" +
                            "[\"c\",\"varchar\",\"string\",true,42,42,42,42,10000000000,42.419998168945,42.42,\"2022-06-06T00:00:00.000000Z\",43920000,\"2022-09-03T12:12:12.000000Z\",\"2022-09-03T12:12:12.000000Z\"]" +
                        "],\"timestamp\":-1,\"count\":1}",
                "select a, b, c, d, h, i, j, k, l, m, n, o, p, q, r from " + testName);
    }

    private void assertSqlEventually(String expectedResult, String query) {
        await().atMost(QUERY_WAITING_TIME_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertSql(expectedResult, query));
    }

    private void assertSql(String expectedResult, String query) throws IOException {
        try (CloseableHttpResponse response = executeQuery(query)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String s = EntityUtils.toString(entity);
                assertEquals(expectedResult, s);
            } else {
                fail("no response");
            }
        }
    }

    private CloseableHttpResponse executeQuery(String query) throws IOException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        HttpGet httpGet = new HttpGet(String.format("http://%s:%d//exec?query=%s", questdb.getHost(), questdb.getMappedPort(HTTP_PORT), encodedQuery));
        return HTTP_CLIENT.execute(httpGet);
    }

    private String getIlpHostAndPort() {
        if (USE_LOCAL_QUEST) {
            return "localhost:9009";
        }
        return questdb.getHost() + ":"  + questdb.getMappedPort(ILP_PORT);
    }

}
