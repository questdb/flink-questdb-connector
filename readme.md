# QuestDB Sink connector for Apache Flink
Sink data from [Apache Flink](https://flink.apache.org/) pipelines to [QuestDB](https://questdb.io/). 

The connector implements Apache Flink [Table / SQL API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/). 

## Usage with Flink SQL
_This guide assumes you are familiar with Apache Flink. Please see [Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/) to learn Flink Basics._ 
 * Save `flink-questdb-connector-<version>-SNAPSHOT.jar` in Flink `./lib/` directory
 * Start Apache Flink server
 * Start QuestDB server
 * Go to Flink SQL console and create a remote table definition:
```sql
CREATE TABLE Orders (
     order_number BIGINT,
     price        BIGINT,
     buyer        STRING
 ) WITH (
   'connector'='questdb',
   'host'='localhost'
);
```
Expected output: `[INFO] Execute statement succeed.` 
 * Still in Flink console execute: `insert into Orders values (0, 42, 'IBM');`
 * Go to QuestDB web console and run: `select * from Orders;` You should see a table being created. Chances are the table will be empty. That's caused by [QuestDB commit lag](https://questdb.io/docs/guides/out-of-order-commit-lag), the inserted row will be visible eventually. 

## Usage with Table API from Java
See a ready to use [sample projects](samples/).

## Configuration
The connector supports following Options:

| Name           | Type    | Example                                     | Default                  | Meaning                                                                   |
|----------------|---------|---------------------------------------------|--------------------------|---------------------------------------------------------------------------|
| host           | STRING  | localhost:9009                              | N/A                      | Host and port where QuestDB server is running                             |
| username       | STRING  | testUser1                                   | admin                    | Username for authentication. The default is used when also `token` is set |
| token          | STRING  | GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0 | admin                    | Token for authentication                                                  |
| table          | STRING  | my_table                                    | Same as Flink table name | Target table in QuestDB                                                   |
| tls            | BOOLEAN | true                                        | false                    | Whether to use TLS/SSL for connecting to QuestDB server                   | 
| buffer.size.kb | INTEGER | 32                                          | 64                       | Size of the QuestDB client send buffer                                    |

## FAQ
Q: Why is QuestDB client not repackaged into a different Java package?<br/>
A: QuestDB client uses native code, this makes repackaging hard.

Q: I need to use QuestDB as a source, what should I do?<br/>
A: This connector is Sink only. If you want to use QuestDB as a Source then your best chance is to use [Flink JDBC source](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/jdbc/) and rely on [QuestDB Postgres compatibility](https://questdb.io/docs/develop/query-data#postgresql-wire-protocol).

## TODO:
- Publish to Maven Central for easy consumption 