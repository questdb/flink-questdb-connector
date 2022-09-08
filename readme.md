# QuestDB Sink connector for Apache Flink
Sink data from [Apache Flink](https://flink.apache.org/) pipelines to [QuestDB](https://questdb.io/). 

The connector implements Apache Flink [Table / SQL API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/). 

## Installation
Save `flink-questdb-connector-0.1-SNAPSHOT.jar` into your Flink installation `lib` directory, e.g. `flink-1.15.2/lib`

## Usage with Flink SQL
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
See a ready to use [sample project](sample/readme.md).

## Configuration
TBD

## FAQ
Q: Why is QuestDB client not repackaged into a different Java package?<br/>
A: QuestDB client uses native code, this makes repackaging hard. 