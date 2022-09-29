# Sample project with Flink QuestDB connector
## What does the sample project do?
The [project](flink-questdb-connector/samples/datagen-to-questdb/src/main/java/io/questdb/flink/DatagenToQuestDB.java) shows how to use the QuestDB connector from [Flink Table API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/overview/). It creates a [datagen](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/datagen/) source generating 100 random rows per second and then feeds the generated rows into [QuestDB](https://questdb.io). 

The application is packaged as Docker containers thus it can run locally on most platforms. 

## Prerequisites:
- Git
- Working Docker environment, including docker-compose
- Internet access to download dependencies

The project was tested on MacOS with M1, but it should work on other platforms too. Please [open a new issue](https://github.com/questdb/flink-questdb-connector/issues/new) if it's not working for you. 

## Usage:
- Clone this repository via `git clone https://github.com/questdb/flink-questdb-connector.git`
- `cd flink-questdb-connector/sample` to enter the directory with this sample.
- Run `docker-compose build` to build a docker image with the sample project.
- Run `docker-compose up` to start both Flink and QuestDB containers.
- The previous command will generate a lot of log messages. Eventually logging should cease. This means both Apache Flink and QuestDB are running. 
- Go to the [Flink console](http://localhost:8082/#/job/running) and you should see one job running.
- Go to the [QuestDB console](http://localhost:19000) and run `select * from from_flink` and you should see some rows.
- Congratulations, the connector is working!
- You can play with the [sample application](flink-questdb-connector/samples/datagen-to-questdb/src/main/java/io/questdb/flink/DatagenToQuestDB.java) source code to change behaviour. See [Flink Table API documentation](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/tableapi/) for more information. 