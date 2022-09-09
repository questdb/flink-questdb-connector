# Samples Projects

There are 2 sample projects.
## [Datagen to QuestDB](datagen-to-questdb)
Simplistic project which uses the [datagen](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/datagen/) source and directly feeds generated data to QuestDB.
## [Coinbase to Apache Kafka to Flink to QuestDB](coinbase-to-kafka-to-questdb)
This is a more advanced project using a node.js application to connect to [Coinbase API](https://docs.cloud.coinbase.com/exchange/docs) to receive latest orders and store them in Apache Kafka. There is a Flink application to read these data, filter them and store into QuestDB. Then you can run various analytics in QuestDB.   