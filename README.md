# getToKafkaFromWebsocket
the Code push data to Kafka, after it get data from WebSocket

create Topic;

bin/kafka-topics.sh -–zookeeper localhost:2181 –-create –-topic ws_coinBase -–partitions 1 -–replication-factor 1 -–config retention.minutes=1
