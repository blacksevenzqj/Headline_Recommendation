#!/bin/bash

echo "--------------KAFKA_ZOOKEEPER_START---------------"
/root/bigdata/kafka/bin/zookeeper-server-start.sh -daemon /root/bigdata/kafka/config/zookeeper.properties
echo "--------------KAFKA_SERVER_START----------------"
/root/bigdata/kafka/bin/kafka-server-start.sh /root/bigdata/kafka/config/server.properties