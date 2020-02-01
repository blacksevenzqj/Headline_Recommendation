#!/bin/bash

echo "--------------KAFKA_SERVER_STOP----------------"
/root/bigdata/kafka/bin/kafka-server-stop.sh
echo "--------------KAFKA_ZOOKEEPER_STOP---------------"
/root/bigdata/kafka/bin/zookeeper-server-stop.sh
