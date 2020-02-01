#!/bin/bash

./docker_start.sh
./hadoop_start.sh
./hbase_start.sh
./hbase_thrift_start.sh
./hive_metastore_start.sh
./kafka_start.sh