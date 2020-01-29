#!/bin/bash

echo "--------------HADOOP_START---------------"
/root/bigdata/hadoop/sbin/start-dfs.sh
echo "--------------YARN_START-----------------"
/root/bigdata/hadoop/sbin/start-yarn.sh