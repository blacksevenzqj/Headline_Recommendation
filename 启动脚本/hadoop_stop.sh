#!/bin/bash

echo "--------------YARN_STOP---------------"
/root/bigdata/hadoop/sbin/stop-yarn.sh
echo "-------------HADOOP_STOP--------------"
/root/bigdata/hadoop/sbin/stop-dfs.sh
