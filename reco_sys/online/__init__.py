# -*- coding: UTF-8 -*-

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from setting.default import DefaultConfig
import happybase

# 1、创建spark streaming context conf
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_ONLINE_CONFIG)
sc = SparkContext(conf=conf)
stream_sc = StreamingContext(sc, 60)

# 2、配置与kafka读取的配置
# 一个点击日志⾏行为如果有多个消费者，可以设置分组，那么这个数据会备份多份
similar_kafka = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER, "group.id": 'similar'}
# 消费者
SIMILAR_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], similar_kafka)

