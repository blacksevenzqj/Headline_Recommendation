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

# 2.1、消费者（用户点击日志）
SIMILAR_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], similar_kafka)

# 2.2、配置HOT文章读取配置
kafka_params = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
HOT_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], kafka_params)

# 2.3、配置新文章的读取KAFKA配置
click_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
NEW_ARTICLE_DS = KafkaUtils.createDirectStream(stream_sc, ['new-article'], click_kafkaParams)
