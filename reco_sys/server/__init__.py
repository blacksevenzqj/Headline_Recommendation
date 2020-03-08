# -*- coding: UTF-8 -*-

import happybase
from setting.default import DefaultConfig
import redis

pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)


# 召回数据
# 加上decode_responses=True，写入的键值对中的value为str类型，不加这个参数写入的则为字节类型。
redis_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=10,
                                 decode_responses=True)

# 用于缓存的Redis数据库
# 加上decode_responses=True，写入的键值对中的value为str类型，不加这个参数写入的则为字节类型。
cache_client = redis.StrictRedis(host=DefaultConfig.REDIS_HOST,
                                 port=DefaultConfig.REDIS_PORT,
                                 db=8,
                                 decode_responses=True)



from pyspark import SparkConf
from pyspark.sql import SparkSession
# spark配置
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_GRPC_CONFIG)
SORT_SPARK = SparkSession.builder.config(conf=conf).getOrCreate()
