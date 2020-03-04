'''
注意注意注意：
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2
在submit时必须提供
'''
import redis
import json
import numpy as np
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


class OnlineProcessing(object):
    # spark配置信息

    SPARK_APP_NAME = "OnlineProcessing"
    SPARK_URL = "spark://192.168.199.88:7077"

    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)
        self.create_dstream_with_kafka()
        self.load_ad_feature()

    def get_spark_conf(self):
        conf = SparkConf()  # 创建spark config对象
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", "2g"),  # 设置该app启动时占用的内存用量，默认1g
            ("spark.master", self.SPARK_URL),  # spark master的地址
            ("spark.executor.cores", "2"),  # 设置spark executor使用的CPU核心数
            # 	('spark.sql.pivotMaxValues', '99999'),  # 当需要pivot DF，且值很多时，需要修改，默认是10000
        )
        # 查看更详细配置及说明：https://spark.apache.org/docs/latest/configuration.html
        #
        conf.setAll(config)
        return conf

    def get_or_create_spark(self, conf=None):
        # 利用config对象，创建spark session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    def create_dstream_with_kafka(self):
        # 注意：初次安装并运行时，由于使用了kafka，所以会自动下载一系列的依赖jar包，会耗费一定时间

        self.ssc = StreamingContext(self.spark.sparkContext, 2)

        kafkaParams = {"metadata.broker.list": "192.168.199.88:9092"}
        self.dstream = KafkaUtils.createDirectStream(self.ssc, ["mytopic"], kafkaParams)

    def load_ad_feature(self):
        #### 获取广告和类别的对应关系
        # 从HDFS中加载广告基本信息数据，返回spark dafaframe对象
        df = self.spark.read.csv("hdfs://localhost:9000/project1-ad-rs/datasets/ad_feature.csv", header=True)

        # 注意：由于本数据集中存在NULL字样的数据，无法直接设置schema，只能先将NULL类型的数据处理掉，然后进行类型转换
        # 替换掉NULL字符串，替换掉
        df = df.replace("NULL", "-1")

        # 更改df表结构：更改列类型和列名称
        ad_feature_df = df. \
            withColumn("adgroup_id", df.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id", "adgroupId"). \
            withColumn("cate_id", df.cate_id.cast(IntegerType())).withColumnRenamed("cate_id", "cateId"). \
            withColumn("campaign_id", df.campaign_id.cast(IntegerType())).withColumnRenamed("campaign_id",
                                                                                            "campaignId"). \
            withColumn("customer", df.customer.cast(IntegerType())).withColumnRenamed("customer", "customerId"). \
            withColumn("brand", df.brand.cast(IntegerType())).withColumnRenamed("brand", "brandId"). \
            withColumn("price", df.price.cast(FloatType()))

        # 这里我们只需要adgroupId、和cateId
        _ = ad_feature_df.select("adgroupId", "cateId")
        # 由于这里数据集其实很少，所以我们再直接转成Pandas dataframe来处理，把数据载入内存
        self.pdf = _.toPandas()

    @staticmethod
    def map(element):
        #日志数据格式："时间,地点,用户ID,商品ID,类别ID,品牌ID,商品价格"
        # 用逗号分割
        return element[1].split(",")

    def process_dstream(self):
        client1 = redis.StrictRedis(host="192.168.199.88", port=6379, db=10)
        client2 = redis.StrictRedis(host="192.168.199.88", port=6379, db=9)

        def foreach(rdd):
            print("foreach", rdd.collect())
            for r in rdd.collect():
                userId = r[2]    # 选出用户ID
                location_level = r[1]  # 取值范围1-4
                new_user_class_level = location_level if int(location_level) in [1, 2, 3, 4] else None
                data = json.loads(client1.hget("user_features", userId))
                data["new_user_class_level"] = new_user_class_level  # 注意：该需求知识假设的一种情况，不一定合理
                client1.hset("user_features", userId, json.dumps(data))

                cateId = r[4]    # 取出类别ID
                ad_list = self.pdf.where(self.pdf.cateId == int(cateId)).dropna().adgroupId.astype(np.int64)
                if ad_list.size > 0:
                    # 随机抽出当前类别50个广告，进行在线召回
                    ret = set(np.random.choice(ad_list, 50))
                    # 更新到redis中
                    client2.sadd(userId, *ret)

        self.dstream.map(self.map).foreachRDD(foreach)

    def start(self):
        self.ssc.start()

    def stop(self):
        self.ssc.stop()

    def join(self):
        while True:
            _ = input("正在运行(退出系统，请输入quit)...")
            if _.strip() == "quit":
                self.stop()
                break

if __name__ == '__main__':
    obj = OnlineProcessing()
    obj.initial()
    obj.process_dstream()
    obj.start()
    obj.join()

