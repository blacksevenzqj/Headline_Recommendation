import redis
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType



class CacheFeatures(object):

    SPARK_APP_NAME = "cacheOfflineFeatures"
    SPARK_URL = "spark://192.168.199.88:7077"

    MODEL_PATH = "hdfs://localhost:9000/project1-ad-rs/models/userCateRatingALSModel.obj"

    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)

    def get_spark_conf(self):
        conf = SparkConf()  # 创建spark config对象
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", "2g"),  # 设置该app启动时占用的内存用量，默认1g
            ("spark.master", self.SPARK_URL),  # spark master的地址
            ("spark.executor.cores", "2")  # 设置spark executor使用的CPU核心数
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

    def load_ad_feature(self):
        '''从HDFS中加载广告基本信息数据'''
        _ad_feature_df = self.spark.read.csv("hdfs://localhost:9000/project1-ad-rs/datasets/ad_feature.csv", header=True)

        # 更改表结构，转换为对应的数据类型

        # 替换掉NULL字符串
        _ad_feature_df = _ad_feature_df.replace("NULL", "-1")

        # 更改df表结构：更改列类型和列名称
        ad_feature_df = _ad_feature_df. \
            withColumn("adgroup_id", _ad_feature_df.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id",
                                                                                                      "adgroupId"). \
            withColumn("cate_id", _ad_feature_df.cate_id.cast(IntegerType())).withColumnRenamed("cate_id", "cateId"). \
            withColumn("campaign_id", _ad_feature_df.campaign_id.cast(IntegerType())).withColumnRenamed("campaign_id",
                                                                                                        "campaignId"). \
            withColumn("customer", _ad_feature_df.customer.cast(IntegerType())).withColumnRenamed("customer",
                                                                                                  "customerId"). \
            withColumn("brand", _ad_feature_df.brand.cast(IntegerType())).withColumnRenamed("brand", "brandId"). \
            withColumn("price", _ad_feature_df.price.cast(FloatType()))
        return ad_feature_df

    def foreachPartition_for_ad_feature(self, partition):

        client = redis.StrictRedis(host="192.168.199.88", port=6379, db=10)

        for r in partition:
            data = {
                "price": r.price
            }
            # 转成json字符串再保存，能保证数据再次倒出来时，能有效的转换成python类型
            client.hset("ad_features", r.adgroupId, json.dumps(data))

    def load_user_profile(self):
        '''从HDFS加载用户基本信息数据'''

        # 构建表结构schema对象
        schema = StructType([
            StructField("userId", IntegerType()),
            StructField("cms_segid", IntegerType()),
            StructField("cms_group_id", IntegerType()),
            StructField("final_gender_code", IntegerType()),
            StructField("age_level", IntegerType()),
            StructField("pvalue_level", IntegerType()),
            StructField("shopping_level", IntegerType()),
            StructField("occupation", IntegerType()),
            StructField("new_user_class_level", IntegerType())
        ])
        # 利用schema从hdfs加载
        user_profile_df = self.spark.read.csv("hdfs://localhost:9000/project1-ad-rs/datasets/user_profile.csv", header=True, schema=schema)
        return user_profile_df

    def foreachPartition_for_user_profile(self, partition):
        client = redis.StrictRedis(host="192.168.199.88", port=6379, db=10)

        for r in partition:
            data = {
                "cms_group_id": r.cms_group_id,
                "final_gender_code": r.final_gender_code,
                "age_level": r.age_level,
                "shopping_level": r.shopping_level,
                "occupation": r.occupation,
                "pvalue_level": r.pvalue_level,
                "new_user_class_level": r.new_user_class_level
            }
            # 转成json字符串再保存，能保证数据再次倒出来时，能有效的转换成python类型
            client.hset("user_features", r.userId, json.dumps(data))


if __name__ == '__main__':
    obj = CacheFeatures()
    obj.initial()