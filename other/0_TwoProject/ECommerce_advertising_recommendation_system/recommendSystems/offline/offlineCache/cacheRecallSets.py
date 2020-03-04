# spark配置信息
import numpy as np
import pandas as pd
import redis
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel


class CacheRecallSets(object):

    SPARK_APP_NAME = "recallAdSets"
    SPARK_URL = "spark://192.168.199.88:7077"

    MODEL_PATH = "hdfs://localhost:9000/project1-ad-rs/models/userCateRatingALSModel.obj"

    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)
        self.load_ad_feature()
        self.load_model()

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

    def load_model(self):
        # 利用ALS模型进行类别的召回

        # 加载als模型，注意必须先有spark上下文管理器，即sparkContext，但这里sparkSession创建后，自动创建了sparkContext
        # 从hdfs加载之前存储的模型
        self.als_model = ALSModel.load(self.MODEL_PATH)

    def cache(self):

        # 存储用户召回，使用redis第9号数据库，类型：sets类型
        client = redis.StrictRedis(host="192.168.199.88", port=6379, db=9)

        for r in self.als_model.userFactors.select("id").collect():

            userId = r.id
            cateId_df = pd.DataFrame(np.array(list(set(self.pdf.cateId))).reshape(6769, 1), columns=["cateId"])
            cateId_df.insert(0, "userId", np.array([userId for i in range(6769)]))
            ret = set()

            # 利用模型，传入datasets(userId, cateId)，这里控制了userId一样，所以相当于是在求某用户对所有分类的兴趣程度
            cateId_list = self.als_model.transform(self.spark.createDataFrame(cateId_df)).sort("prediction", ascending=False).na.drop()
            # 从前20个分类中选出500个进行召回
            for i in cateId_list.head(20):
                need = 500 - len(ret)  # 如果不足500个，那么随机选出need个广告
                ret = ret.union(
                    np.random.choice(self.pdf.where(self.pdf.cateId == i.cateId).adgroupId.dropna().astype(np.int64), need))
                if len(ret) >= 500:  # 如果达到500个则退出
                    break
            client.sadd(userId, *ret)

        # 如果redis所在机器，内存不足，会抛出异常

if __name__ == '__main__':
    obj = CacheRecallSets()
    obj.initial()
    obj.cache()
