# spark配置信息
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


class PreprocessingBehaviorLog(object):

    SPARK_APP_NAME = "preprocessingBehaviorLog"
    SPARK_URL = "spark://192.168.199.88:7077"

    HDFS_PATH = "hdfs://localhost:9000/project1-ad-rs/datasets/behavior_log.csv"
    
    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)
        self.behavior_log_df = self.create_behavior_log_df_from_csv()

    def get_spark_conf(self):

        conf = SparkConf()    # 创建spark config对象
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),    # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", "2g"),    # 设置该app启动时占用的内存用量，默认1g
            ("spark.master", self.SPARK_URL),    # spark master的地址
            ("spark.executor.cores", "2")    # 设置spark executor使用的CPU核心数
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

    def create_behavior_log_df_from_csv(self):
        # 构建结构对象
        schema = StructType([
            StructField("userId", IntegerType()),
            StructField("timestamp", LongType()),
            StructField("btag", StringType()),
            StructField("cateId", IntegerType()),
            StructField("brandId", IntegerType())
        ])
        # 从hdfs加载数据为dataframe，并设置结构
        behavior_log_df = self.spark.read.csv(self.HDFS_PATH, header=True, schema=schema)
        return behavior_log_df

    def save_cate_count(self):
        # 统计每个用户对各类商品的pv、fav、cart、buy数量
        cate_count_df = self.behavior_log_df.groupBy(self.behavior_log_df.userId, self.behavior_log_df.cateId).pivot("btag",["pv","fav","cart","buy"]).count()
        # 由于运算时间比较长，所以这里先将结果存储起来，供后续其他操作使用
        cate_count_df.write.csv("hdfs://localhost:9000/project1-ad-rs/preprocessing-datasets/cate_count.csv", header=True)

    def save_brand_count(self):
        # 统计每个用户对各个品牌的pv、fav、cart、buy数量
        brand_count_df = self.behavior_log_df.groupBy(self.behavior_log_df.userId, self.behavior_log_df.brandId).pivot("btag",["pv","fav","cart","buy"]).count()
        brand_count_df.write.csv("hdfs://localhost:9000/project1-ad-rs/preprocessing-datasets/brand_count.csv", header=True)

if __name__ == '__main__':
    obj = PreprocessingBehaviorLog()
    obj.initial()
    obj.save_cate_count()
    obj.save_brand_count()
