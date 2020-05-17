# spark配置信息
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType


class UserCateRatingALSModel(object):

    SPARK_APP_NAME = "createUserBrandRatingALSModel"
    SPARK_URL = "spark://192.168.199.88:7077"
    HDFS_PATH = "hdfs://localhost:9000/project1-ad-rs/preprocessing-datasets/brand_count.csv"
    MODEL_PATH = "hdfs://localhost:9000/project1-ad-rs/models/userCateRatingALSModel.obj"

    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)
        self.user_brand_count_df = self.get_user_cate_count_df()

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
        conf.setAll(config)
        return conf

    def get_or_create_spark(self, conf=None):
        # 利用config对象，创建spark session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    def get_user_cate_count_df(self):
        schema = StructType([
            StructField("userId", IntegerType()),
            StructField("brandId", IntegerType()),
            StructField("pv", IntegerType()),
            StructField("fav", IntegerType()),
            StructField("cart", IntegerType()),
            StructField("buy", IntegerType())
        ])
        # 从hdfs加载预处理好的品牌的统计数据
        df = self.spark.read.csv(self.HDFS_PATH, header=True, schema=schema)
        return df

    def process_row(self, r):
        # 处理每一行数据：r表示row对象

        # 偏好评分规则：
        #     m: 用户对应的行为次数
        #     该偏好权重比例，次数上限仅供参考，具体数值应根据产品业务场景权衡
        #     pv: if m<=20: score=0.2*m; else score=4
        #     fav: if m<=20: score=0.4*m; else score=8
        #     cart: if m<=20: score=0.6*m; else score=12
        #     buy: if m<=20: score=1*m; else score=20

        # 注意这里要全部设为浮点数，spark运算时对类型比较敏感，要保持数据类型都一致
        pv_count = r.pv if r.pv else 0.0
        fav_count = r.fav if r.fav else 0.0
        cart_count = r.cart if r.cart else 0.0
        buy_count = r.buy if r.buy else 0.0

        pv_score = 0.2 * pv_count if pv_count <= 20 else 4.0
        fav_score = 0.4 * fav_count if fav_count <= 20 else 8.0
        cart_score = 0.6 * cart_count if cart_count <= 20 else 12.0
        buy_score = 1.0 * buy_count if buy_count <= 20 else 20.0

        rating = pv_score + fav_score + cart_score + buy_score
        # 返回用户ID、品牌ID、用户对品牌的偏好打分
        return r.userId, r.brandId, rating

    def create_user_brand_rating_df(self):
        # 用户对品牌的打分数据
        self.user_brand_rating_df = self.user_brand_count_df.rdd.map(self.process_row).toDF(["userId", "brandId", "rating"])

    def get_user_brand_rating_matrix(self):
        # 可通过该方法获得 user-brand-matrix
        # 但由于brandId字段过多，这里运算量比很大，机器内存要求很高才能执行，否则无法完成任务
        # 请谨慎使用

        # 但好在我们训练ALS模型时，不需要转换为user-cate-matrix，所以这里可以不用运行
        self.user_brand_rating_df.groupBy("userId").povit("brandId").min("rating")

    def train_model_and_save(self, user_brand_rating_df):
        # 注意注意注意：由于历史数据中brand数据量太多，这里训练量很大
        '''训练模型并存储'''
        # 使用pyspark中的ALS矩阵分解方法实现CF评分预测
        # 文档地址：https://spark.apache.org/docs/2.2.2/api/python/pyspark.ml.html?highlight=vectors#module-pyspark.ml.recommendation

        # 利用打分数据，训练ALS模型
        als = ALS(userCol='userId', itemCol='brandId', ratingCol='rating', checkpointInterval=2)
        # 此处训练时间较长
        model = als.fit(user_brand_rating_df)
        # 保存模型
        model.save(self.MODEL_PATH)
        # 返回模型
        return model

    def load_model(self):
        '''加载模型'''
        # 从hdfs加载之前存储的模型
        als_model = ALSModel.load(self.MODEL_PATH)
        return als_model


