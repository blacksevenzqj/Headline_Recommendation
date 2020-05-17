# spark配置信息
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, StringType
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler


class PreprocessingData(object):
    SPARK_APP_NAME = "createCTRModelByLR"
    SPARK_URL = "spark://192.168.199.88:7077"

    RAW_SAMPLE_PATH = "hdfs://localhost:9000/project1-ad-rs/datasets/raw_sample.csv"
    AD_FEATURE_PATH = "hdfs://localhost:9000/project1-ad-rs/datasets/ad_feature.csv"
    USER_PROFILE_PATH = "hdfs://localhost:9000/project1-ad-rs/datasets/user_profile.csv"

    def initial(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)
        self.process_onthot_field()

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

    def load_raw_sample(self):
        '''从HDFS中加载样本数据信息'''
        _raw_sample_df1 = self.spark.read.csv(self.RAW_SAMPLE_PATH, header=True)

        # 更改df表结构：更改列类型和列名称
        _raw_sample_df2 = _raw_sample_df1. \
            withColumn("user", _raw_sample_df1.user.cast(IntegerType())).withColumnRenamed("user", "userId"). \
            withColumn("time_stamp", _raw_sample_df1.time_stamp.cast(LongType())).withColumnRenamed("time_stamp",
                                                                                                    "timestamp"). \
            withColumn("adgroup_id", _raw_sample_df1.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id",
                                                                                                       "adgroupId"). \
            withColumn("pid", _raw_sample_df1.pid.cast(StringType())). \
            withColumn("nonclk", _raw_sample_df1.nonclk.cast(IntegerType())). \
            withColumn("clk", _raw_sample_df1.clk.cast(IntegerType()))
        stringindexer = StringIndexer(inputCol='pid', outputCol='pid_feature')
        encoder = OneHotEncoder(dropLast=False, inputCol='pid_feature', outputCol='pid_value')
        pipeline = Pipeline(stages=[stringindexer, encoder])
        pipeline_fit = pipeline.fit(_raw_sample_df2)
        raw_sample_df = pipeline_fit.transform(_raw_sample_df2)
        return raw_sample_df

    def load_ad_feature(self):
        '''从HDFS中加载广告基本信息数据'''
        _ad_feature_df = self.spark.read.csv(self.AD_FEATURE_PATH, header=True)

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
        _user_profile_df1 = self.spark.read.csv(self.USER_PROFILE_PATH, header=True, schema=schema)

        '''对缺失数据进行特征热编码'''

        # 使用热编码转换pvalue_level的一维数据为多维，增加n-1个虚拟变量，n为pvalue_level的取值范围
        # 需要先将缺失值全部替换为数值，便于处理，否则会抛出异常
        _user_profile_df2 = _user_profile_df1.na.fill(-1)

        # 热编码时，必须先将待处理字段转为字符串类型才可处理
        _user_profile_df3 = _user_profile_df2.withColumn("pvalue_level",\
                                                         _user_profile_df2.pvalue_level.cast(StringType())) \
            .withColumn("new_user_class_level", _user_profile_df2.new_user_class_level.cast(StringType()))

        # 对pvalue_level进行热编码，求值
        # 运行过程是先将pvalue_level转换为一列新的特征数据，然后对该特征数据求出的热编码值，存在了新的一列数据中，类型为一个稀疏矩阵
        stringindexer = StringIndexer(inputCol='pvalue_level', outputCol='pl_onehot_feature')
        encoder = OneHotEncoder(dropLast=False, inputCol='pl_onehot_feature', outputCol='pl_onehot_value')
        pipeline = Pipeline(stages=[stringindexer, encoder])
        pipeline_fit = pipeline.fit(_user_profile_df3)
        _user_profile_df4 = pipeline_fit.transform(_user_profile_df3)
        # pl_onehot_value列的值为稀疏矩阵，存储热编码的结果

        # 使用热编码转换new_user_class_level的一维数据为多维
        stringindexer = StringIndexer(inputCol='new_user_class_level', outputCol='nucl_onehot_feature')
        encoder = OneHotEncoder(dropLast=False, inputCol='nucl_onehot_feature', outputCol='nucl_onehot_value')
        pipeline = Pipeline(stages=[stringindexer, encoder])
        pipeline_fit = pipeline.fit(_user_profile_df4)
        user_profile_df = pipeline_fit.transform(_user_profile_df4)
        user_profile_df.show()
        return user_profile_df

    def join_datasets(self):
        raw_sample_df = self.load_raw_sample()
        ad_feature_df = self.load_ad_feature()
        user_profile_df = self.load_user_profile()

        # raw_sample_df和ad_feature_df合并条件
        condition = [raw_sample_df.adgroupId == ad_feature_df.adgroupId]
        _ = raw_sample_df.join(ad_feature_df, condition, 'outer')

        # _和user_profile_df合并条件
        condition2 = [_.userId == user_profile_df.userId]
        datasets = _.join(user_profile_df, condition2, "outer")

        return datasets

    # 热编码处理函数封装
    def oneHotEncoder(self, col1, col2, col3, data):
        stringindexer = StringIndexer(inputCol=col1, outputCol=col2)
        encoder = OneHotEncoder(dropLast=False, inputCol=col2, outputCol=col3)
        pipeline = Pipeline(stages=[stringindexer, encoder])
        pipeline_fit = pipeline.fit(data)
        return pipeline_fit.transform(data)

    def process_onthot_field(self):

        datasets = self.join_datasets()

        datasets_2 = datasets.withColumn("cms_group_id", datasets.cms_group_id.cast(StringType())) \
            .withColumn("final_gender_code", datasets.final_gender_code.cast(StringType())) \
            .withColumn("age_level", datasets.age_level.cast(StringType())) \
            .withColumn("shopping_level", datasets.shopping_level.cast(StringType())) \
            .withColumn("occupation", datasets.occupation.cast(StringType()))

        useful_cols_2 = [
            # 时间值，划分训练集和测试集
            "timestamp",
            # label目标值
            "clk",
            # 特征值
            "price",
            "cms_group_id",
            "final_gender_code",
            "age_level",
            "shopping_level",
            "occupation",
            "pid_value",
            "pl_onehot_value",
            "nucl_onehot_value"
        ]
        # 筛选指定字段数据
        datasets_2 = datasets_2.select(*useful_cols_2)
        # 由于前面使用的是outer方式合并的数据，产生了部分空值数据，这里必须先剔除掉
        datasets_2 = datasets_2.dropna()

        # 对这五个字段进行热独编码
        #     "cms_group_id",
        #     "final_gender_code",
        #     "age_level",
        #     "shopping_level",
        #     "occupation",
        datasets_2 = self.oneHotEncoder("cms_group_id", "cms_group_id_feature", "cms_group_id_value", datasets_2)
        datasets_2 = self.oneHotEncoder("final_gender_code", "final_gender_code_feature", "final_gender_code_value",
                                   datasets_2)
        datasets_2 = self.oneHotEncoder("age_level", "age_level_feature", "age_level_value", datasets_2)
        datasets_2 = self.oneHotEncoder("shopping_level", "shopping_level_feature", "shopping_level_value", datasets_2)
        datasets_2 = self.oneHotEncoder("occupation", "occupation_feature", "occupation_value", datasets_2)
        self._datasets = datasets_2

    def split_datasets(self):
        # 由于热独编码后，特征字段不再是之前的字段，重新定义特征值字段
        feature_cols = [
            # 特征值
            "price",
            "cms_group_id_value",
            "final_gender_code_value",
            "age_level_value",
            "shopping_level_value",
            "occupation_value",
            "pid_value",
            "pl_onehot_value",
            "nucl_onehot_value"
        ]

        datasets = VectorAssembler().setInputCols(feature_cols).setOutputCol("features").transform(self._datasets)
        self._train_datasets = datasets.filter(datasets.timestamp <= (1494691186 - 24 * 60 * 60))
        self._test_datasets = datasets.filter(datasets.timestamp > (1494691186 - 24 * 60 * 60))

    @property
    def datasets(self):
        return self._datasets

    @property
    def train_datasets(self):
        return self._train_datasets

    @property
    def test_datasets(self):
        return self._test_datasets

