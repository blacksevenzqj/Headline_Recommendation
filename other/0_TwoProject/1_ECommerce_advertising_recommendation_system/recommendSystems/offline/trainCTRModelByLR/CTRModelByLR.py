# spark配置信息
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel

class CTRModelByLR(object):
    SPARK_APP_NAME = "createCTRModelByLR"
    SPARK_URL = "spark://192.168.199.88:7077"
    MODEL_PATH = "hdfs://localhost:9000/project1-ad-rs/models/CTRModel_AllOneHot.obj"

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

    def train(self, train_datasets):
        # 创建逻辑回归训练器，并训练模型

        lr = LogisticRegression()
        model = lr.setLabelCol("clk").setFeaturesCol("features").fit(train_datasets)
        model.save(self.MODEL_PATH)
        return model

    def test(self, test_datasets):
        # 载入训练好的模型
        model = LogisticRegressionModel.load(self.MODEL_PATH)
        result = model.transform(test_datasets)
        return result
