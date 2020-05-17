import redis
import json
import pandas as pd
from pyspark.ml.linalg import DenseVector
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel


class OnlineRecommended(object):

    SPARK_APP_NAME = "OnlineRecommended"
    SPARK_URL = "spark://192.168.199.88:7077"

    MODEL_PATH = "hdfs://localhost:9000/project1-ad-rs/models/CTRModel_AllOneHot.obj"

    def inital(self):
        conf = self.get_spark_conf()
        self.spark = self.get_or_create_spark(conf)

        self.set_rela()
        self.load_model()
        self.create_redis_client()

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

    def set_rela(self):
        "pvalue_level"
        # 特征对应关系
        self.pvalue_level_rela = {-1: 0, 3:3, 1:2, 2:1}
        self.new_user_class_level_rela = {-1:0, 3:2, 1:4, 4:3, 2:1}
        self.cms_group_id_rela = {
            7: 9,
            11: 6,
            3: 0,
            8: 8,
            0: 12,
            5: 3,
            6: 10,
            9: 5,
            1: 7,
            10: 4,
            4: 1,
            12: 11,
            2: 2
        }
        self.final_gender_code_rela = {1:1, 2:0}
        self.age_level_rela = {3:0, 0:6, 5:2, 6:5, 1:4, 4:1, 2:3}
        self.shopping_level_rela = {3:0, 1:2, 2:1}
        self.occupation_rela = {0: 0, 1: 1}
        self.pid_rela = {
            "430548_1007": 0,
            "430549_1007": 1
        }

    def create_redis_client(self):

        self.client_of_recall = redis.StrictRedis(host="192.168.199.88", port=6379, db=9)
        self.client_of_features = redis.StrictRedis(host="192.168.199.88", port=6379, db=10)

    def create_datasets(self, userId, pid):

        # 获取用户特征
        user_feature = json.loads(self.client_of_features.hget("user_features", userId))

        # 获取用户召回集
        recall_sets = self.client_of_recall.smembers(userId)

        result = []

        # 遍历召回集
        for adgroupId in recall_sets:
            adgroupId = int(adgroupId)
            # 获取该广告的特征值
            ad_feature = json.loads(self.client_of_features.hget("ad_features", adgroupId))

            features = {}
            features.update(user_feature)
            features.update(ad_feature)

            for k, v in features.items():
                if v is None:
                    features[k] = -1

            features_col = [
                # 特征值
                "price",
                "cms_group_id",
                "final_gender_code",
                "age_level",
                "shopping_level",
                "occupation",
                "pid",
                "pvalue_level",
                "new_user_class_level"
            ]
            '''
            "cms_group_id", 类别型特征，约13个分类 ==> 13维
            "final_gender_code", 类别型特征，2个分类 ==> 2维
            "age_level", 类别型特征，7个分类 ==>7维
            "shopping_level", 类别型特征，3个分类 ==> 3维
            "occupation", 类别型特征，2个分类 ==> 2维
            '''

            price = float(features["price"])

            pid_value = [0 for i in range(2)]
            cms_group_id_value = [0 for i in range(13)]
            final_gender_code_value = [0 for i in range(2)]
            age_level_value = [0 for i in range(7)]
            shopping_level_value = [0 for i in range(3)]
            occupation_value = [0 for i in range(2)]
            pvalue_level_value = [0 for i in range(4)]
            new_user_class_level_value = [0 for i in range(5)]

            pid_value[self.pid_rela[pid]] = 1
            cms_group_id_value[self.cms_group_id_rela[int(features["cms_group_id"])]] = 1
            final_gender_code_value[self.final_gender_code_rela[int(features["final_gender_code"])]] = 1
            age_level_value[self.age_level_rela[int(features["age_level"])]] = 1
            shopping_level_value[self.shopping_level_rela[int(features["shopping_level"])]] = 1
            occupation_value[self.occupation_rela[int(features["occupation"])]] = 1
            pvalue_level_value[self.pvalue_level_rela[int(features["pvalue_level"])]] = 1
            new_user_class_level_value[self.new_user_class_level_rela[int(features["new_user_class_level"])]] = 1

            #         print(pid_value)
            #         print(cms_group_id_value)
            #         print(final_gender_code_value)
            #         print(age_level_value)
            #         print(shopping_level_value)
            #         print(occupation_value)
            #         print(pvalue_level_value)
            #         print(new_user_class_level_value)

            vector = DenseVector([price] + pid_value + cms_group_id_value + final_gender_code_value \
                                 + age_level_value + shopping_level_value + occupation_value + pvalue_level_value + new_user_class_level_value)

            result.append((userId, adgroupId, vector))

        return result

    def load_model(self):
        # 载入训练好的模型
        self.CTR_model = LogisticRegressionModel.load(self.MODEL_PATH)

    def handle_request(self, userId, pid, n=10):
        if pid not in ["430548_1007", "430539_1007"]:
            raise Exception("Invalid pid value! It should be one of the: 430548_1007, 430539_1007")

        pdf = pd.DataFrame(self.create_datasets(userId, pid), columns=["userId", "adgroupId", "features"])
        datasets = self.spark.createDataFrame(pdf)
        prediction = self.CTR_model.transform(datasets).sort("probability")
        return [i.adgroupId for i in prediction.select("adgroupId").head(n)]

if __name__ == '__main__':
    print("推荐系统正在启动...")
    obj = OnlineRecommended()
    obj.inital()
    print("推荐系统启动成功.")
    while True:
        userId = input("请输入用户ID: ")

        _pid = input("请输入广告资源位(输入1(默认): 430548_1007 | 输入2: 430539_1007): ")
        if _pid.strip() == "1":
            pid = "430548_1007"
        elif _pid.strip() == "2":
            pid = "430539_1007"
        else:
            pid = "430548_1007"

        n = input("请输入需要推荐的广告个数(默认10，最大500): ")
        if n.strip() == "":
            n = 10
        if int(n) > 500:
            n = 500

        ret = obj.handle_request(int(userId), pid, int(n))
        print("给用户%s推荐的广告ID列表为："%userId, ret)

        _ = input("继续请输入1，否则输入其他任意键退出：")
        if _.strip() != "1":
            break
