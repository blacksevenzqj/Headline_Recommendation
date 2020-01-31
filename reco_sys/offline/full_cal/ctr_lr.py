# -*- coding: UTF-8 -*-

import os
import sys

'''
spark-submit \
--master local \
/root/toutiao_project/reco_sys/offline/full_cal/ctr_lr.py
或
python3 ctr_lr.py
'''

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, "reco_sys"))
print(sys.path)

PYSPARK_PYTHON = "/miniconda2/envs/py365/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from offline import SparkSessionBase

class CtrLogisticRegression(SparkSessionBase):

    SPARK_APP_NAME = "ctrLogisticRegression"
    SPARK_URL = "local"
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):

        self.spark = self._create_spark_hbase()

ctr = CtrLogisticRegression()



# (1)、用户行为日志数据读取
ctr.spark.sql('use profile')
user_article_basic = ctr.spark.sql("select * from user_article_basic").select(['user_id', 'article_id', 'clicked'])
user_article_basic.show()

# (2)、用户画像读取处理 与 日志数据 合并
user_profile_hbase = ctr.spark.sql("select user_id, information.gender, information.birthday, article_partial from user_profile_hbase")
user_profile_hbase.show()
