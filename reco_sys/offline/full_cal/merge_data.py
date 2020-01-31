# -*- coding: UTF-8 -*-

import os
import sys

'''
文章标题 + 文章频道名称 + 文章内容 = 组成文章完整内容

spark-submit \
--master local \
/root/toutiao_project/reco_sys/offline/full_cal/merge_data.py
'''

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
print(BASE_DIR)
print(sys.path)

PYSPARK_PYTHON = "/miniconda2/envs/py365/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
print(os.environ["PYSPARK_PYTHON"])

from offline import SparkSessionBase

print("Python Version {}".format(str(sys.version).replace('\n', '')))

# In[]:
class OriginArticleData(SparkSessionBase):
    SPARK_APP_NAME = "mergeArticle"  # 可通过spark-submit命令在脚本中指定，但如果使用python执行则需在代码中指定。
    SPARK_URL = "local" # 重写  # 可通过spark-submit命令在脚本中指定，但如果使用python执行则需在代码中指定。
    SPARK_EXECUTOR_MEMORY = "1g"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


# In[]:
oa = OriginArticleData()
# In[]:
oa.spark.sql("use toutiao")
# 由于运行速度原因，选择一篇文章部分数据进行测试（news_article_content数据有问题，article_id全为NULL）
basic_content = oa.spark.sql(
    "select a.article_id, a.channel_id, a.title, b.content from news_article_basic a inner join news_article_content b on a.article_id=b.article_id where a.article_id=141469")

# In[]:
basic_content.show()


import pyspark.sql.functions as F
import gc

# 增加channel的名字，后面会使用
basic_content.registerTempTable("temparticle")
channel_basic_content = oa.spark.sql(
  "select t.*, n.channel_name from temparticle t left join news_channel n on t.channel_id=n.channel_id")

# 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
oa.spark.sql("use article")
sentence_df = channel_basic_content.select("article_id", "channel_id", "channel_name", "title", "content", \
                                           F.concat_ws(
                                             ",",
                                             channel_basic_content.channel_name,
                                             channel_basic_content.title,
                                             channel_basic_content.content
                                           ).alias("sentence")
                                          )

del basic_content
del channel_basic_content
gc.collect() # 释放内存

# 将数据写入Hive
# sentence_df.write.insertInto("article_data")