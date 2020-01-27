# -*- coding: UTF-8 -*-

import os
import sys

'''
spark-submit \
--master local \
/root/toutiao_project/reco_sys/offline/full_cal/similar.py
或
python3 similar.py
'''

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

from offline import SparkSessionBase
from setting.default import CHANNEL_INFO
from pyspark.ml.feature import Word2Vec


class TrainWord2VecModel(SparkSessionBase):
    SPARK_APP_NAME = "Word2Vec"
    SPARK_URL = "local"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()


w2v = TrainWord2VecModel()

# 训练一个频道的模型
w2v.spark.sql("use article")
article_data = w2v.spark.sql("select article_id from article_data where channel_id=18 limit 5")
article_data.show()


# 文章数据进行分词处理,得到分词结果
# 分词
def segmentation(partition):
    import os
    import re

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "/root/backup/words"

    # 结巴加载用户词典
    userDict_path = os.path.join(abspath, "ITKeywords.txt")
    jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path).readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    # 分词
    def cut_sentence(sentence):
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        # print(sentence,"*"*100)
        # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
        seg_list = pseg.lcut(sentence)
        seg_list = [i for i in seg_list if i.flag not in stopwords_list]
        filtered_words_list = []
        for seg in seg_list:
            # print(seg)
            if len(seg.word) <= 1:
                continue
            elif seg.flag == "eng":
                if len(seg.word) <= 2:
                    continue
                else:
                    filtered_words_list.append(seg.word)
            elif seg.flag.startswith("n"):
                filtered_words_list.append(seg.word)
            elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
                filtered_words_list.append(seg.word)
        return filtered_words_list

    for row in partition:
        sentence = re.sub("<.*?>", "", row.sentence)    # 替换掉标签数据
        words = cut_sentence(sentence)
        yield row.article_id, row.channel_id, words

'''
words_df = article_data.rdd.mapPartitions(segmentation).toDF(['article_id', 'channel_id', 'words'])
words_df.show()

# 直接调用word2vec训练
w2v_model = Word2Vec(vectorSize=100, inputCol='words', outputCol='model', minCount=3)

model = w2v_model.fit(words_df)
model.save("hdfs://hadoop-master:9000/headlines/models/test.word2vec")
'''


# 1、加载某个频道模型，得到每个词的向量
from pyspark.ml.feature import Word2VecModel
channel_id = 18
channel = "python"
wv_model = Word2VecModel.load(
                "hdfs://hadoop-master:9000/headlines/models/word2vec_model/channel_%d_%s.word2vec" % (channel_id, channel))
vectors = wv_model.getVectors()
vectors.show()