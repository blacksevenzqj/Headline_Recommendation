# -*- coding: UTF-8 -*-

import os
import sys

'''
spark-submit \
--master local \
/root/toutiao_project/reco_sys/offline/full_cal/compute_tfidf.py
或
python3 compute_tfidf.py
'''

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))
PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
from offline import SparkSessionBase

class KeywordsToTfidf(SparkSessionBase):

    SPARK_APP_NAME = "keywordsByTFIDF"  # 可通过spark-submit命令在脚本中指定，但如果使用python执行则需在代码中指定。
    SPARK_URL = "local"  # 可通过spark-submit命令在脚本中指定，但如果使用python执行则需在代码中指定。
    SPARK_EXECUTOR_MEMORY = "1g" # "7g"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

ktt = KeywordsToTfidf()


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


# 1、读取文章原始数据（并分词）
ktt.spark.sql("use article")
article_dataframe = ktt.spark.sql("select * from article_data limit 20")
article_dataframe.show()
# DataFrame.map(函数)：是一次性将所有数据都传入函数中，不是一个数据掉一次函数。
words_df = article_dataframe.rdd.mapPartitions(segmentation).toDF(["article_id", "channel_id", "words"])
words_df.show()


# 2、训练模型,得到每个文章词的频率Counts结果
'''
# 词语与词频统计
from pyspark.ml.feature import CountVectorizer
# 总词汇的大小，文本中必须出现的次数
cv = CountVectorizer(inputCol="words", outputCol="countFeatures", vocabSize=200*10000, minDF=1.0)
# 训练词频统计模型
cv_model = cv.fit(words_df)
cv_model.write().overwrite().save("hdfs://hadoop-master:9000/headlines/models/CV.model")
'''


# 3.1、训练idf模型，保存
'''
# 词语与词频统计
from pyspark.ml.feature import CountVectorizerModel
cv_model = CountVectorizerModel.load("hdfs://hadoop-master:9000/headlines/models/CV.model")
# 得出词频向量结果
cv_result_df = cv_model.transform(words_df)
# 训练IDF模型
from pyspark.ml.feature import IDF
idf = IDF(inputCol="countFeatures", outputCol="idfFeatures")
idfModel = idf.fit(cv_result_df)
idfModel.write().overwrite().save("hdfs://hadoop-master:9000/headlines/models/IDF.model")


cv_result_df.show() # article_id, channel_id, words（分词）, countFeatures（分词的词频：稀疏向量）

print(cv_model.vocabulary[:20]) # 分词的词列表
# 每个词的逆文档频率，在历史13万文章当中是固定的值，也作为后面计算TFIDF依据
print(idfModel.idf.toArray()[:20])
# cv_model.vocabulary 和 idfModel.idf.toArray() 是一一对应的。
'''

'''
3.2、上传训练13万文章的模型（意思就是使用全量数据训练得到的模型）
20篇文章，计算出代表20篇文章中N个词的IDF以及每个文档的词频，最终得到的是这20片文章的TFIDF
两个模型训练需要很久，所以在这里我们上传已经训练好的模型到指定路径
hadoop dfs -mkdir /headlines
hadoop dfs -mkdir /headlines/models/
hadoop dfs -put /root/backup/modelsbak/countVectorizerOfArticleWords.model/ /headlines/models/
hadoop dfs -put /root/backup/modelsbak/IDFOfArticleWords.model/ /headlines/models/
'''


'''
4、计算N篇文章数据的TFIDF值
步骤：
4.1、获取两个模型相关参数，计算并保存所有的13万文章中的关键字对应的idf值和索引。
为什么要保存这些值？并且存入数据库当中？
后续计算tfidf画像需要使用，避免放入内存中占用过多，持久化使用

Hive中建立表：idf_keywords_values
CREATE TABLE idf_keywords_values(
keyword STRING comment "article_id",
idf DOUBLE comment "idf",
index INT comment "index");
'''
from pyspark.ml.feature import CountVectorizerModel
# cv_model = CountVectorizerModel.load("hdfs://hadoop-master:9000/headlines/models/countVectorizerOfArticleWords.model")
cv_model = CountVectorizerModel.load("hdfs://hadoop-master:9000/headlines/models/CV.model")

from pyspark.ml.feature import IDFModel
# idf_model = IDFModel.load("hdfs://hadoop-master:9000/headlines/models/IDFOfArticleWords.model")
idf_model = IDFModel.load("hdfs://hadoop-master:9000/headlines/models/IDF.model")

keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))

def func(data):
    for index in range(len(data)):
        data[index] = list(data[index])
        data[index].append(index)
        data[index][1] = float(data[index][1])

print(len(keywords_list_with_idf))
func(keywords_list_with_idf)
sc = ktt.spark.sparkContext
rdd = sc.parallelize(keywords_list_with_idf)
df = rdd.toDF(["keywords", "idf", "index"])
df.show()

# df.write.insertInto('idf_keywords_values')

'''
4.2、模型计算得出N篇文章的TFIDF值选取TOPK，与IDF索引查询得到词
     模型计算得出N篇文章的TFIDF值，IDF索引结果合并得到词

保存TFIDF的结果,在article数据库中创建表：
CREATE TABLE tfidf_keywords_values(
article_id INT comment "article_id",
channel_id INT comment "channel_id",
keyword STRING comment "keyword",
tfidf DOUBLE comment "tfidf");
'''
# 计算tfidf值进行存储
'''
from pyspark.ml.feature import CountVectorizerModel
cv_model = CountVectorizerModel.load("hdfs://hadoop-master:9000/headlines/models/countVectorizerOfArticleWords.model")
from pyspark.ml.feature import IDFModel
idf_model = IDFModel.load("hdfs://hadoop-master:9000/headlines/models/IDFOfArticleWords.model")
cv_result = cv_model.transform(words_df)
tfidf_result = idf_model.transform(cv_result)

def func(partition):
    TOPK = 20
    for row in partition:
        # 找到索引与IDF值并进行排序
        _ = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
        _ = sorted(_, key=lambda x: x[1], reverse=True)
        result = _[:TOPK]
        for word_index, tfidf in result:
            yield row.article_id, row.channel_id, int(word_index), round(float(tfidf), 4)

_keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["article_id", "channel_id", "index", "tfidf"])
_keywordsByTFIDF.show()

# 保存对应words读取idf_keywords_values表结果合并
keywordsIndex = ktt.spark.sql("select keyword, index idx from idf_keywords_values")
# 利用结果索引与 idf_keywords_values join 得到 keyword
keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex, keywordsIndex.idx == _keywordsByTFIDF.index).select(["article_id", "channel_id", "keyword", "tfidf"])
# keywordsByTFIDF.write.insertInto("tfidf_keywords_values")
'''

#########################################################################################################

# 5、TextRank计算

