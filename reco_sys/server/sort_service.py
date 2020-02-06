# -*- coding: UTF-8 -*-

from server import SORT_SPARK
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegressionModel
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger("recommend")


def lr_sort_service(reco_set, temp, hbu, recommend_num=100):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    """
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []

    if user_feature:
        # 2、读取文章特征中心特征
        result = []

        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = [0.0] * 111

            # 必须和训练模型时的特征变量的顺序相同。
            f = []
            # 第一个channel_id
            f.extend([article_feature[0]])
            # 第二个word2vec文章100向量article_vector
            f.extend(article_feature[1:101])
            # 第三个文章关键词权重10个
            f.extend(article_feature[101:111])
            # 第四个用户权重10
            f.extend(user_feature)

            vector = DenseVector(f)
            result.append([temp.user_id, article_id, vector])

        # 4、预测并进行排序是筛选
        df = pd.DataFrame(result, columns=["user_id", "article_id", "features"])
        test = SORT_SPARK.createDataFrame(df)

        # 加载逻辑回归模型
        model = LogisticRegressionModel.load("hdfs://hadoop-master:9000/headlines/models/logistic_ctr_model.obj")
        predict = model.transform(test)

        def vector_to_double(row):
            return float(row.article_id), float(row.probability[1])

        res = predict.select(['article_id', 'probability']).rdd.map(vector_to_double).toDF(
            ['article_id', 'probability']).sort('probability', ascending=False)

        article_list = [i.article_id for i in res.collect()]
        logger.info("{} INFO sorting user_id:{} recommend article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          temp.user_id))
        # 排序后，只将排名在前100个文章ID返回给用户推荐N个
        if len(article_list) > recommend_num:
            article_list = article_list[:recommend_num]
        reco_set = list(map(int, article_list))

    return reco_set