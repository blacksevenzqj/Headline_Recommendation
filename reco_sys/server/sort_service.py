# -*- coding: UTF-8 -*-

from server import SORT_SPARK
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegressionModel
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger("recommend")


# 排序
def lr_sort_service(reco_set, temp, hbu, recommend_num=100):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    """
    # 1、读取 用户特征中心特征（根据user_id、channel_id）
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []

    if user_feature: # 用户特征存在：
        # 2.1、读取 文章特征中心特征（根据 多路召回结果集）
        result = []
        # 多路召回结果集
        for article_id in reco_set:
            try:
                # 一篇文章：channel_id（1维）+ 文章向量article_vector（数组类型：100维）+ 文章关键词权重article_keywords_weights（数组类型：10维）= 111个特征向量
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = [0.0] * 111

            # 2.2、合并用户文章特征构造预测样本（必须和训练模型时的特征变量的顺序相同）
            # 合并特征向量(channel_id（1维）+ 文章向量（数组类型：100维）+ 文章关键词权重（数组类型：10维）+ 用户特征权重（数组类型：10维）) = 121个特征
            f = []
            # 第一个：channel_id
            f.extend([article_feature[0]])
            # 第二个：word2vec文章向量article_vector（数组类型：100维）
            f.extend(article_feature[1:101]) # 左闭右开
            # 第三个：文章画像关键词权重（数组类型：10维）
            f.extend(article_feature[101:111]) # 左闭右开
            # 第四个：用户画像主题词权重（数组类型：10维）
            f.extend(user_feature)

            # 将特征合并为 features字段 并转换为 Vector向量类型
            # 循环添加进列表构建 features字段  和  使用模型特征收集器VectorAssembler效果相同
            vector = DenseVector(f)
            result.append([temp.user_id, article_id, vector])
        '''
        注意：
        1、训练模型时：
        1.1、使用 模型特征收集器VectorAssembler：收集特征从 channel_id → user_article_partial 共4个特征。columns[2:6]左闭右开
        train_vecrsion_two = VectorAssembler().setInputCols(columns[2:6]).setOutputCol('features').transform(train_1) # API必须连串书写
        1.2、lr = LogisticRegression() 
        必须指定：训练特征字段名：features（向量）  和  目标字段名：clicked
        model = lr.setLabelCol("clicked").setFeaturesCol("features").fit(train_vecrsion_two) # API必须连串书写
        model.save("hdfs://hadoop-master:9000/headlines/models/test_ctr.obj")
        2、预测模型时：
        同样需要一个特征字段名：features（向量），字段名 和 向量顺序 都必须和训练时相同。
        也就是说：预测模型时 只针对 特征字段名：features（向量），其他无视（我估计的，因为模型预测时传入了训练时没有的字段）
        '''

        # 3、加载模型预测 → 倒排序点击概率（筛选）→ 截取 自定义推荐数量 的推荐结果
        # 构建 特征数组 → Pandas的DataFrame → Spark的DataFrame
        df = pd.DataFrame(result, columns=["user_id", "article_id", "features"])
        test = SORT_SPARK.createDataFrame(df)

        # 加载逻辑回归模型
        model = LogisticRegressionModel.load("hdfs://hadoop-master:9000/headlines/models/logistic_ctr_model.obj")
        predict = model.transform(test)

        def vector_to_double(row):
            return float(row.article_id), float(row.probability[1]) # probability[1]点击概率

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