# -*- coding: UTF-8 -*-

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from server import redis_client
from server import pool
import logging
from datetime import datetime
from server.utils import HBaseUtils

logger = logging.getLogger('recommend')


class ReadRecall(object):
    '''
    读取召回集的结果
    '''
    def __init__(self):
        self.client = redis_client
        self.hbu = HBaseUtils(pool)

    def read_hbase_recall_data(self, table_name, key_format, column_format):
        '''
        获取指定用户的对应频道的召回结果：在线内容画像召回，离线内容画像召回，离线ALS协同过滤召回 都在HBASE的 cb_recall 表中
        在 合并多路召回数据 做推荐准备时，从cb_recall表中拿取数据后，要进行删除。
        '''
        reco_set = []
        try:
            data = self.hbu.get_table_cells(table_name, key_format, column_format)
            for _ in data:
                reco_set = list(set(reco_set).union(set(eval(_))))

            # 删除召回结果（测试时 注释掉）
            self.hbu.get_table_delete(table_name, key_format, column_format)
        except Exception as e:
            logger.warning("{} WARN read {} recall exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),table_name, e))

        return reco_set


    def read_redis_new_article(self, channel_id):
        '''
        读取用户的新文章
        '''
        logger.info("{} INFO read channel {} redis new article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),channel_id))
        key = 'ch:{}:new'.format(channel_id)
        try:
            reco_list = self.client.zrevrange(key, 0, -1)
        except Exception as e:
            logger.warning("{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            reco_list = []

        return list(map(int, reco_list))


    def read_redis_hot_article(self, channel_id, hot_num=10):
        '''
        读取新闻章召回结果
        '''
        logger.info("{} INFO read channel {} redis hot article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),channel_id))
        _key = "ch:{}:hot".format(channel_id)
        try:
            res = self.client.zrevrange(_key, 0, -1)
        except Exception as e:
            logger.warning(
                "{} WARN read new article exception:{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []

        # 由于每个频道的热门文章有很多，因为保留文章点击次数
        res = list(map(int, res))
        if len(res) > hot_num:
            res = res[:hot_num]

        return res


    def read_hbase_article_similar(self, table_name, key_format, article_num=10):
        """获取文章相似结果
        """
        # 第一种表结构方式测试：
        # create 'article_similar', 'similar'
        # put 'article_similar', '1', 'similar:1', 0.2
        # put 'article_similar', '1', 'similar:2', 0.34
        try:
            _dic = self.hbu.get_table_row(table_name, key_format)

            res = []
            _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)
            if len(_srt) > article_num:
                _srt = _srt[:article_num]
            for _ in _srt:
                res.append(int(_[0].decode().split(':')[1]))
        except Exception as e:
            logger.error(
                "{} ERROR read similar article exception: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
            res = []

        return res


# if __name__ == '__main__':
#     rr = ReadRecall()
#     print("离线ALS模型召回" + str(rr.read_hbase_recall('cb_recall', b'recall:user:2', b'als:18')))
#     print("离线内容召回" + str(rr.read_hbase_recall('cb_recall', b'recall:user:2', b'content:18')))
#     print("在线内容召回" + str(rr.read_hbase_recall('cb_recall', b'recall:user:2', b'online:18')))
#
#     print("在线新文章召回" + str(rr.read_redis_new_article(18)))
#     print("在线热门文章召回" + str(rr.read_redis_hot_article(18)))
#
#     print(rr.read_hbase_article_similar('article_similar', b'1', 10))