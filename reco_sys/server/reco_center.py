# -*- coding: UTF-8 -*-

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
import hashlib
from setting.default import RAParam
from server.utils import HBaseUtils
from server import pool
from datetime import datetime
import logging
import json

logger = logging.getLogger('recommend')

'''
{
    'param': '{"action": "exposure", "userId": "1", "articleId": "[17283, 140357, 14668]", "algorithmCombine": "Algo-1"}', 
    'recommends': [
        {'article_id': 17283, 'param': {'click': '{"action": "click", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}'}},
        {'article_id': 140357, 'param': {'click': '{"action": "click", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}'}},   
        {'article_id': 14668, 'param': {'click': '{"action": "click", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}'}}
    ],
    'timestamp': 0
}
'''
def add_track(res, temp):
    """
    封装埋点参数
    :param res: 推荐文章id列表
    :param temp: 用户相关参数
    :param cb: 合并参数
    :param rpc_param: rpc参数
    :return: 埋点参数
             文章列表参数
             单文章参数
    """
    # 添加埋点参数
    track = {}

    # 准备曝光参数
    # 全部字符串形式提供，在hive端不会解析问题
    _exposure = {"action": "exposure", "userId": temp.user_id, "articleId": json.dumps(res), "algorithmCombine": temp.algo}

    track['param'] = json.dumps(_exposure)
    track['recommends'] = []

    # 准备其它点击参数
    for _id in res:
        # 构造字典
        _dic = {}
        _dic['article_id'] = _id
        _dic['param'] = {}

        _p = {"action": "click", "userId": temp.user_id, "articleId": str(_id), "algorithmCombine": temp.algo}

        # 准备click参数
        _dic['param']['click'] = json.dumps(_p)

        # 准备collect参数
        _p["action"] = 'collect'
        _dic['param']['collect'] = json.dumps(_p)

        # 准备share参数
        _p["action"] = 'share'
        _dic['param']['share'] = json.dumps(_p)

        # 准备detentionTime参数
        _p["action"] = 'read'
        _dic['param']['read'] = json.dumps(_p)

        track['recommends'].append(_dic)

    track['timestamp'] = temp.time_stamp
    return track


class RecoCenter(object):
    """推荐中心
    1、处理时间戳逻辑
    2、召回、排序、缓存
    """
    def __init__(self):
        self.hbu = HBaseUtils(pool)

    def feed_recommend_time_stamp_logic(self, temp):
        """
        用户刷新时间戳的逻辑
        :param temp: ABTest传入的用户请求参数
        :return:
        """
        # 1、获取用户的历史数据库中最近一次时间戳last_stamp
        # 如果用户没有历史记录
        try:
            last_stamp = self.hbu.get_table_row('history_recommend',
                                                'reco:his:{}'.format(temp.user_id).encode(),
                                                'channel:{}'.format(temp.channel_id).encode(),
                                                include_timestamp=True)[1]
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

        except Exception as e:
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp, exception:{}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id, e))
            last_stamp = 0


        logger.info(str(last_stamp) + "___" + str(temp.time_stamp))
        # 2、如果last_stamp < 用户请求时间戳, 用户的刷新操作
        if last_stamp < temp.time_stamp:
            # 走正常的推荐流程
            # 缓存读取、召回排序流程
            # last_stamp应该是temp.time_stamp前面一条数据
            # 返回给用户上一条时间戳给定为last_stamp
            # 1559148615353
            temp.time_stamp = last_stamp
            _track = add_track([], temp)
            logger.info("My_Log is " + str(_track))
        else:
            # 3、如果last_stamp >= 用户请求时间戳, 用户才翻历史记录
            # 根据用户传入的时间戳请求，去读取对应的历史记录
            # temp.time_stamp
            # 1559148615353,hbase取出1559148615353小的时间戳的数据， 1559148615354
            logger.info("{} INFO read user_id:{} channel:{} history recommend data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
            try:
                row = self.hbu.get_table_cells('history_recommend',
                                               'reco:his:{}'.format(temp.user_id).encode(),
                                               'channel:{}'.format(temp.channel_id).encode(),
                                               timestamp=temp.time_stamp + 1,
                                               include_timestamp=True)
            except Exception as e:
                logger.warning("{} WARN read history recommend exception:{}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
                row = []
                res = []

            # [(,), ()]
            # 1559148615353, [15140, 16421, 19494, 14381, 17966]
            # 1558236647437, [18904, 14300, 44412, 18238, 18103, 43986, 44339, 17454, 14899, 18335]
            # 1558236629309, [43997, 14299, 17632, 17120]

            # 3步判断逻辑
            #1、如果没有历史数据，返回时间戳0以及结果空列表
            # 1558236629307
            if not row:
                temp.time_stamp = 0
                res = []
            elif len(row) == 1 and row[0][1] == temp.time_stamp:
                # [([43997, 14299, 17632, 17120], 1558236629309)]
                # 2、如果历史数据只有一条，返回这一条历史数据以及时间戳正好为请求时间戳，修改时间戳为0，表示后面请求以后就没有历史数据了(APP的行为就是翻历史记录停止了)
                res = row[0][0]
                temp.time_stamp = 0
            elif len(row) >= 2:
                res = row[0][0]
                temp.time_stamp = int(row[1][1])
                # 3、如果历史数据多条，返回最近的第一条历史数据，然后返回之后第二条历史数据的时间戳

            # res bytes--->list
            # list str---> int id
            res = list(map(int, eval(res)))

            logger.info(
                "{} INFO history:{}, {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), res, temp.time_stamp))

            _track = add_track(res, temp)
            _track['param'] = ''

        return _track



