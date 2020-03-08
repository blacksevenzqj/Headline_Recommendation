# -*- coding: UTF-8 -*-

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
import hashlib
from setting.default import RAParam
from server.utils import HBaseUtils
from server import pool
from server.recall_service import ReadRecall
from server.redis_cache import get_cache_from_redis_hbase
from server.sort_service import lr_sort_service
from datetime import datetime
import logging
import json

logger = logging.getLogger('recommend')

sort_dict = {
    'LR': lr_sort_service,
}

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
    # 埋点参数参考 中的 曝光参数："param": '{"action": "exposure", "userId": 1, "articleId": [1,2,3,4],  "algorithmCombine": "c1"}'
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
        self.recall_service = ReadRecall()

    # 增加feed_recommend_logic函数，进行时间戳逻辑判断
    def feed_recommend_time_stamp_logic(self, temp):
        """
        用户刷新时间戳的逻辑
        :param temp: ABTest传入的用户请求参数
        """
        # 1、获取用户的历史数据库中最近一次时间戳last_stamp
        try:
            last_stamp = self.hbu.get_table_row('history_recommend',
                                                'reco:his:{}'.format(temp.user_id).encode(),
                                                'channel:{}'.format(temp.channel_id).encode(),
                                                include_timestamp=True)[1] # 返回的是列表，[1]是时间戳
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
        except Exception as e:
            # 如果用户没有历史记录会报异常
            logger.info("{} INFO get user_id:{} channel:{} history last_stamp, exception:{}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id, e))
            last_stamp = 0

        # 返回的 Track的time_stamp字段：上一条历史记录的时间戳（没有赋值为0）
        logger.info(str(last_stamp) + "___" + str(temp.time_stamp))

        # 2、如果last_stamp < 用户请求时间戳：用户的刷新操作
        if last_stamp < temp.time_stamp:
            # 2.1、走正常的推荐流程：缓存读取
            res = get_cache_from_redis_hbase(temp, self.hbu)
            if not res: # 缓存中没有，则：召回排序流程
                logger.info("{} INFO cache is Null get user_id:{} channel:{} recall/sort data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id, e))
                # 2.2、走正常的推荐流程：召回排序流程
                res = self.user_reco_list(temp)
            # 历史数据库中最近一次时间戳last_stamp 赋值给 temp.time_stamp 最后封装成 Track的time_stamp字段 返回给前端
            temp.time_stamp = last_stamp
            _track = add_track(res, temp)
        else:
            # 3、如果last_stamp >= 用户请求时间戳, 用户才翻历史记录
            '''
            如果历史时间戳大于用户请求的这次时间戳，那么就是在获取历史记录，用户请求的历史时间戳是具体某个历史记录的时间戳T，
            Hbase当中不能够直接用T去获取，而需要去（T + N=1）> T 的时间戳获取，才能拿到包含T时间的结果，并且使用get_table_cells去获取
            '''
            logger.info("{} INFO read user_id:{} channel:{} history recommend data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
            try:
                # 根据 用户传入的时间戳（需要 传入的时间戳 + 1） 使用get_table_cells去获取 <= 传入的时间戳 的所有历史数据。
                row = self.hbu.get_table_cells('history_recommend',
                                               'reco:his:{}'.format(temp.user_id).encode(),
                                               'channel:{}'.format(temp.channel_id).encode(),
                                               timestamp=temp.time_stamp + 1,
                                               include_timestamp=True)
            except Exception as e:
                # 如果用户没有历史记录会报异常
                logger.warning("{} WARN read history recommend exception:{}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
                row = []
                res = []

            # 历史推荐结果 三步判断逻辑：
            # 1、如果没有历史数据，返回时间戳0以及结果空列表
            if not row:
                temp.time_stamp = 0 # temp.time_stamp 最后封装成 Track的time_stamp字段 返回给前端
                res = [] # 推荐列表
            elif len(row) == 1 and row[0][1] == temp.time_stamp:
                # 2、如果历史数据只有一条，返回这一条历史数据以及时间戳正好为请求时间戳，修改时间戳为0，表示后面请求以后就没有历史数据了(APP的行为就是翻历史记录停止了)
                res = row[0][0]
                temp.time_stamp = 0 # temp.time_stamp 最后封装成 Track的time_stamp字段 返回给前端
            elif len(row) >= 2:
                # 3、如果历史数据多条，返回最近的第一条历史数据，然后返回最近的第二条历史数据的时间戳（为下次翻历史记录做准备）
                res = row[0][0]
                temp.time_stamp = int(row[1][1]) # temp.time_stamp 最后封装成 Track的time_stamp字段 返回给前端


            # res(bytes) → eval(res) → list(str)
            # list(str) → map(int, eval(res)) → list(int)
            # 最外层再套list：list(map(int, eval(res))) 防止res为空时报异常
            res = list(map(int, eval(res))) # 一条历史推荐结果（封装成列表）
            logger.info(
                "{} INFO history:{}, {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), res, temp.time_stamp))
            _track = add_track(res, temp)
            # 埋点参数参考 中的 曝光参数："param": '{"action": "exposure", "userId": 1, "articleId": [1,2,3,4],  "algorithmCombine": "c1"}'
            # 返回历史记录时，曝光参数就置为 '' 了。
            _track['param'] = ''

        return _track


    def user_reco_list(self, temp):
        """
        用户下拉刷新获取新数据的逻辑
        1、循环算法的召回集组合参数，合并多路召回结果集
        2.1、过滤当前该请求频道推荐历史结果（对合并的召回结果集进行history_recommend过滤）
        2.2、如果0号频道（推荐频道）有历史推荐记录，也需要过滤
        3、过滤之后，推荐出去指定个数的文章列表，写入历史记录history_recommend，剩下的写入待推荐结果wait_recommend
        """
        reco_set = []
        '''
        COMBINE={
            'Algo-1': (1, [100, 101, 102, 103, 104], [200]),  # 算法集名称 : (序号, [召回结果数据集列表], [排序模型列表])
            'Algo-2': (2, [100, 101, 102, 103, 104], [200])   # 目前为止不使用 105:文章相似度 直接查询 article_similar 的HBase表
        }
        '''
        # 1、循环算法的召回集组合参数，合并多路召回结果集
        for _num in RAParam.COMBINE[temp.algo][1]:
            # 进行每个召回结果的读取100,101,102,103,104
            if _num == 103:
                # 新文章召回读取
                _res = self.recall_service.read_redis_new_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res))) # 合并召回的结果
            elif _num == 104:
                # 热门文章召回读取
                _res = self.recall_service.read_redis_hot_article(temp.channel_id)
                reco_set = list(set(reco_set).union(set(_res))) # 合并召回的结果
            else:
                # 离线模型ALS召回、离线文章内容召回、在线文章内容召回
                _res = self.recall_service.read_hbase_recall_data(RAParam.RECALL[_num][0],
                                           'recall:user:{}'.format(temp.user_id).encode(),
                                           '{}:{}'.format(RAParam.RECALL[_num][1], temp.channel_id).encode())
                reco_set = list(set(reco_set).union(set(_res)))  # 合并召回的结果


        # 2.1、过滤当前该请求频道推荐历史结果（对合并的召回结果集进行history_recommend过滤）
        history_list = []
        try:
            # 所有版本
            data = self.hbu.get_table_cells('history_recommend',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(temp.channel_id).encode())
            for _ in data:
                history_list = list(set(history_list).union(set(eval(_))))

            logger.info("{} INFO filter user_id:{} channel:{} history data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
        except Exception as e:
            logger.warning("{} WARN filter history article exception:{}".format(datetime.now().
                                                                     strftime('%Y-%m-%d %H:%M:%S'), e))

        # 2.2、如果0号频道（推荐频道）有历史推荐记录，也需要过滤
        try:
            data = self.hbu.get_table_cells('history_recommend',
                                            'reco:his:{}'.format(temp.user_id).encode(),
                                            'channel:{}'.format(0).encode())
            for _ in data:
                history_list = list(set(history_list).union(set(eval(_))))

            logger.info("{} INFO filter user_id:{} channel:{} history data".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, 0))
        except Exception as e:
            logger.warning(
                "{} WARN filter history article exception:{}".format(datetime.now().
                                                                     strftime('%Y-%m-%d %H:%M:%S'), e))

        # 过滤操作 reco_set 与 history_list 进行过滤
        reco_set = list(set(reco_set).difference(set(history_list)))
        logger.info("{} INFO after filter history is {}".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), reco_set))


        # 3、过滤之后，推荐出去指定个数的文章列表，写入历史记录history_recommend，剩下的写入待推荐结果wait_recommend
        # 如果过滤之后没有数据，直接返回
        if not reco_set:
            return reco_set
        else:
            # 排序代码逻辑
            _sort_num = RAParam.COMBINE[temp.algo][2][0] # 排序模型列表 中 索引出 排序模型编号
            reco_set = sort_dict[RAParam.SORT[_sort_num]](reco_set, temp, self.hbu)

            # 类型进行转换
            reco_set = list(map(int, reco_set))

            # 跟请求需要推荐的文章数量article_num 进行比对
            # 如果请求推荐文章数量article_num > 实际推荐文章总数量reco_set
            if len(reco_set) <= temp.article_num:
                # 按 实际推荐文章总数量reco_set 进行推荐
                res = reco_set
            else:
                # 如果请求推荐文章数量article_num < 实际推荐文章总数量reco_set
                # 3.1、截取请求推荐文章数量
                res = reco_set[:temp.article_num] # 左开右闭
                # 3.2、剩下的实际推荐结果放入wait_recommend，等待下次刷新时直接推荐（len(reco_set) - article_num）
                self.hbu.get_table_put('wait_recommend',
                                       'reco:{}'.format(temp.user_id).encode(),
                                       'channel:{}'.format(temp.channel_id).encode(),
                                       str(reco_set[temp.article_num:]).encode(), # 多出的实际推荐结果
                                       timestamp=temp.time_stamp)
                logger.info(
                    "{} INFO put user_id:{} channel:{} wait data".format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))


            # 将实际推荐出去的结果 放入历史记录表当中
            self.hbu.get_table_put('history_recommend',
                                   'reco:his:{}'.format(temp.user_id).encode(),
                                   'channel:{}'.format(temp.channel_id).encode(),
                                   str(res).encode(),
                                   timestamp=temp.time_stamp)
            # 将实际推荐出去的结果 放入历史记录日志
            logger.info(
                "{} INFO store recall/sorted user_id:{} channel:{} history_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

        return res

