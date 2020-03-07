# -*- coding: UTF-8 -*-

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from concurrent import futures
from abtest import user_reco_pb2
from abtest import user_reco_pb2_grpc
from setting.default import DefaultConfig, RAParam
from server.reco_center import add_track, RecoCenter
import grpc
import time
import json
import hashlib
import setting.logging as lg
import logging


logger = logging.getLogger('recommend')


def feed_recommend(user_id, channel_id, article_num, time_stamp):
    """
    1、根据web提供的参数，进行分流
    2、找到对应的算法组合之后，去推荐中心调用不同的召回和排序服务
    3、进行埋点参数封装
    :param user_id:用户id
    :param article_num:推荐文章个数
    :return: track:埋点参数结果: 参考上面埋点参数组合
    """

    #  产品前期推荐由于较少的点击行为，所以去做 用户冷启动 + 文章冷启动
    # 用户冷启动：'推荐'频道：热门频道的召回+用户实时行为画像召回（在线的不保存画像）  'C2'组合
    #              其它 频道：热门召回 + 新文章召回   'C1'组合
    # 定义返回参数的类
    class TempParam(object):
        user_id = -10
        channel_id = -10
        article_num = -10
        time_stamp = -10
        algo = ""

    temp = TempParam()
    temp.user_id = user_id
    temp.channel_id = channel_id
    temp.article_num = article_num
    # 请求的时间戳大小
    temp.time_stamp = time_stamp

    # 进行用户的分流
    # 如果用户ID为空，则不做推荐，返回
    if temp.user_id == "":
        return add_track([], temp)

    # ABTest分流（用户分桶）：用户名md5加密之后取首字母在 default.py→RAParam.BYPASS 设置的哪个桶区间中
    code = hashlib.md5(temp.user_id.encode()).hexdigest()[:1]
    if code in RAParam.BYPASS[0]['Bucket']:
        temp.algo = RAParam.BYPASS[0]['Strategy'] # 设置算法名称
    else:
        temp.algo = RAParam.BYPASS[1]['Strategy'] # 设置算法名称

    # 推荐服务中心推荐结果
    # track = add_track([], temp) # (这里做测试，直接构建结果)
    _track = RecoCenter().feed_recommend_time_stamp_logic(temp)

    return _track


# user_reco_pb2_grpc.UserRecommendServicer类 是根据自定义protoco协议：user_reco.proto 中的 service UserRecommend 定义的
# 需重写 user_reco_pb2_grpc.UserRecommendServicer 中的 def user_recommend(self, request, context) 方法
class UserRecommendServicer(user_reco_pb2_grpc.UserRecommendServicer):
    """grpc黑马推荐接口服务端逻辑写
    """
    def user_recommend(self, request, context):

        # 1、接收参数解析封装
        user_id = request.user_id
        channel_id = request.channel_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 2、获取用户abtest分流，到推荐中心获取推荐结果，封装参数
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        _track = feed_recommend(user_id, channel_id, article_num, time_stamp)
        # 埋点参数参考：def add_track(res, temp)方法生成
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

        # 3、将参数进行grpc消息体封装，返回
        _reco = []
        for d in _track['recommends']:
            # 封装param2的消息体
            _param2 = user_reco_pb2.param2(click=d['param']['click'],
                                           collect=d['param']['collect'],
                                           share=d['param']['share'],
                                           read=d['param']['read'])

            # 封装param1的消息体
            # [(article_id, params), (article_id, params),(article_id, params),(article_id, params)]
            _param1 = user_reco_pb2.param1(article_id=d['article_id'], params=_param2)
            _reco.append(_param1)

        # 写代码时，从后往前推
        return user_reco_pb2.Track(exposure=_track['param'], recommends=_reco, time_stamp=_track['timestamp'])


def serve():
    # 创建recommend日志
    lg.create_logger()

    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册：重写的 自定义protoco协议 服务方法：
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    # 监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    # 测试grpc服务
    serve()