# -*- coding: UTF-8 -*-

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from abtest import user_reco_pb2_grpc
from abtest import user_reco_pb2
import grpc
from setting.default import DefaultConfig
import time


def test():
    article_dict = {}
    # 构造传入数据

    req_article = user_reco_pb2.User()
    req_article.user_id = '1'
    req_article.channel_id = 18
    req_article.article_num = 10
    # req_article.time_stamp = int(time.time() * 1000)
    req_article.time_stamp = 1580736947389

    with grpc.insecure_channel(DefaultConfig.RPC_SERVER) as rpc_cli:
        print('''''')
        try:
            stub = user_reco_pb2_grpc.UserRecommendStub(rpc_cli)
            resp = stub.user_recommend(req_article)
        except Exception as e:
            print(e)
            article_dict['param'] = []
        else:

            # 解析返回结果参数
            article_dict['exposure_param'] = resp.exposure

            reco_arts = resp.recommends

            reco_art_param = []
            reco_list = []
            for art in reco_arts:
                reco_art_param.append({
                    'artcle_id': art.article_id,
                    'params': {
                        'click': art.params.click,
                        'collect': art.params.collect,
                        'share': art.params.share,
                        'read': art.params.read
                    }
                })

                reco_list.append(art.article_id)
            article_dict['param'] = reco_art_param

            # 文章列表以及参数（曝光参数 以及 每篇文章的点击等参数）
            print(reco_list, article_dict)

            '''
            {'exposure_param': '', 
                'param': [
                    {'artcle_id': 17283, 'params': {'click': '{"action": "click", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "17283", "algorithmCombine": "Algo-1"}'}}, 
                    {'artcle_id': 140357, 'params': {'click': '{"action": "click", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "140357", "algorithmCombine": "Algo-1"}'}}, 
                    {'artcle_id': 14668, 'params': {'click': '{"action": "click", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'collect': '{"action": "collect", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'share': '{"action": "share", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}', 'read': '{"action": "read", "userId": "1", "articleId": "14668", "algorithmCombine": "Algo-1"}'}}
                ]
            }
            '''

if __name__ == '__main__':
    test()