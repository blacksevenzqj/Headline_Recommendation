# -*- coding: UTF-8 -*-

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from online import stream_sc, SIMILAR_DS
import json
import time
from datetime import datetime
import setting.logging as lg
import logging

logger = logging.getLogger('online')


class OnlineRecall(object):
    """在线计算部分
    1、在线内容召回，实时写入用户点击或者操作文章的相似文章
    2、在线新文章召回
    3、在线热门文章召回
    """
    def __init__(self):
        pass

    def _update_content_recall(self):
        '''
        在线内容召回计算
        '''
        # {"actionTime":"2019-04-10 21:04:39","readTime":"","channelId":18,"param":{"action": "click", "userId": "2", "articleId": "116644", "algorithmCombine": "C2"}}
        # x [,'json.....']
        def get_similar_online_recall(rdd):
            import happybase
            pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
            '''
            解析rdd中的内容，然后进行获取计算
            '''
            # rdd的[row(1,2,3), row(4,5,6)] -----> rdd.collect()的[[1,2,3], [4,5,6]]
            for data in rdd.collect():

                # 进行data字典处理过滤
                if data['param']['action'] in ["click", "collect", "share"]:
                    logger.info("{} INFO: get user_id:{} action:{}  log".format(
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                data['param']['userId'], data['param']['action']))
                    # 读取param当中articleId，相似的文章
                    with pool.connection() as conn:
                        sim_table = conn.table("article_similar")
                        # 根据用户点击流日志涉及文章找出与之最相似文章(基于内容的相似)，选取TOP-k相似的作为召回推荐结果
                        _dic = sim_table.row(str(data["param"]["articleId"]).encode(), columns=[b"similar"])
                        if _dic:
                            logger.info("_dic is " + str(_dic))
                            # {b'similar:1': b'0.2', b'similar:2': b'0.34', b'similar:3': b'0.267', b'similar:4': b'0.56', b'similar:5': b'0.7', b'similar:6': b'0.819', b'similar:8': b'0.28'}

                            _srt = sorted(_dic.items(), key=lambda obj: obj[1], reverse=True)  # 按相似度排序
                            logger.info("_srt is " + str(_srt))
                            # [(b'similar:6', b'0.819'), (b'similar:5', b'0.7'), (b'similar:4', b'0.56'), (b'similar:2', b'0.34'), (b'similar:8', b'0.28'), (b'similar:3', b'0.267'), (b'similar:1', b'0.2')]

                            topKSimIds = [int(i[0].split(b":")[1]) for i in _srt[:10]]
                            logger.info("topKSimIds is " + str(topKSimIds))
                            # [6, 5, 4, 2, 8, 3, 1]

                            # 根据历史推荐集过滤，已经给用户推荐过的文章
                            history_table = conn.table("history_recall")

                            _history_data = history_table.cells(
                                b"reco:his:%s" % data["param"]["userId"].encode(),
                                b"channel:%d" % data["channelId"]
                            )
                            logger.info("_history_data is " + str(_history_data))

                            history = []
                            if len(_history_data) >= 1:
                                for l in _history_data:
                                    history.extend(eval(l))
                            logger.info("history is " + str(history))

                            # 根据历史召回记录，过滤召回结果
                            recall_list = list(set(topKSimIds) - set(history))
                            logger.info("recall_list is " + str(recall_list))

                            # 如果有推荐结果集，那么将数据添加到cb_recall表中，同时记录到历史记录表中
                            logger.info("{} INFO: store online recall data:{}".format(
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(recall_list)))

                            if recall_list:
                                recall_table = conn.table("cb_recall")

                                recall_table.put(
                                    b"recall:user:%s" % data["param"]["userId"].encode(),
                                    {b"online:%d" % data["channelId"]: str(recall_list).encode()}
                                )

                                history_table.put(
                                    b"reco:his:%s" % data["param"]["userId"].encode(),
                                    {b"channel:%d" % data["channelId"]: str(recall_list).encode()}
                                )

                        conn.close()
                        logger.info("-"*30)

        # x可以是多次点击行为数据，同时拿到多条数据
        SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)


if __name__ == '__main__':
    lg.create_logger()
    ore = OnlineRecall()
    ore._update_content_recall()
    stream_sc.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pass
