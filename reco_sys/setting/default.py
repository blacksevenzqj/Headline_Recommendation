# -*- coding: UTF-8 -*-

# 增加spark online 启动配置
class DefaultConfig(object):
    """默认的一些配置信息
    """
    # 在线计算spark配置
    SPARK_ONLINE_CONFIG = (
        ("spark.app.name", "onlineUpdate"),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
        ("spark.master", "local[2]"),
        ("spark.executor.instances", 4)
    )

    # KAFKA配置
    KAFKA_SERVER = "192.168.19.137:9092"

    # redis IP和端口配置
    REDIS_HOST = "127.0.0.1" # 192.168.19.137
    REDIS_PORT = 6379

    RPC_SERVER = '192.168.19.137:9999'

    # 实时运行spark
    # SPARK grpc配置
    SPARK_GRPC_CONFIG = (
        ("spark.app.name", "grpcSort"),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
        ("spark.master", "local"), # yarn
        ("spark.executor.instances", 4)
    )


from collections import namedtuple

# ABTest参数
param = namedtuple('RecommendAlgorithm', ['COMBINE',
                                          'RECALL',
                                          'SORT',
                                          'CHANNEL',
                                          'BYPASS']
                   )

RAParam = param(
    COMBINE={
        'Algo-1': (1, [100, 101, 102, 103, 104], [200]),  # 算法集名称 : (序号, [召回结果数据集列表], [排序模型列表])
        'Algo-2': (2, [100, 101, 102, 103, 104], [201])   # 目前为止不使用 105:文章相似度 直接查询 article_similar 的HBase表
    },
    RECALL={
        100: ('cb_recall', 'als'),  # 离线模型ALS召回，recall:user:1115629498121 column=als:18
        101: ('cb_recall', 'content'),  # 离线word2vec的文章画像内容召回 'recall:user:5', 'content:1'
        102: ('cb_recall', 'online'),  # 在线word2vec的文章画像内容召回 'recall:user:1', 'online:1'
        103: 'new_article',  # 新文章召回redis当中：ch:18:new
        104: 'popular_article',  # 热门文章召回redis当中：ch:18:hot
        105: ('article_similar', 'similar')  # 文章相似推荐结果 '1' 'similar:2'
    },
    SORT={
        200: 'LR',
        201: 'WDL'
    },
    CHANNEL=25,
    BYPASS=[
            {
                "Bucket": ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd'],
                "Strategy": "Algo-1"
            },
            {
                "BeginBucket": ['e', 'f'],
                "Strategy": "Algo-2"
            }
    ]
)


CHANNEL_INFO = {
            1: "html",
            2: "开发者资讯",
            3: "ios",
            4: "c++",
            5: "android",
            6: "css",
            7: "数据库",
            8: "区块链",
            9: "go",
            10: "产品",
            11: "后端",
            12: "linux",
            13: "人工智能",
            14: "php",
            15: "javascript",
            16: "架构",
            17: "前端",
            18: "python",
            19: "java",
            20: "算法",
            21: "面试",
            22: "科技动态",
            23: "js",
            24: "设计",
            25: "数码产品",
        }