# -*- coding: UTF-8 -*-

from server import cache_client
import logging
from datetime import datetime

logger = logging.getLogger('recommend')


def get_cache_from_redis_hbase(temp, hbu, redis_cache_len=100):
    """
    读取用户的缓存结果
    :temp param: 用户请求参数
    :hbu param: hbase 读取
    """
    # 1、从 redis:8 取数据
    key = 'reco:{}:{}:art'.format(temp.user_id, temp.channel_id)
    res = cache_client.zrevrange(key, 0, temp.article_num - 1) # 疑问：如果redis:8中不够请求article_num数量的情况是什么？
    if res:
        # 1、如果 redis:8 有数据，则删除取出的数据
        cache_client.zrem(key, *res)
    else:
        # 2、如果 redis:8 没有数据，进行wait_recommend读取，放入redis中
        cache_client.delete(key)
        try:
            hbase_cache = eval(hbu.get_table_row('wait_recommend',
                                                 'reco:{}'.format(temp.user_id).encode(),
                                                 'channel:{}'.format(temp.channel_id).encode()))
        except Exception as e:
            logger.warning("{} WARN read user_id:{} wait_recommend exception:{} not exist".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, e))
            hbase_cache = []

        # 2.1、如果hbase_cache为Null，则直接返回 空列表[]
        if not hbase_cache:
            return hbase_cache

        # 2.2、wait_recommend的 HBase表存储数量 > redis:8 中设置的存储数量（100）
        if len(hbase_cache) > redis_cache_len:
            logger.info(
                "{} INFO reduce cache  user_id:{} channel:{} wait_recommend data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

            # 2.2.1、截取 redis:8 中设置的存储数量（100） 的数据 放入 Redis:8
            redis_cache = hbase_cache[:redis_cache_len]
            cache_client.zadd(key, dict(zip(redis_cache, range(len(redis_cache)))))

            # 2.2.2、剩下的数据重新放回到 wait_recommend 的HBase表。
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str(hbase_cache[redis_cache_len:]).encode(),
                              timestamp=temp.time_stamp)
        else:
            # 2.3、wait_recommend的 HBase表存储数量 < redis:8 中设置的存储数量（100）
            logger.info(
                "{} INFO delete user_id:{} channel:{} wait_recommend cache data".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
            # 2.3.1、wait_recommend的 HBase表存储数量 全部 放入redis:8
            cache_client.zadd(key, dict(zip(hbase_cache, range(len(hbase_cache)))))
            # 2.3.2、清空 wait_recommend的 HBase表存储数量
            hbu.get_table_put('wait_recommend',
                              'reco:{}'.format(temp.user_id).encode(),
                              'channel:{}'.format(temp.channel_id).encode(),
                              str([]).encode(),
                              timestamp=temp.time_stamp)

        # 3、取出 redis:8 的 请求指定个数article_num 的数据（redis:8剩下的数据：redis_cache_len - article_num）
        res = cache_client.zrevrange(key, 0, temp.article_num - 1)
        if res:
            cache_client.zrem(key, *res) # 删除取出的 请求指定个数article_num 数据

    # 数据类型转换为int
    res = list(map(int, res))
    logger.info("{} INFO get cache data and store user_id:{} channel:{} cache data".format(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

    # 4、实际推荐出去的结果放入 历史推荐history_recommend 的HBase表（表示这次又成功推荐一次）
    hbu.get_table_put('history_recommend',
                           'reco:his:{}'.format(temp.user_id).encode(),
                           'channel:{}'.format(temp.channel_id).encode(),
                           str(res).encode(),
                           timestamp=temp.time_stamp)

    return res
