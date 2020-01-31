# -*- coding: UTF-8 -*-

import os
import sys

'''
spark-submit \
--master local \
/root/toutiao_project/reco_sys/offline/full_cal/update_hive_partition.py
或
python3 update_hive_partition.py
'''

# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))

PYSPARK_PYTHON = "/miniconda2/envs/py365/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

from offline import SparkSessionBase
import pyhdfs
import time

class UpdateUserProfile(SparkSessionBase):
    """离线相关处理程序
    """
    SPARK_APP_NAME = "updateUser"
    SPARK_URL = "local"
    ENABLE_HIVE_SUPPORT = True

    SPARK_EXECUTOR_MEMORY = "1g"

    def __init__(self):

        self.spark = self._create_spark_session()


# 手动关联所有日期文件
import pandas as pd
from datetime import datetime

def datelist(beginDate, endDate):
    date_list=[datetime.strftime(x,'%Y-%m-%d') for x in list(pd.date_range(start=beginDate, end=endDate))]
    return date_list


def update_all(uup, date="2019-03-05", hosts='hadoop-master:50070'):
    dl = datelist(date, time.strftime("%Y-%m-%d", time.localtime()))

    fs = pyhdfs.HdfsClient(hosts=hosts)
    for d in dl:
        try:
            _localions = '/user/hive/warehouse/profile.db/user_action/' + d
            if fs.exists(_localions):
                uup.spark.sql("use profile")
                uup.spark.sql("alter table user_action add partition (dt='%s') location '%s'" % (d, _localions))
        except Exception as e:
            # 已经关联过的异常忽略,partition与hdfs文件不直接关联
            pass


def update_today(uup, time_str=None, hosts='hadoop-master:50070'):
    if time_str is None:
        # 如果hadoop没有今天该日期文件，则没有日志数据，结束
        time_str = time.strftime("%Y-%m-%d", time.localtime())

    fs = pyhdfs.HdfsClient(hosts=hosts)
    _localions = '/user/hive/warehouse/profile.db/user_action/' + time_str
    if fs.exists(_localions):
        uup.spark.sql("use profile")
        # 如果有该文件直接关联，捕获关联重复异常
        # try:
        #     uup.spark.sql("alter table user_action add partition (dt='%s') location '%s'" % (time_str, _localions))
        # except Exception as e:
        #     pass

        sqlDF = uup.spark.sql("select actionTime, readTime, channelId, param.articleId, param.algorithmCombine, "
                              "param.action, param.userId from user_action where dt='{}'".format(time_str))
        # sqlDF = uup.spark.sql("select actionTime, readTime, channelId, param.articleId, param.algorithmCombine, "
        #                       "param.action, param.userId from user_action where dt= '" + time_str + "' limit 10")
        sqlDF.show()
    else:
        pass



if __name__ == '__main__':
    uup = UpdateUserProfile()
    # update_all(uup)
    update_today(uup, "2019-03-05")
    uup.spark.stop()