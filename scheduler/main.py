# -*- coding: UTF-8 -*-

import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from scheduler.update import update_article_profile, update_user_profile, update_user_recall
import setting.logging as lg
lg.create_logger()


# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(3)
}

scheduler = BlockingScheduler(executors=executors)

# 添加一个定时运行文章画像更新的任务， 每隔一个小时运行一次
scheduler.add_job(update_article_profile, trigger='interval', hours=1)
# 添加一个定时运行用户画像更新的任务， 每隔二个小时运行一次
scheduler.add_job(update_user_profile, trigger='interval', hours=2)
# 添加一个定时运行用户召回更新的任务， 每隔三个小时运行一次
scheduler.add_job(update_user_recall, trigger='interval', hours=3)

scheduler.start()



'''
Supervisor进程管理
在reco.conf中添加如下：

[program:offline]
environment=JAVA_HOME=/root/bigdata/jdk,SPARK_HOME=/root/bigdata/spark,HADOOP_HOME=/root/bigdata/hadoop,PYSPARK_PYTHON=/miniconda2/envs/reco_sys/bin/python,PYSPARK_DRIVER_PYTHON=/miniconda2/envs/reco_sys/bin/python
command=/miniconda2/envs/reco_sys/bin/python /root/toutiao_project/scheduler/main.py
directory=/root/toutiao_project/scheduler
user=root
autorestart=true
redirect_stderr=true
stdout_logfile=/root/logs/offlinesuper.log
loglevel=info
stopsignal=KILL
stopasgroup=true
killasgroup=true
'''

