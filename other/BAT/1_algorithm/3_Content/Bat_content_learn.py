# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 21:45:06 2020

@author: dell
"""

import numpy as np
import pandas as pd
import os 
import operator
import timeit
from collections import OrderedDict

import FeatureTools as ft
ft.set_file_path(r"E:\soft\Anaconda\Anaconda_Python3.6_code\data_analysis\103_Recommend\BAT\data\ml-1m")
import Tools_customize as tc
import Data_Samping as ds

# In[]:
def get_ave_score(input_file, split_char="::", title_num=None, encoding="UTF-8"):
    if not os.path.exists(input_file):
        return {}
    record_dict = {}
    score_dict = {}
    fp = open(input_file, encoding=encoding)
    line_num = 0
    for line in fp:
        if (title_num is not None) and (line_num <= title_num):
            line_num += 1
            continue
        item = line.strip().split(split_char)
        if len(item) < 4:
            continue
        userId, itemId, rating = item[0], item[1], item[2]
        if itemId not in record_dict:
            record_dict[itemId] = [0,0]
        record_dict[itemId][0] += 1
        record_dict[itemId][1] += int(rating)
    fp.close
    for itemId in record_dict:
        score_dict[itemId] = round(record_dict[itemId][1]/record_dict[itemId][0], 3)
    return score_dict
# In[]:
score_dict = get_ave_score("ratings.txt")


# In[]:
# item_cate： 每个电影 对应 自身电影类别占比
# cate_item_sort： 按电影类别分组 电影评分倒排序 得到 每个电影类别 对应的 电影列表
def get_item_cate(input_file, ave_score, topk=10, split_char="::", title_num=None, encoding="UTF-8"):
    '''
    Args:
        ave_score: a dict, key:itemid, value:rating score
        input_file: item info file
    Return:
        a dict: key:itemid, value:a dict: key：cate, value：ratio
        a dict: key:cate, value:[itemid1, itemid2, itemid3]
    '''
    if not os.path.exists(input_file):
        return {}
    item_cate = {}
    record = {}
    cate_item_sort = {}
    fp = open(input_file, encoding=encoding)
    line_num = 0
    for line in fp:
        if (title_num is not None) and (line_num <= title_num):
            line_num += 1
            continue
        item = line.strip().split(split_char)
        if len(item) < 3:
            continue
        itemid, title, cate_str = item[0], item[1], item[2]
        cate_list = cate_str.strip().split("|")
        ratio = round(1 / len(cate_list), 3)
        if itemid not in item_cate:
            item_cate[itemid] = {}
        for fix_cate in cate_list:
            item_cate[itemid][fix_cate] = ratio
    fp.close
    for itemid in item_cate:
        for cate in item_cate[itemid]:
            if cate not in record:
                record[cate] = {}
            itemid_rating_score = ave_score.get(itemid, 0)
            record[cate][itemid] = itemid_rating_score
    for cate in record:
        if cate not in cate_item_sort:
            cate_item_sort[cate] = []
        for zuhe in tc.dict_sorted(record[cate])[:topk]:
            cate_item_sort[cate].append(zuhe[0])
    return item_cate, cate_item_sort
# In[]:
item_cate, cate_item_sort = get_item_cate('movies.txt', score_dict)


# In[]:
# 评分文件 最大时间戳
def get_latest_timestamp(input_file):
    """
    Args:
        input_file:user rating file
    only need run once
    """

    if not os.path.exists(input_file):
        return
    linenum = 0
    latest = 0
    fp = open(input_file)
    for line in fp:
        if linenum == 0:
            linenum += 1
            continue
        item = line.strip().split(",")
        if len(item) < 4:
            continue
        timestamp = int(item[3])
        if timestamp > latest:
            latest = timestamp
    fp.close()
    print(latest)
    #ans is :1476086345
    
    
# In[]:
# up： 用户 对 电影类别 的平均喜好  
def get_up(item_cate, input_file):
    """
    Args:
        item_cate:key itemid, value: dict , key category value ratio
        input_file:user rating file
    Return:
        a dict: key userid, value [(category, ratio), (category1, ratio1)]
    """
    if not os.path.exists(input_file):
        return {}
    record = {}
    up = {}
    linenum = 0
    score_thr = 4.0
    topk = 2 # 只选择 用户喜欢的 两种电影类别
    fp = open(input_file)
    for line in fp:
        if linenum == 0:
            linenum += 1
            continue
        item = line.strip().split('::')
        if len(item) < 4:
            continue
        userid, itemid, rating, timestamp = item[0], item[1], float(item[2]), int(item[3])
        if rating < score_thr:
            continue
        if itemid not in item_cate:
            continue
        time_score = get_time_score(timestamp)
        if userid not in record:
            record[userid] = {}
        for fix_cate in item_cate[itemid]:
            if fix_cate not in record[userid]:
                record[userid][fix_cate] = 0
            # 用户对电影类别的喜好 += 该用户对该部电影评分 * 时间评分 * 该部电影中该类别的占比
            record[userid][fix_cate] += rating * time_score * item_cate[itemid][fix_cate]
    fp.close()
    for userid in record:
        if userid not in up:
            up[userid] = []
        total_score = 0
        # 用户 对 电影类别 的喜好 倒排序（限定了2种电影类别）
        for zuhe in tc.dict_sorted(record[userid])[:topk]:
            if userid == "1":
                print(zuhe[0], zuhe[1])
            up[userid].append((zuhe[0], zuhe[1])) # (电影类别， 喜好)
            total_score += zuhe[1]
        # 用户 对 电影类别 的喜好 / 用户打过的总评分  =  用户 对 电影类别 的平均喜好  
        for index in range(len(up[userid])):
            up[userid][index] = (up[userid][index][0], round(up[userid][index][1]/total_score, 3))
    return up


def get_time_score(timestamp):
    """
    Args:
        timestamp:input timestamp
    Return:
        time score
    """
    fix_time_stamp = 1476086345
    total_sec = 24*60*60
    # 当前电影评分时间 与 整个评分表最晚评分时间 的时间差
    delta = (fix_time_stamp - timestamp)/total_sec/100 # 再除以100是因为，间隔天数太大，后一步计算得到0。
    # 时间间隔越久 时间评分越低： 1 / (1+时间间隔)
    return round(1/(1+delta), 3)


# 根据用户喜欢的电影类别倒排序 推荐 该电影类别中得分最高的电影
def recom(cate_item_sort, up, userid, topk= 10):
    """
    Args:
        cate_item_sort:reverse sort
        up:user profile
        userid:fix userid to recom
        topk:recom num
    Return:
         a dict, key userid value [itemid1, itemid2]
    """
    if userid not in up:
        return {}
    recom_result = {}
    if userid not in recom_result:
        recom_result[userid] = {}
    for zuhe in up[userid]:
        print(zuhe[0], zuhe[1])
        cate = zuhe[0]
        ratio = zuhe[1]
        num = int(topk*ratio) + 1
        if cate not in cate_item_sort:
            continue
        recom_list = cate_item_sort[cate][:num]
        if cate not in recom_result[userid]:
            recom_result[userid][cate] = []
        recom_result[userid][cate].append(recom_list)
    return  recom_result

# In[]:
up = get_up(item_cate, "ratings.txt")
print(len(up))
# In[]:
recom_result = recom(cate_item_sort, up, "1")


# In[]:
# dict默认貌似是有序的
for cate in recom_result["1"]:
    print(cate, recom_result["1"][cate])
    print(sum(recom_result["1"][cate], [])) # 降维
# In[]:
temp_list = ft.set_diff(['989', '3382', '3607', '3245', '53', '2503', '2019', '318'], ['919', '3114', '1'])
temp_list[0]
# In[]:

