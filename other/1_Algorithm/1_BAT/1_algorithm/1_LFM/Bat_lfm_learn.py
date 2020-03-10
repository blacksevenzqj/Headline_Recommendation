# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 13:12:40 2019

@author: dell
"""

import numpy as np
import pandas as pd
import os 
import operator
import timeit

import FeatureTools as ft
ft.set_file_path(r"E:\soft\Anaconda\Anaconda_Python3.6_code\data_analysis\103_Recommend\BAT\data\ml-1m")
import Tools_customize as tc
import Data_Samping as ds

# In[]:
# 电影表
item_info = ft.get_item_info('movies.txt')
print(len(item_info))
print(item_info["1"])
print(item_info["11"])
# In[]:
item_df = ft.readFile_inputData_no_header("movies.txt", names=["item_id","title","genre"], sep="::")

# In[]:
# 评分表
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
rating_df = ft.readFile_inputData_no_header("ratings.txt", names=["user_id","item_id","rating","timestamp"], sep="::")
# In[]:
aggs = {'rating_mean' : np.mean}
score_df = tc.groupby_agg_oneCol(rating_df, ["item_id"], "rating", aggs, as_index=False)


# In[]:
train_data = ds.native_get_train_data_pos_neg("ratings.txt", score_dict)
# In[]:
train_data[:36]


# In[]:
'''
rating_df_tmp = rating_df.merge(score_df, left_on='item_id', right_on='item_id', how='left')
# In[]:
rating_df_tmp["label"] = rating_df_tmp["rating"].map(lambda x: 1 if x > 4.0 else 0)
# In[]:
rating_df_tmp_pos = rating_df_tmp[rating_df_tmp["label"] == 1]
rating_df_tmp_neg = rating_df_tmp[rating_df_tmp["label"] == 0]
# In[]:
rating_df_tmp_neg_sort = tc.groupby_apply_sort(rating_df_tmp_neg, ["user_id"], ["rating_mean"], [False], False)
# In[]:
train_data_df = pd.DataFrame(columns=rating_df_tmp.columns)

# In[]:
tmp = rating_df_tmp.groupby(["user_id", "label"], as_index=False)["rating"].count()
# In[]:
tmp1 = tmp.groupby(["user_id"], as_index=False)["rating"].count()
# In[]:
user_id_tmp = tmp1[tmp1["rating"] == 2]
# In[]:
tmp2 = tmp.merge(user_id_tmp, left_on='user_id',right_on='user_id') 
# In[]:
tmp3 = tmp2.groupby(["user_id"], as_index=False)["rating_x"].min()
# In[]:
for item in tmp3.itertuples():
    # 必须使用 item[1] 或 item.user_id 取值， 不能使用： item["user_id"]；  索引取值： item[0] 或 item.Index
    temp1 = rating_df_tmp_pos[rating_df_tmp_pos["user_id"] == item.user_id][:item.rating_x]
    temp2 = rating_df_tmp_neg_sort[rating_df_tmp_neg_sort["user_id"] == item.user_id][:item.rating_x]
    train_data_df = pd.concat([train_data_df, temp1, temp2])

# In[]:
# 检查抽样情况：
tmp4 = train_data_df.groupby(["user_id", "label"], as_index=False)["rating"].count()
# In[]:
agg = {'rating_count1':lambda x:len(set(x)), 'rating_count2':lambda x:x.nunique()}
tmp5 = tc.groupby_agg_oneCol(tmp4, ["user_id"], "rating", agg, as_index=False)
# In[]:
tmp5[(tmp5["rating_count1"] > 1) | (tmp5["rating_count2"] > 1)]
'''
# In[]:
train_data_df = ds.get_train_data_pos_neg(rating_df, score_df)
# In[]:
train_data_df = train_data_df[["user_id","item_id","label"]]


# In[]:
# 20分钟都跑不出来
def lfm_train(train_data, F, lambd, alpha, step=50):
    '''
    Args:
        F:user vector len, item vector len
        lambd:regularization factor
        alpha:learning rate
        step:iteration unm
    Return:
        user_vec: np.ndarray
        item_vec: np.ndarray
    '''
    user_vec = {}
    item_vec = {}
    time_start = timeit.default_timer() 
    for step_index in range(step):
        for data_instance in train_data:
            userid, itemid, label = data_instance
            if userid not in user_vec:
                user_vec[userid] = init_model(F)
            if itemid not in item_vec:
                item_vec[itemid] = init_model(F)
            # 它的原代码中 下面的 delta...行 和 for index...行 是和上面 for data_instance...行 平级的，这种的话永远只能训练 最后一个 数据： 6040 589 0
#            print(userid, itemid, label)
            delta = label - model_predict(user_vec[userid], item_vec[itemid])
            for index in range(F):
                user_vec[userid][index] +=  alpha*(delta*item_vec[itemid][index] - lambd*user_vec[userid][index])
                item_vec[itemid][index] +=  alpha*(delta*user_vec[userid][index] - lambd*item_vec[itemid][index])
        alpha = alpha * 0.9
    time_end = timeit.default_timer()
    print(time_end - time_start) 
    return user_vec, item_vec


def init_model(vector_len):
    '''
    Args:
        vector_len:the len of vector
    Return:
        a ndarray
    '''
    return np.random.randn(vector_len)
    

def model_predict(user_vector, item_vector):
    res = np.dot(user_vector, item_vector) / (np.linalg.norm(user_vector)*np.linalg.norm(item_vector))
    return res

# In[]:
user_vec, item_vec = lfm_train(train_data, 50, 0.01, 0.1, 50)
print(train_data[-1])
print(user_vec["6040"])
print(item_vec["589"])  

# In[]:
# 指定的user向量 和 所有item向量 相似度计算
def give_recom_result(user_vec, item_vec, userid):
    fix_num = 10
    if userid not in user_vec:
        return {}
    record = {}
    recom_list = []
    user_vector = user_vec[userid]
    for itemid in item_vec:
        item_vector = item_vec[itemid]
        res = np.dot(user_vector, item_vector) / (np.linalg.norm(user_vector)*np.linalg.norm(item_vector))
        record[itemid] = res
    for zuhe in tc.dict_sorted(record)[:fix_num]:
        itemid = zuhe[0]
        score = round(zuhe[1], 3)
        recom_list.append((itemid, score))
    return recom_list
# In[]:
recom_result = give_recom_result(user_vec, item_vec, "24")

# In[]:
def ana_recom_result(train_data, userid, item_info, recom_list):
    '''
    Args:
        train_data: train data for lfm model
        userid: fix userid
        recom_list: recom result by lfm
    '''
    for data_instance in train_data:
        tmp_userid, itemid, label = data_instance
        if tmp_userid == userid and label == 1:
            print(item_info[itemid])    
    print("recom result")
    for zuhe in recom_list:
        print(item_info[zuhe[0]])
# In[]:
ana_recom_result(train_data, "24", recom_result)
        
        
        
        
        
        
        
        
        
        
