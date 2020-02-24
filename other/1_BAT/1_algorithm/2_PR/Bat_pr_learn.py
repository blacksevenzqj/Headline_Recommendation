# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 13:46:07 2020

@author: dell
"""

import numpy as np
import pandas as pd
import os 
import operator
import timeit

import FeatureTools as ft
ft.set_file_path(r"E:\soft\Anaconda\Anaconda_Python3.6_code\data_analysis\103_Recommend\BAT\data")
import Tools_customize as tc
import Data_Samping as ds

# In[]:
#graph_data = ft.get_graph_from_data("ml-1m/ratings.txt")
graph_data = ft.get_graph_from_data("log.txt", ",")

# In[]:
def personal_rank(graph, root, alpha, iter_num, recom_num=10):
    '''
    Args:
        graph: user item graph
        root: the fixed user for which to recom
        alpha: the prob to go to random walk
        iter_num: iteration num
        recom_num: recom item num
    Return:
        a dict: key itemid, value pr
        
    疑问： 是否有计算的先后顺序问题？
    '''
    rank = {}
    # 除root顶点外，所有节点初始化为0
    rank = {point:0 for point in graph} # 迭代的是dict的key： 所有userid 和 itemid
    rank[root] = 1
    recom_result = {}
    for iter_index in range(iter_num):
        tmp_rank = {}
        tmp_rank = {point:0 for point in graph}
        for out_point, out_dict in graph.items():
#            if iter_index == 0:
#                print("out_point is ", out_point)
            for inner_point, value in graph[out_point].items():
#                if iter_index == 0:
#                    print("inner_point is", inner_point)
                '''
                1、相当于公式的上半部分，如求a的PR值，out_point为A，inner_point为a： 
                由A的PR值 / A的出度(3条出边) * α（当前out_point=A）  +=  由B的PR值 / B的出度(2条出边) * α（后续循环out_point=B）
                
                2.1、相当于公式的下半部分的后部，如求A（root顶点）的PR值，A的出度(3条出边)，out_point为a，inner_point为A：
                由a的PR值 / a的出度(2条出边) * α（当前out_point=a）  +=  由b的PR值 / b的出度(2条出边) * α（后续循环out_point=b）  +=  由d的PR值 / d的出度(2条出边) * α（后续循环out_point=d）
                '''
                tmp_rank[inner_point] += round(alpha * rank[out_point] / len(out_dict), 4)
                
                '''
                2.2、相当于公式的下半部分的前部，如求A（root顶点）的PR值： A的出度(3条出边)，out_point为a，inner_point为A：
                A的3条出边连接节点都会以（1-alpha）的概率回到A本身，包括： a的1-alpha（当前out_point=a）  +=  b的1-alpha（后续循环out_point=b）  +=  d的1-alpha（后续循环out_point=d）   
                '''
                if inner_point == root:
                    tmp_rank[inner_point] += round(1-alpha, 4)
#            if iter_index == 0:
#                print()
                
        if tmp_rank == rank:
            print("out" + str(iter_index))
            break
        rank = tmp_rank
    
#    print(rank)
    right_num = 0
    for zuhe in tc.dict_sorted(rank):
        point, pr_score = zuhe[0], zuhe[1]
        if len(point.split("_")) < 2: # 不是item节点，过滤掉
            continue
        if point in graph[root]: # 是item节点，但已经和root节点发生过连接，过滤掉
            continue
        recom_result[point] = pr_score
        right_num += 1
        if right_num > recom_num:
            break
        
    return recom_result
    
# In[]:
#recom_result = personal_rank(graph_data, "A", 0.8, 100, 10)
recom_result = personal_rank(graph_data, "1", 0.8, 100, 10)
# In[]:
item_info = ft.get_item_info('ml-1m/movies.txt')
# In[]:
print(len(graph_data["1"]))
for itemid in graph_data["1"]:
    pure_itemid = itemid.split("_")[1]
    print(pure_itemid, item_info[pure_itemid])
# In[]:
print("result:")
for itemid in recom_result:
    pure_itemid = itemid.split("_")[1]
    print(pure_itemid, item_info[pure_itemid], recom_result[itemid])
# In[]:
rank111 = {point:0 for point in graph_data}
# In[]:
#kkk = {}
#kkk["a"] = {}
#kkk["a"]["aaa"] = 11
#kkk["a"]["ccc"] = 33
#kkk["b"] = {}
#kkk["b"]["bbb"] = 22
print({point:0 for point in graph_data})
print()
for out_point, out_dict in graph_data.items():
    print(out_point, out_dict)
    for inner_point, value in graph_data[out_point].items():
        print(inner_point, value)
    print()
    
    
    
    