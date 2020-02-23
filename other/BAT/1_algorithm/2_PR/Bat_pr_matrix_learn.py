# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 01:26:44 2020

@author: dell

python稀疏矩阵的存储与表示 https://blog.csdn.net/winycg/article/details/80967112
"""

import numpy as np
import pandas as pd
from __future__ import division
import os 
import operator
import timeit

from scipy.sparse import coo_matrix
from scipy.sparse.linalg import gmres

import FeatureTools as ft
ft.set_file_path(r"E:\soft\Anaconda\Anaconda_Python3.6_code\data_analysis\103_Recommend\BAT\data")
import Tools_customize as tc
import Data_Samping as ds

# In[]:
# 构建 M矩阵：
def graph_to_m(graph):
    '''
    Args:
        graph: user item graph 用户评分文件的二分图
    Return:
        m: a coo_matrix, sparse mat M 稀疏矩阵
        vertex: a list, total user and item point 
        address_dict: a dict, map all the point to row index 
    '''
    vertex = list(graph.keys())
    address_dict = {}
    total_len = len(vertex)
    for index in range(len(vertex)):
        address_dict[vertex[index]] = index
    '''
    根据 graph_data、 address_dict数据结构， 再结合 稀疏矩阵m 能看出缘由。
    矩阵的行和列 是 根据 address_dict中节点的位置值而定的。
    '''
    row = []
    col = []
    data = []
    for element_i in graph: # 迭代字典的key
        weight = round(1/len(graph[element_i]), 3)
        row_index = address_dict[element_i]
        for element_j in graph[element_i]:
            col_index = address_dict[element_j]
            row.append(row_index)
            col.append(col_index)
            data.append(weight)
            
    row = np.array(row)
    col = np.array(col)
    data = np.array(data)
    m = coo_matrix((data, (row, col)), shape=(total_len, total_len))
    
    return m, vertex, address_dict

# 构建单位阵E： 求 (E - α * M^T)
def mat_all_point(m_mat, vertex, alpha):
    '''
    get E-alpha*m_mat.T
    Args:
        m_mat:
        vertex: a list, total user and item point 
        alpha: the prob for random walking
    Return:
        a sparse mat
    '''
    total_len = len(vertex) # 所有顶点数
    row = []
    col = []
    data = []
    for index in range(total_len):
        row.append(index)
        col.append(index)
        data.append(1)
    row = np.array(row)
    col = np.array(col)
    data = np.array(data)     
    eye_t = coo_matrix((data, (row, col)), shape=(total_len, total_len))
#    print(eye_t.toarray())
    # 求 (E - α * M^T)
    return eye_t.tocsr() - alpha * m_mat.tocsr().transpose()
   
         
# In[]:
#graph_data = ft.get_graph_from_data("ml-1m/ratings.txt")
graph_data = ft.get_graph_from_data("log.txt", ",")
# In[]:
m, vertex, address_dict = graph_to_m(graph_data)
print(vertex)
print(address_dict)
print(m.toarray())
# In[]:
print(mat_all_point(m, vertex, 0.8))



# In[]:
def personal_rank_mat(graph, root, alpha, recom_num = 10):
    '''
    Args:
        graph: user item graph
        root: the fix user to recom
        alpha: the prob to random walk
        recom_num: recom item num
    Return:
        a dict, key: itemid, value: pr score
    '''
    m, vertex, address_dict = graph_to_m(graph_data)
    if root not in address_dict:
        return {}
    score_dict = {}
    recom_dict = {}
    # 求得 (E - α * M^T)
    mat_all = mat_all_point(m, vertex, alpha)
    
    '''
    续而求 (E - α * M^T)^-1 逆矩阵
    相当于Ax = B线性方程： (E - α * M^T)*(E - α * M^T)^-1 = E（单位阵） 求解x也就是(E - α * M^T)^-1
    另：gmres函数据说在 解Ax = B线性方程时，只有当B是 n行1列矩阵 时才能正确执行。
    
    根据公式： (E - α * M^T)*r = r0 （丢弃(1-α)） 相当于线性方程Ax = B： A为(E - α * M^T)、B为r0（也即单位阵E）；
    求解的x为r，也就是A的逆矩阵(E - α * M^T)^-1。 
    也就符合了最终的公式 r = (E - α * M^T)^-1 * r0 （（丢弃(1-α)）
    '''
    # 初始化 r0矩阵：
    index_root = address_dict[root]
    initial_list = [[0] for index in range(len(vertex))]
    initial_list[index_root] = [1]
    r_zero = np.array(initial_list)
    res = gmres(mat_all, r_zero, tol=1e-8)[0]
    for index in range(len(res)):
        point = vertex[index]
        if len(point.strip().split("_")) < 2: # 不是item节点，过滤掉
            continue
        if point in graph[root]: # 是item节点，但已经和root节点发生过连接，过滤掉
            continue
        score_dict[point] = round(res[index], 3)
    for zuhe in tc.dict_sorted(score_dict)[:recom_num]:
        point, score = zuhe[0], zuhe[1]
        recom_dict[point] = score
    return recom_dict

# In[]:
recom_result = personal_rank_mat(graph=graph_data, root="A", alpha=0.8, recom_num=100)


# In[]:
            

            
            
            