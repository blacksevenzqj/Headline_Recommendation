# -*- coding: utf-8 -*-
"""
Created on Sun Jan  5 09:48:31 2020

@author: dell
"""

import numpy as np
import pandas as pd
import os 
import operator
import timeit
import matplotlib.pyplot as plt

import FeatureTools as ft
ft.set_file_path(r"E:\soft\Anaconda\Anaconda_Python3.6_code\data_analysis\103_Recommend\BAT\data\Adult")
import Tools_customize as tc
import Data_Samping as ds

# In[]:
names = ["age", "workclass", "fnlwgt", "education_level", "education-num", "marital-status",
         "occupation", "relationship", "race", "sex", "capital-gain", 
         "capital-loss", "hours-per-week", "native-country", "income"]
dtype = {
            "age":np.int32,
            "education-num":np.int32,
            "capital-gain":np.int32,
            "capital-loss":np.int32,
            "hours-per-week":np.int32
         }

#usecols = ["age", "workclass", "education_level", "education-num", "marital-status",
#         "occupation", "relationship", "race", "sex", "capital-gain", 
#         "capital-loss", "hours-per-week", "native-country", "income"]
usecols = list(range(15))
usecols.remove(2)
# In[]:
train_data = ft.readFile_inputData_no_header("adult.data", names, na_values=" ?", dtype=dtype, usecols=usecols)
# In[]:
ft.missing_values_table(train_data)
# In[]:
mis_val_table_ren_columns, train_data = ft.missing_values_table(train_data, customize_axis=0, percent=0, del_type=2)
# In[]:
ft.category_remove_spaces(train_data)

# In[]:
test_data = ft.readFile_inputData_no_header("adult.test", names, header=0, na_values=" ?", dtype=dtype, usecols=usecols)
# In[]:
ft.missing_values_table(test_data)
# In[]:
mis_val_table_ren_columns, test_data = ft.missing_values_table(test_data, customize_axis=0, percent=0, del_type=2)
# In[]:
ft.category_remove_spaces(test_data)
# In[]:
test_data["income"] = ft.category_remove_spaces(test_data["income"], del_str=".")

# In[]:
print(set(train_data["income"]))
unique_label, counts_label, unique_dict = ft.category_quantity_statistics_all(train_data, "income")
# In[]:
print(set(test_data["income"]))
unique_label, counts_label, unique_dict = ft.category_quantity_statistics_all(test_data, "income")
# In[]:
maps = {"<=50K" : 0, ">50K" : 1}
# In[]:
train_data["income"] = ft.category_manual_coding(train_data["income"], maps)
# In[]:
test_data["income"] = ft.category_manual_coding(test_data["income"], maps)


# In[]:
# 分类变量：
print(set(train_data["native-country"]))
unique_label, counts_label, unique_dict = ft.category_quantity_statistics_all(train_data, "native-country")
# In[]:
print(set(test_data["workclass"]))
unique_label, counts_label, unique_dict = ft.category_quantity_statistics_all(test_data, "workclass")
# In[]:
train_data_1 = train_data.copy()
test_data_1 = test_data.copy()
# In[]:
ft.category_customize_coding(train_data_1, test_data_1)


# In[]:
# 连续变量
f, axes = plt.subplots(2,2, figsize=(20, 18))
ft.class_data_distribution(train_data_1, "age", "income", axes)
# In[]:
ft.class_data_with_y_scatter(train_data_1, "age", "income")
# In[]:
ft.box_whisker_diagram_Interval(train_data_1["age"])
# In[]:
f, axes = plt.subplots(2,1, figsize=(20, 18))
ft.con_data_distribution(train_data_1, "age", axes)
# In[]:
train_data_1["age"].describe()




# In[]:
# In[]:
# In[]:

# In[]:
# In[]:
# In[]:

