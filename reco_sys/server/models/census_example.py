# -*- coding: UTF-8 -*-

import numpy as np
import tensorflow as tf
import functools

# tensorflow1的版本
print(tf.__version__)


# In[]:
_CSV_COLUMN_DEFAULTS = [[0], [''], [0], [''], [0], [''], [''], [''], [''], [''],
                        [0], [0], [0], [''], ['']]

_CSV_COLUMNS = [
    'age', 'workclass', 'fnlwgt', 'education', 'education_num',
    'marital_status', 'occupation', 'relationship', 'race', 'gender',
    'capital_gain', 'capital_loss', 'hours_per_week', 'native_country',
    'income_bracket'
]


train_file = "E:\\code\\python_workSpace\\idea_space\\toutiao_project\\reco_sys\\server\\models\\data\\adult.data"
test_file = "E:\\code\\python_workSpace\\idea_space\\toutiao_project\\reco_sys\\server\\models\\data\\adult.test"


def input_func(file, epoches, batch_size):
    """
    解普查数据csv格式样本
    """
    def deal_with_csv(value):
        data = tf.decode_csv(value, record_defaults=_CSV_COLUMN_DEFAULTS) # list
        # 构建列名称与这一行值的字典数据
        feature_dict = dict(zip(_CSV_COLUMNS, data))
        labels = feature_dict.pop('income_bracket') # Tensor("DecodeCSV:14", shape=(), dtype=string)
        classes = tf.equal(labels, '>50K')
        return feature_dict, classes

    # 1、读取美国普查收入数据
    # tensor的迭代，一行样本数据
    # 名称要制定
    # 39, State-gov, 77516,Bachelors, 13, , Adm-clerical
    dataset = tf.data.TextLineDataset(file) # DatasetV1Adapter 理解为Tensor迭代器
    print(dataset)
    dataset = dataset.map(deal_with_csv) # 只调用了一次，包含了feature_dict, classes
    print(dataset)
    dataset = dataset.repeat(epoches) # 训练时重复次数
    dataset = dataset.batch(batch_size)
    return dataset


def get_feature_column():
    """
    指定输入extimator中特征列类型
    :return:
    """
    # 数值型特征
    age = tf.feature_column.numeric_column('age')
    education_num = tf.feature_column.numeric_column('education_num')
    capital_gain = tf.feature_column.numeric_column('capital_gain')
    capital_loss = tf.feature_column.numeric_column('capital_loss')
    hours_per_week = tf.feature_column.numeric_column('hours_per_week')

    numeric_columns = [age, education_num, capital_gain, capital_loss, hours_per_week]

    # 类别型特征
    # categorical_column_with_vocabulary_list, 将字符串转换成ID
    relationship = tf.feature_column.categorical_column_with_vocabulary_list(
        'relationship',
        ['Husband', 'Not-in-family', 'Wife', 'Own-child', 'Unmarried', 'Other-relative'])

    marital_status = tf.feature_column.categorical_column_with_vocabulary_list(
        'marital_status', [
            'Married-civ-spouse', 'Divorced', 'Married-spouse-absent',
            'Never-married', 'Separated', 'Married-AF-spouse', 'Widowed'])

    workclass = tf.feature_column.categorical_column_with_vocabulary_list(
        'workclass', [
            'Self-emp-not-inc', 'Private', 'State-gov', 'Federal-gov',
            'Local-gov', '?', 'Self-emp-inc', 'Without-pay', 'Never-worked'])

    # categorical_column_with_hash_bucket--->哈希列
    # 对不确定类别数量以及字符时，哈希列进行分桶
    occupation = tf.feature_column.categorical_column_with_hash_bucket(
        'occupation', hash_bucket_size=1000)

    categorical_columns = [relationship, marital_status, workclass, occupation]

    return numeric_columns + categorical_columns


'''
特征交叉（连续特征分桶之后进行特征组合）
'''
def get_feature_column_v2():
    age = tf.feature_column.numeric_column('age')
    education_num = tf.feature_column.numeric_column('education_num')
    capital_gain = tf.feature_column.numeric_column('capital_gain')
    capital_loss = tf.feature_column.numeric_column('capital_loss')
    hours_per_week = tf.feature_column.numeric_column('hours_per_week')

    numeric_columns = [age, education_num, capital_gain, capital_loss, hours_per_week]

    # 类别型特征
    # categorical_column_with_vocabulary_list, 将字符串转换成ID
    relationship = tf.feature_column.categorical_column_with_vocabulary_list(
        'relationship',
        ['Husband', 'Not-in-family', 'Wife', 'Own-child', 'Unmarried', 'Other-relative'])

    marital_status = tf.feature_column.categorical_column_with_vocabulary_list(
        'marital_status', [
            'Married-civ-spouse', 'Divorced', 'Married-spouse-absent',
            'Never-married', 'Separated', 'Married-AF-spouse', 'Widowed'])

    workclass = tf.feature_column.categorical_column_with_vocabulary_list(
        'workclass', [
            'Self-emp-not-inc', 'Private', 'State-gov', 'Federal-gov',
            'Local-gov', '?', 'Self-emp-inc', 'Without-pay', 'Never-worked'])

    # categorical_column_with_hash_bucket--->哈希列
    # 对不确定类别数量以及字符时，哈希列进行分桶
    occupation = tf.feature_column.categorical_column_with_hash_bucket(
        'occupation', hash_bucket_size=1000)
    categorical_columns = [relationship, marital_status, workclass, occupation]

    # 分桶，交叉特征
    age_buckets = tf.feature_column.bucketized_column(
        age, boundaries=[18, 25, 30, 35, 40, 45, 50, 55, 60, 65])
    crossed_columns = [
        tf.feature_column.crossed_column(
            ['education', 'occupation'], hash_bucket_size=1000),
        tf.feature_column.crossed_column(
            [age_buckets, 'education', 'occupation'], hash_bucket_size=1000),
    ]

    return numeric_columns + categorical_columns + crossed_columns



def parser(a):
    features = {"feature": a[:, 0:9]}
    y = a[:, 9]
    return features, y

def test():
    """
    API测试
    :return:
    """
    a = tf.random_normal([4, 10])
    print(a.shape, a.shape.as_list()) # a.ndim错误API
    print(a)
    dataset1 = tf.data.Dataset.from_tensor_slices(a)
    print(dataset1.output_shapes) # 取出的是一个Dataset
    print(dataset1.output_types)
    dataset1 = dataset1.shuffle(buffer_size=100)
    dataset1 = dataset1.batch(2) # 一次取2行数据
    dataset1 = dataset1.map(parser)
    dataset1 = dataset1.repeat()
    print(dataset1)
    
    iterator = dataset1.make_initializable_iterator()
    next_element = iterator.get_next()
    with tf.Session() as sess:
        sess.run(iterator.initializer)
        for i in range(2):
            print(sess.run(next_element)) # 一次取2行数据，取了2次
    
    print("-"*30)
    
    
    dataset2 = tf.data.Dataset.from_tensor_slices({"f": tf.random_normal([4, 10]),
                                                    "l": tf.random_normal([4])})
    print(dataset2.output_shapes) # 取出的是一个Dataset
    print(dataset2.output_types)
    print("-"*30)


    dataset3 = tf.data.Dataset.range(100) # 给一个值
    iterator = dataset3.make_one_shot_iterator() # 不常用 dataset3.make_initializable_iterator
    example = iterator.get_next()
    with tf.Session() as sess:
        for i in range(10):
            print(sess.run(example))
    print("-"*30)
    
    
    features = {'SepalLength': np.array([6.4, 5.0]),
              'SepalWidth':  np.array([2.8, 2.3]),
              'PetalLength': np.array([5.6, 3.3]),
              'PetalWidth':  np.array([2.2, 1.0])}
     
    labels = np.array([2, 1])
    
    dataset = tf.data.Dataset.from_tensor_slices((features, labels))
    dataset = dataset.shuffle(1000).repeat().batch(2) # 一次取2行数据
    print(dataset)
    example = dataset.make_one_shot_iterator().get_next()
    with tf.Session() as sess:
            for i in range(4):
                print(sess.run(example)) # 一次取2行数据，取了4次
    print("-"*30)



def version1():
    # 特征列：
    feature_cl = get_feature_column()
    # 构造模型
    classifiry = tf.estimator.LinearClassifier(feature_columns=feature_cl)
    # train输入的input_func，不能调用传入
    # 1、input_func，构造的时候不加参数，但是这样不灵活， 里面参数不能固定的时候
    # 2、functools.partial
    train_func = functools.partial(input_func, train_file, epoches=3, batch_size=32)
    test_func = functools.partial(input_func, test_file, epoches=1, batch_size=32)
    classifiry.train(train_func)
    result = classifiry.evaluate(test_func)
    print(result)


def version2():
    # 分桶与特征交叉
    # 构造模型
    feature_v2 = get_feature_column_v2()
    # classifiry = tf.estimator.LinearClassifier(feature_columns=feature_v2)
    # 加入正则化项
    classifiry = tf.estimator.LinearClassifier(feature_columns=feature_v2,
                                               optimizer=tf.train.FtrlOptimizer(
                                                   learning_rate=0.01,
                                                   l1_regularization_strength=10,
                                                   l2_regularization_strength=15,
                                               ))

    # train输入的input_func，不能调用传入
    # 1、input_func，构造的时候不加参数，但是这样不灵活， 里面参数不能固定的时候
    # 2、functools.partial
    train_func = functools.partial(input_func, train_file, epoches=3, batch_size=32)
    test_func = functools.partial(input_func, test_file, epoches=1, batch_size=32)
    classifiry.train(train_func)
    result = classifiry.evaluate(test_func)
    print(result)



if __name__ == '__main__':
    import sys
    print(sys.version)
    print(sys.version_info)

    # dataset = input_func(train_file, 3, 32)

    # version1()

    # version2()

    # test()
