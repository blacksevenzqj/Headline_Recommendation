# -*- coding: UTF-8 -*-

import tensorflow as tf
from tensorflow import keras

# tensorflow的版本
print(tf.__version__)
print(keras.__version__)
import sys
print(sys.version)
print(sys.version_info)


# 词库的词数量大小
vocab_size = 5000
# 最大序列长度
max_sentence = 200


def get_train_test():
    """
    获取电影评论文本数据
    :return:
    """
    imdb = keras.datasets.imdb

    # 词库的词数量为 vocab_size = 5000
    (x_train_source, y_train), (x_test_source, y_test) = imdb.load_data(num_words=vocab_size)

    # 每个样本（评论）词的数量不是统一的，也就是样本的特征个数不统一，所以要固定序列（特征）长度，以序列长度最长的为准。
    # 每个样本（评论）序列长度固定：矩阵的 行数量为样本数量， 列数量为 maxlen； 每个元素的值（序号）是由 词库所决定。
    x_train = keras.preprocessing.sequence.pad_sequences(x_train_source,
                                                         maxlen=max_sentence,
                                                         padding='post', value=0)
    
    x_test = keras.preprocessing.sequence.pad_sequences(x_test_source,
                                                         maxlen=max_sentence,
                                                         padding='post', value=0)

    return (x_train, y_train), (x_test, y_test)


if __name__ == '__main__':
    # - 1、电影评论数据读取
    (x_train, y_train), (x_test, y_test) = get_train_test()
    print(x_train.shape, y_train.shape, len(x_train), x_test.shape, y_test.shape, len(x_test))
    print(x_train)
    print(y_train)
    # print(x_test)
    

    def parser(x, y):
        features = {"feature": x}
        return features, y


    def train_input_fn():
        dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
        dataset = dataset.shuffle(buffer_size=25000)
        dataset = dataset.batch(64)
        dataset = dataset.map(parser)
        dataset = dataset.repeat()
        print(dataset)
        iterator = dataset.make_one_shot_iterator()
        return iterator.get_next()


    def eval_input_fn():
        dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))
        dataset = dataset.batch(64)
        dataset = dataset.map(parser)
        iterator = dataset.make_one_shot_iterator()
        return iterator.get_next()


    # 测试：数据集
    # example = train_input_fn()
    # with tf.Session() as sess:
    #     for i in range(1):
    #         print(sess.run(example))


    # - 2、模型输入特征列指定：
    # 和 imdb.load_data(num_words=vocab_size) 大小相同，词库的词数量。
    column = tf.feature_column.categorical_column_with_identity('feature', vocab_size)

    # 词向量长度大小
    embedding_column = tf.feature_column.embedding_column(column, dimension=50) # 词向量大小为50维

    # - 3、模型训练与保存
    classifier = tf.estimator.DNNClassifier(
        hidden_units=[100], # 神经元的每层的数量
        feature_columns=[embedding_column],
        model_dir="tmp/text_embedding/"
    )

    classifier.train(input_fn=train_input_fn)
    result = classifier.evaluate(input_fn=eval_input_fn)
    print(result)
