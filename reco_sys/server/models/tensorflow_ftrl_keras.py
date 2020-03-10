import tensorflow as tf
from tensorflow.python import keras

# keras版本代码完整代码（没有跑）
class LrWithFtrl(object):
    """LR以FTRL方式优化
    """
    def __init__(self):
        self.model = keras.Sequential([
            keras.layers.Dense(1, activation='sigmoid', input_shape=(121,))
        ])

    @staticmethod
    def read_ctr_records():
        # 定义转换函数,输入时序列化的
        def parse_tfrecords_function(example_proto):
            features = {
                "label": tf.FixedLenFeature([], tf.int64),
                "feature": tf.FixedLenFeature([], tf.string)
            }
            parsed_features = tf.parse_single_example(example_proto, features)

            feature = tf.decode_raw(parsed_features['feature'], tf.float64)
            feature = tf.reshape(tf.cast(feature, tf.float32), [1, 121])
            label = tf.reshape(tf.cast(parsed_features['label'], tf.float32), [1, 1])
            return feature, label

        dataset = tf.data.TFRecordDataset(["./train_ctr_201904.tfrecords"])
        dataset = dataset.map(parse_tfrecords_function)
        dataset = dataset.shuffle(buffer_size=10000)
        dataset = dataset.repeat(10000)
        return dataset


    def train(self, dataset):
        self.model.compile(optimizer=tf.train.FtrlOptimizer(0.03, l1_regularization_strength=0.01,
                                                            l2_regularization_strength=0.01),
                           loss='binary_crossentropy',
                           metrics=['binary_accuracy'])
        self.model.fit(dataset, steps_per_epoch=10000, epochs=10)
        self.model.summary()
        self.model.save_weights('./ckpt/ctr_lr_ftrl.h5')


    def predict(self, inputs):
        """预测
        :return:
        """
        # 首先加载模型
        self.model.load_weights('/root/toutiao_project/reco_sys/offline/models/ckpt/ctr_lr_ftrl.h5')
        init = tf.global_variables_initializer()

        with tf.Session() as sess:
            sess.run(init)
            predictions = self.model.predict(sess.run(inputs))
        return predictions



# 在线预测
def lrftrl_sort_service(reco_set, temp, hbu):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    :return:
    """
    print(344565)
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []
        logger.info("{} WARN get user user_id:{} channel:{} profile data failed".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))

    reco_set = [13295, 44020, 14335, 4402, 2, 14839, 44024, 18427, 43997, 17375] # 召回集
    if user_feature and reco_set:
        # 2、读取文章特征中心特征
        result = []
        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = []

            if not article_feature:
                article_feature = [0.0] * 111
            f = []
            f.extend(user_feature)
            f.extend(article_feature)

            result.append(f)

        # 4、预测并进行排序是筛选
        arr = np.array(result)

        # 加载逻辑回归模型
        lwf = LrWithFtrl()
        print(tf.convert_to_tensor(np.reshape(arr, [len(reco_set), 121])))
        predictions = lwf.predict(tf.constant(arr))

        df = pd.DataFrame(np.concatenate((np.array(reco_set).reshape(len(reco_set), 1), predictions),
                                          axis=1),
                          columns=['article_id', 'prob'])

        df_sort = df.sort_values(by=['prob'], ascending=True)

        # 排序后，只将排名在前100个文章ID返回给用户推荐
        if len(df_sort) > 100:
            reco_set = list(df_sort.iloc[:100, 0])
        reco_set = list(df_sort.iloc[:, 0])

    return reco_set



if __name__ == '__main__':
    lwf = LrWithFtrl()
    dataset = lwf.read_ctr_records()
    inputs, labels = dataset.make_one_shot_iterator().get_next()
    print(inputs, labels)
    lwf.predict(inputs)

