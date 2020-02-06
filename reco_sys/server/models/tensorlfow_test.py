import tensorflow as tf
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'


def tensorflow_demo():
    """
    通过简单案例来了解tensorflow的基础结构
    :return: None
    """
    # 一、原生python实现加法运算
    # a = 10
    # b = 20
    # c = a + b
    # print("原生Python实现加法运算方法1：\n", c)
    # def add(a, b):
    #     return a + b
    # sum = add(a, b)
    # print("原生python实现加法运算方法2：\n", sum)
    # 二、tensorflow实现加法运算
    a_t = tf.constant(10, name='a_t')
    b_t = tf.constant(20, name='b_t')
    # 不提倡直接运用这种符号运算符进行计算
    # 更常用tensorflow提供的函数进行计算
    # c_t = a_t + b_t
    c_t = tf.add(a_t, b_t, name='add')
    print("tensorflow实现加法运算：\n", c_t)
    print(tf.get_default_graph())

    # 创建一个新图，去做加法运算
    new_g = tf.Graph()
    with new_g.as_default():
        new_a = tf.constant(10)
        new_b = tf.constant(20)
        # 不提倡直接运用这种符号运算符进行计算
        # 更常用tensorflow提供的函数进行计算
        # c_t = a_t + b_t
        new_t = tf.add(new_a, new_b)

    # 打印Tensor的图
    print(tf.get_default_graph())
    print("默认的图：", a_t.graph, b_t.graph)
    print("创建的新图：", new_a.graph, new_b.graph)

    # 如何让计算结果出现？
    # 开启会话
    with tf.Session(config=tf.ConfigProto(allow_soft_placement=True,log_device_placement=True)) as sess:
        print(sess.graph)
        print(b_t.eval())
        c_t = sess.run(c_t)
        print("在sess当中的sum_t:\n", c_t)
        # 必须session上下文环境中
        # print(c_t.eval())

        # 将图保存到文件
        file_writer = tf.summary.FileWriter("./tmp/", graph=sess.graph)

    return None


def tensor_test():

    # tensor1 = tf.constant(4.0)
    # tensor2 = tf.constant([1, 2, 3, 4])
    # linear_squares = tf.constant([[4], [9], [16], [25]], dtype=tf.int32)
    # print(tensor1, tensor2, linear_squares)

    # # 定义占位符
    # a_p = tf.placeholder(dtype=tf.float32, shape=[None, None])
    # b_p = tf.placeholder(dtype=tf.float32, shape=[None, 10])
    # c_p = tf.placeholder(dtype=tf.float32, shape=[3, 2])
    #
    # # set_shape
    # a_p.set_shape([4, 5])
    # print(a_p)
    # # 静态形状
    # # 转换静态形状的时候，1-D到1-D，2-D到2-D，不能跨阶数改变形状
    # # 对于已经固定的张量的静态形状的张量，不能再次设置静态形状
    # # a_p.set_shape([5, 4])
    # # 使用tf.reshape
    # new_a_p = tf.reshape(a_p, [5, 4])
    # print(new_a_p)
    # # tf.reshape()动态创建新张量时，张量的元素个数必须匹配
    # print(a_p)

    # 变量OP

    a = tf.Variable(initial_value=30)
    b = tf.Variable(initial_value=40)
    sum = tf.add(a, b)
    # print(sum, a, b)
    # 初始化变量
    init = tf.global_variables_initializer()

    with tf.Session() as sess:
        sess.run(init)
        print(sess.run([sum, a, b]))
    return None


tf.app.flags.DEFINE_integer("max_step", 0, "模型运行次数")

FLAGS = tf.app.flags.FLAGS


def my_linearmodel():
    """
    Tensorflow实现简单的线性回归
    :return:
    """
    # - 1 准备好数据集：y = 0.8x + 0.7 100个样本
    #   - 特征值，和目标值
    # 100个样本， 1特征
    X = tf.random_normal([100, 1], mean=1.0, stddev=1.0)
    # x [100, 1] x [1, 1] = [100, 1]
    y_true = tf.matmul(X, [[0.8]]) + 0.7

    # - 2 建立线性模型
    #   - 随机初始化W1和b1
    #   - y = X x w + b，目标：求出权重W和偏置b
    # X 只有一个特征，线性回归 一个权重+一个偏置
    # 可训练，tf.Variable
    # 随机初始化权重
    weigths = tf.Variable(initial_value=tf.random_normal([1, 1]), trainable=True)
    bias = tf.Variable(initial_value=tf.random_normal([1, 1]), trainable=True)
    y_predict = tf.matmul(X, weigths) + bias

    # - 3 确定损失函数（预测值与真实值之间的误差）-均方误差
    # 100个样本都有100个差的平方[,,,,,,,,,,,]
    # 损失是一个值，100个样本的损失的平均值
    loss = tf.reduce_mean(tf.square(y_predict - y_true))

    # - 4 梯度下降优化损失：需要指定学习率（超参数）
    optimizer = tf.train.GradientDescentOptimizer(0.1).minimize(loss)

    # 2、收集Tensor
    tf.summary.scalar('losses', loss)
    tf.summary.histogram('W', weigths)
    tf.summary.histogram('B', bias)

    # 3、合并变量
    merge = tf.summary.merge_all()

    init = tf.global_variables_initializer()

    saver = tf.train.Saver()
    # 训练优化
    with tf.Session() as sess:
        # 第一初始化变量
        sess.run(init)

        print("优化开始前的参数权重：{}, 偏置：{}".format(weigths.eval(), bias.eval()))
        # 保存模型的观察
        file_writer = tf.summary.FileWriter("./tmp/", graph=sess.graph)

        # 加载训练过的模型
        saver.restore(sess, "./tmp/ckpt/linearRegression.ckpt")

        print("优化开始前的参数权重：{}, 偏置：{}".format(weigths.eval(), bias.eval()))

        # 训练
        for i in range(FLAGS.max_step):
            sess.run(optimizer)
            # 打印优化的结果
            print("优化第{}次数：之后的的参数权重：{}, 偏置：{}, 损失为：{}".format(i, weigths.eval(), bias.eval(), loss.eval()))

            #  4、运行这几个tensor的值
            summary = sess.run(merge)
            file_writer.add_summary(summary, i)

            #保存模型
            if i % 100 == 0:
                saver.save(sess, "./tmp/ckpt/linearRegression.ckpt")

    return None


if __name__ == '__main__':
    # tensorflow_demo()
    # tensor_test()
    my_linearmodel()