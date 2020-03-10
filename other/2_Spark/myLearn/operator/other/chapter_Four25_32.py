from pyspark import SparkContext
sc = SparkContext( 'local', 'test')
# logFile = "C:\\Users\\dell\\Desktop\\Python的临时记录.txt"
# logData = sc.textFile(logFile, 2).cache()
# # logData.foreach(print)
# numAs = logData.filter(lambda line: 'C' in line).count()
# numBs = logData.filter(lambda line: 'INFO' in line).count()
# print('Lines with C: %s, Lines with INFO: %s' % (numAs, numBs))

# textFile = sc.textFile("hdfs://192.168.43.230:9000/user/hadoop/word.txt")
# textFile.foreach(print)
# textFile = sc.textFile("/user/hadoop/word.txt")
# textFile = sc.textFile("word.txt")


# 转换操作：
# filter
# lines = sc.textFile("word.txt", 2).cache()
# words = lines.filter(lambda line : "Spark" in line)
# words.foreach(print)

# map
# lines = sc.textFile("word.txt", 2).cache()
# words = lines.map(lambda line : line.split(" "))
# words.foreach(print)

# flatMap
# lines = sc.textFile("word.txt", 2).cache()
# words = lines.flatMap(lambda line : line.split(" "))
# words.foreach(print)

# groupByKey
# words = sc.parallelize([("Hadoop",1),("is",1),("Good",1),
#                         ("Spark",1),("is",1),("Good",1),
#                         ("Spark",1),("is",1),("better",1)
#                         ])
# words1 = words.groupByKey()
# words1.foreach(print)

# reduceByKey
# words = sc.parallelize([("Hadoop",1),("is",1),("Good",1),
#                         ("Spark",1),("is",1),("Good",1),
#                         ("Spark",1),("is",1),("better",1)
#                         ])
# words1 = words.reduceByKey(lambda a,b:a+b)
# words1.foreach(print)


# 行动操作
# rdd = sc.parallelize([1,2,3,4,5])
# rdd.count()
# rdd.first()
# rdd.take(3) # 数量
# rdd.reduce(lambda a,b:a+b) # 累加
# rdd.collect()
# rdd.foreach(lambda elem:print(elem))


# 惰性机制
# lines = sc.textFile("word.txt")
# lineLengths = lines.map(lambda s : len(s))
# totalLength = lineLengths.reduce( lambda a, b : a + b)

# 实例
'''
最后，针对这个RDD[Int]，调用reduce()行动操作，完成计算。reduce()操作每次接收两个参数，取出较大者留下，然后再继续比较，
例如，RDD[Int]中包含了1,2,3,4,5，那么，执行reduce操作时，首先取出1和2，把a赋值为1，把b赋值为2，然后，执行大小判断，保留2。
下一次，让保留下来的2赋值给a，再从RDD[Int]中取出下一个元素3，把3赋值给b，然后，对a和b执行大小判断，保留较大者3.依此类推。
最终，reduce()操作会得到最大值是5
'''
# lines = sc.textFile("word.txt")
# maxline = lines.map(lambda line : len(line.split(" "))).reduce(lambda a,b : (a > b and a or b))
# print(maxline)


# 持久化（需要显示设置）
# list = ["Hadoop","Spark","Hive"]
# rdd = sc.parallelize(list)
# rdd.cache()  # 会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd，这时rdd还没有被计算生成
# print(rdd.count()) # 第一次行动操作，触发一次真正从头到尾的计算，这时才会执行上面的rdd.cache()，把这个rdd放到缓存中
# print(','.join(rdd.collect())) # 第二次行动操作，不需要触发从头到尾的计算，只需要重复使用上面缓存中的rdd
# # 最后，可以使用unpersist()方法手动地把持久化的RDD从缓存中移除。


# 分区
# # 对于parallelize而言，如果没有在方法中指定分区数，则默认为spark.default.parallelism
# array = [1,2,3,4,5]
# rdd = sc.parallelize(array,2) #设置两个分区
# # 对于textFile而言，如果没有在方法中指定分区数，则默认为min(defaultParallelism,2)，其中，defaultParallelism对应的就是spark.default.parallelism。
# lines = sc.textFile("word.txt", 2) #设置两个分区
# # 如果是从HDFS中读取文件，则分区数为文件分片数(比如，128MB/片)。
#
# data = sc.parallelize([1,2,3,4,5],2)
# print(len(data.glom().collect())) # 显示data这个RDD的分区数量
# rdd = data.repartition(1) # 对data这个RDD进行重新分区
# print(len(rdd.glom().collect()))# 对于parallelize而言，如果没有在方法中指定分区数，则默认为spark.default.parallelism
# array = [1,2,3,4,5]
# rdd = sc.parallelize(array,2) #设置两个分区
# # 对于textFile而言，如果没有在方法中指定分区数，则默认为min(defaultParallelism,2)，其中，defaultParallelism对应的就是spark.default.parallelism。
# lines = sc.textFile("word.txt", 2) #设置两个分区
# # 如果是从HDFS中读取文件，则分区数为文件分片数(比如，128MB/片)。
#
data = sc.parallelize([1,2,3,4,5],2)
print(len(data.glom().collect())) # 显示data这个RDD的分区数量
rdd = data.repartition(1) # 对data这个RDD进行重新分区
print(len(rdd.glom().collect()))

# #自定义分区
# from pyspark import  SparkConf, SparkContext
#
# def MyPartitioner(key):
#     print("MyPartitioner is running")
#     print("The key is %d" % key)
#     return key%10
#
# def main():
#     print("The main function is running")
#     conf = SparkConf().setMaster("local").setAppName("MyApp")
#     sc = SparkContext(conf = conf)
#     data = sc.parallelize(range(10), 5)
#     # 必须是 键值对.partitionBy，其实是 元组.partitionBy。
#     # partitionBy会取出 元组的key，作为分区值，所以用自定义函数MyPartitioner接收元组的key，做自定义分区操作。
#     data.map(lambda x:(x,1)) \
#             .partitionBy(10, MyPartitioner)\
#             .map(lambda x:x[0])\
#             .saveAsTextFile("E:\soft\Large_Data\my_temp") # 文件路径必须 不存在（郝建）
#
# if __name__ == '__main__':
#     main()

# 两种提交方式：
# 1、python3 Chapter_Four.py
# 2、spark-submit Chapter_Four.py