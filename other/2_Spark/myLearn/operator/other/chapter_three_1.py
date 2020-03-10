from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)

def stopSpark():
    sc.stop()


textFile = sc.textFile("hdfs://hadoop104:9000/user/hadoop/word.txt")
# textFile = sc.textFile("/user/hadoop/input/word.txt")
# textFile = sc.textFile("word.txt")
print(textFile.collect())

spark_count= textFile.filter(lambda line: "Spark" in line).count()
print(spark_count)

# 惰性机制
biggerCount = textFile.map(lambda line : len(line.split(" "))).reduce(lambda a,b : (a > b and a or b))
print(biggerCount)

wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b)
wordCount.foreach(print)


# 持久化
list = ["Hadoop","Spark","Hive"]
rdd = sc.parallelize(list)
rdd.cache()  # 会调用persist(MEMORY_ONLY)，但是，语句执行到这里，并不会缓存rdd，这是rdd还没有被计算生成
print(rdd.count()) # 第一次行动操作，触发一次真正从头到尾的计算，这时才会执行上面的rdd.cache()，把这个rdd放到缓存中
print(','.join(rdd.collect())) # 第二次行动操作，不需要触发从头到尾的计算，只需要重复使用上面缓存中的rdd
# 使用unpersist()方法手动地把持久化的RDD从缓存中移除
rdd.unpersist()


# 分区
# 对于parallelize而言，如果没有在方法中指定分区数，则默认为spark.default.parallelism
array = [1,2,3,4,5]
rdd = sc.parallelize(array,2) #设置两个分区
# 对于textFile而言，如果没有在方法中指定分区数，则默认为min(defaultParallelism,2)，其中，defaultParallelism对应的就是spark.default.parallelism。
lines = sc.textFile("word.txt", 2) #设置两个分区
# 如果是从HDFS中读取文件，则分区数为文件分片数(比如，128MB/片)。

data = sc.parallelize([1,2,3,4,5],2)
print(len(data.glom().collect())) # 显示data这个RDD的分区数量
rdd = data.repartition(1) # 对data这个RDD进行重新分区
print(len(rdd.glom().collect()))