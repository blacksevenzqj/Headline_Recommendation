from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)

def stopSpark():
    sc.stop()


# 第一种创建方式：从文件中加载
'''
textFile = sc.textFile("hdfs://hadoop104:9000/user/hadoop/word.txt")
# textFile = sc.textFile("/user/hadoop/input/word.txt")
# textFile = sc.textFile("word.txt")
print(textFile.collect())

pairRDD = textFile.flatMap(lambda line: line.split(" ")).map(lambda word : (word,1))
pairRDD.foreach(print)
'''


# 第二种创建方式：通过并行集合（列表）创建RDD
list = ["Hadoop","Spark","Hive","Spark"]
rdd = sc.parallelize(list)
pairRDD = rdd.map(lambda word : (word,1))
pairRDD.foreach(print)


# 1、reduceByKey(func)
pairRDD.reduceByKey(lambda a,b : a+b).foreach(print)

# 2、groupByKey()
pairRDD.groupByKey().foreach(print)

# 3、keys()
pairRDD.keys().foreach(print)

# 4、values()
pairRDD.values().foreach(print)

# 5、sortByKey()
pairRDD.sortByKey().foreach(print)

# 6、mapValues(func)
pairRDD.mapValues( lambda x : x+1).foreach(print)

# 7、join
pairRDD2 = sc.parallelize([('spark','fast')])
pairRDD.join(pairRDD2).foreach(print)


# 一个综合实例
rdd = sc.parallelize([("spark",2),("hadoop",6),("hadoop",4),("spark",6)])
result = rdd.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])).mapValues(lambda x : (x[0] / x[1])).collect()
print(result)
