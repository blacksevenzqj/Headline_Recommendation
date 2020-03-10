from pyspark import SparkConf, SparkContext


# In[]
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)


# In[]:
def stopSpark():
    sc.stop()


# In[]:
# stopSpark()

# In[]:
# textFile = sc.textFile("hdfs://hadoop104:9000/user/hadoop/word.txt")
# textFile = sc.textFile("/user/hadoop/input/word.txt")
# textFile = sc.textFile("word.txt")
textFile = sc.textFile("E:\\code\\python_workSpace\\idea_space\\lzy_spark_code\\src\\main\\pyspark\\First\\word.txt")


print(textFile.collect())
wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b).sortByKey(True)
wordCount.foreach(print)
