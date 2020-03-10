from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
from pyspark.sql.context import SQLContext, HiveContext, UDFRegistration
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame, DataFrameNaFunctions, DataFrameStatFunctions
from pyspark.sql.group import GroupedData
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.window import Window, WindowSpec


conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
spark = SQLContext(sc)

def stopSpark():
    sc.stop()

def f(x):
    rel = {}
    rel['name'] = x[0]
    rel['age'] = x[1]
    return rel

people = sc.textFile("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.txt")\
    .map(lambda line : line.split(','))\
    .map(lambda p: Row(name=p[0], age=int(p[1])))
peopleDF = spark.createDataFrame(people)

# peopleDF = sc.textFile("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.txt")\
#     .map(lambda line : line.split(','))\
#     .map(lambda x: Row(**f(x))).toDF()

peopleDF.createOrReplaceTempView("people")  # 必须注册为临时表才能供下面的查询使用
personsDF = spark.sql("select name,age from people where age>20")
personsDF.rdd.map(lambda t : "Name:"+t[0]+","+"Age:"+str(t[1])).foreach(print)