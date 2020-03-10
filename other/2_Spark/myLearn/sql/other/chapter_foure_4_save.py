from pyspark import SparkConf, SparkContext
from pyspark.sql.types import Row
from pyspark.sql.context import SQLContext, HiveContext, UDFRegistration
from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame, DataFrameNaFunctions, DataFrameStatFunctions
from pyspark.sql.group import GroupedData
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType


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


# 第一种方式：（直接保存DataFrame）
peopleDF = spark.read.format("json").load("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.json")

'''
write.format()支持输出 json,parquet, jdbc, orc, libsvm, csv, text等格式文件，如果要输出文本文件，可以采用write.format(“text”)，
但是，需要注意，只有select()中只存在一个列时，才允许保存成文本文件，如果存在两个列，比如select(“name”, “age”)，就不能保存成文本文件。
'''
peopleDF.select("name", "age").write.format("csv").save("file:///usr/local/my_soft/tmp_code/newpeople.csv")

# newpeople.csv文件夹（注意，不是文件），这个文件夹中包含下面两个文件。直接读取newpeople.csv文件夹。
textFile = sc.textFile("file:///usr/local/my_soft/tmp_code/newpeople.csv")
textFile.foreach(print)



# 第二种方式：（DataFrame转换为RDD后保存）
peopleDF = spark.read.format("json").load("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.json")
peopleDF.rdd.saveAsTextFile("file:///usr/local/my_soft/tmp_code/newpeople.txt")

textFile = sc.textFile("file:///usr/local/my_soft/tmp_code/newpeople.txt")
textFile.foreach(print)