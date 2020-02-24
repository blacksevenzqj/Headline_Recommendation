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

def stopSpark():
    sc.stop()

spark=SparkSession.builder.getOrCreate()
df = spark.read.json("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.json")

df.printSchema()

# 选择多列
df.select(df.name,df.age + 1).show()

# 条件过滤
df.filter(df.age > 20 ).show()

# 分组聚合
df.groupBy("age").count().show()

# 排序
df.sort(df.age.desc()).show()

# 多列排序
df.sort(df.age.desc(), df.name.asc()).show()

# 对列进行重命名
df.select(df.name.alias("username"),df.age).show()