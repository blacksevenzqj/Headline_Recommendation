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

peopleRDD = sc.textFile("file:///usr/local/my_soft/spark-2.1.0/examples/src/main/resources/people.txt")

# 定义一个模式字符串
schemaString = "name age"

# 根据模式字符串生成模式
fields = list(map(lambda fieldName : StructField(fieldName, StringType(), nullable = True), schemaString.split(" ")))
schema = StructType(fields)
# 从上面信息可以看出，schema描述了模式信息，模式中包含name和age两个字段

rowRDD = peopleRDD.map(lambda line : line.split(',')).map(lambda attributes : Row(attributes[0], attributes[1]))

peopleDF = spark.createDataFrame(rowRDD, schema)

# 必须注册为临时表才能供下面查询使用
peopleDF.createOrReplaceTempView("people")

results = spark.sql("SELECT * FROM people")
results.rdd.map(lambda attributes : "name: " + attributes[0]+","+"age:"+attributes[1]).foreach(print)


