from pyspark import SparkConf, SparkContext


def stopSpark(SparkContext sc):
    sc.stop()

index = 0

def getindex():
    global index
    index+=1
    return index

def main():
    conf = SparkConf().setMaster("local").setAppName("FileSort")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("file://usr/local/my_soft/spark-2.1.0/mycode/rdd/filesort/file*.txt")
    index = 0
    result1 = lines.filter(lambda line:(len(line.strip())>0))
    result2 = result1.map(lambda x:(int(x.strip()),""))
    result3 = result2.repartition(1)
    result4 = result3.sortByKey(True)
    result5 = result4.map(lambda x:x[0])
    result6 = result5.map(lambda x:(getindex(),x))
    result6.foreach(print)
    result6.saveAsTextFile("file://usr/local/my_soft/spark-2.1.0/mycode/rdd/filesort/sortresult")

if __name__ == "__main__":
    main()
