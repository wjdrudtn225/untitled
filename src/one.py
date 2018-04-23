//1. A 지점에서 어떤 항목이 몇 번 판매되었는지 계산하는 Spark 응용프로그램을 작성해 보시오.

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("count")
sc = SparkContext(conf=conf)

rdd = sc.textFile("martdata1.txt")
words = rdd.flatMap(lambda x: x.split(',')[1:])
result = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

print(result.collect())