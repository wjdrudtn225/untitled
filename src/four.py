//4.A, B 지점 모두에서 물건을 구매한 고객이 구매한 품목을 계산하는 Spark 응용프로그램을 작성해 보시오.


from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("AllUser")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("martdata1.txt")
rdd2 = sc.textFile("martdata2.txt")

resultrdd1 = rdd1.map(lambda x:(x.split(",")[0],x.split(",")[1:])).reduceByKey(lambda x,y: x+y)
resultrdd2 = rdd2.map(lambda x:(x.split(",")[0],x.split(",")[1:])).reduceByKey(lambda x,y: x+y)

result = resultrdd1.join(resultrdd2)

result = result.map(lambda x: (x[0], x[1][0]+x[1][1]))
print(result.collect())
