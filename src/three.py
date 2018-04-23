3. A, B 지점에서 공통적으로 판매한 물건이 총 몇회 판매되었는지 계산하는 Spark 응용프로그램을 작성해 보시오.

rom pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("martdata1.txt").flatMap(lambda x: x.split(",")[1:])
rdd2 = sc.textFile("martdata2.txt").flatMap(lambda x: x.split(",")[1:])

resultRDD1 = rdd1.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
resultRDD2 = rdd2.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)

joinRDD = resultRDD1.join(resultRDD2)

sumRDD = joinRDD.map(lambda x: (x[0],x[1][0]+x[1][1]))
print(sumRDD.collect())