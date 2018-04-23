2. A, B 지점 전체에서 1주일동안 각 고객이 어떤 품목을 몇회 구매했는지 계산하는 Spark 응용프로그램을 작성해 보시오.

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("UnionCount")
sc = SparkContext(conf=conf)
t = []

rdd1 = sc.textFile("martdata1.txt")
rdd2 = sc.textFile("martdata2.txt")

urdd = rdd1.union(rdd2)
rmrdd = urdd.map(lambda x: (x.split(',')[0],(x.split(',')[1:])))
t = rmrdd.map(lambda x: int(len(x[1])))


sunrdd = rmrdd.map(lambda x: (x[0],x[1][0]))

for i in range(1,max(t.collect())):
        rdd = rmrdd.filter(lambda x: len(x[1])>i)
        sunrdd = sunrdd + rdd.map(lambda x: (x[0],x[1][i]))


result = sunrdd.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+ y)


result2 = result.map(lambda x: (x[0][0],(x[0][1],x[1])))

p= result2.combineByKey(lambda value: (value,1),
                        lambda x, value: (x[0] + value,x[1]+1 ),
                        lambda x, y: (x[0] + y[0], x[1] + y[1]))

for line in p.collect():
        print(line)

