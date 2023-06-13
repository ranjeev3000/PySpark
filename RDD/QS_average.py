from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("average_quiz_sample.csv")
rdd1 = rdd.map(lambda x: (x.split(',')[0],(float(x.split(',')[2]),1)))
rdd2 = rdd1.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd3 = rdd2.map(lambda x: (x[0],x[1][0]/x[1][1]))
print(rdd3.collect())