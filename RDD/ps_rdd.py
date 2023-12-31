
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('movie_ratings.csv')
rdd2 = rdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1]),1)))


rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
rdd3 = rdd3.map(lambda x: (x[0],x[1][0]/x[1][1]))
print(rdd3.collect())
