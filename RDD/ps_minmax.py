from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("min and max")
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('movie_ratings.csv')
rdd1 = rdd.map(lambda x: (x.split(',')[0],int(x.split(',')[1])))
print(rdd1.collect())
print('-------------------------output-----------')
rdd2 = rdd1.reduceByKey(lambda x,y: x if x<y else y)

print('------ot2--')
print(rdd2.collect())