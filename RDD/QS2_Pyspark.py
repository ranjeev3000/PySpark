from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Quiz2")

sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile("Quiz_Sample2.txt")

print(rdd.collect())
print('--------------------------------------------------------')

rdd1 = rdd.flatMap(lambda x: x.split(" ")).filter(lambda x: x[0]!='a' and x[0]!='c')
print('--------------------------------------------')
print(rdd1.collect())
print('--------------------------------')