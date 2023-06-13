from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile("Quiz_Sample2.txt")
print(rdd.collect())
print('-------------------------------------------------------------')

print("Mapped Value")
print(rdd.map(lambda x: (x,len(x.split(" ")))).collect())
print('---------------------------------------')

print("FlatMapped Value")
rdd2 = rdd.flatMap(lambda x: x.split(" "))

rdd3 = rdd2.map(lambda x: (x, len(x)))

print(rdd3.collect())
print('---------------------------------------')

print(rdd3.groupByKey().mapValues(list).collect())
print('---------------------------------------')

