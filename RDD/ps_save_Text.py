from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("SaveAsText")
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('Quiz_Sample2.txt')
print('----------------------------')
print(rdd.getNumPartitions())
print('----------------------------')
rdd.saveAsTextFile('output/test_save2.txt')

print('---------------------')

print(rdd.collect())