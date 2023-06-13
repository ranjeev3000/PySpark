from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('QS_3')
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('Quiz_Sample3.txt')

print(rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,len(x))).collect())

