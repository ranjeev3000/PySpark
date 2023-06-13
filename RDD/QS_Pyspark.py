from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("QS")

sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile("Quiz_Sample.txt")
print(rdd.collect())
print('-------------------------------------------------------------------------')
# Mapping function
def len_ele(x):
    x = x.split(" ")
    l = []
    for ele in x:
        l.append(len(ele))
    return l

rdd1 = rdd.map(len_ele)

print(rdd1.collect())

    