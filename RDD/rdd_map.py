from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read File")

sc = SparkContext.getOrCreate(conf=conf)
print('Test.........................................')
rdd = sc.textFile("sample.txt")
print('Test1.........................................')
# Mapping using lambda expression
# rdd1 = rdd.map(lambda x: x.split(' '))

# Mapping using function
def foo(x):
    return x.split(' ')

rdd1 = rdd.map(foo)
                        
print(rdd.collect())

print('----------------------------------------------------------------')
print(rdd1.collect())