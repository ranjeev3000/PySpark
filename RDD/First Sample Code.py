from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("Read File")

sc = SparkContext.getOrCreate(conf=conf)

text = sc.textFile("sample.txt")

print(text.collect()) 
