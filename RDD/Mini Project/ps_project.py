from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Mini Project")
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('StudentData.csv')


headers = rdd.first()
rdd = rdd.filter(lambda x: x!=headers)
rdd_n = rdd.map(lambda x: x.split(','))
# print(rdd.collect())
count = rdd.count()
print('-------------------------')
# print(f"Number of students in the file = {count}")

# Show the total marks achieved by male and female
rdd1 = rdd.map(lambda x: (x.split(',')[1], int(x.split(',')[5])))
rdd2 = rdd1.reduceByKey(lambda x,y: x+y)
# print(rdd2.collect())

# show the total number of students who have passed and total number of students who have failed 50+ marked are required to pass

print("show the total number of students who have passed and total number of students who have failed")
rdd3 = rdd.filter(lambda x:int(x.split(',')[5])>50)
# print(f'Number of passed students = {rdd3.count()}')
# print(f"Number of failed students = {rdd.count()-rdd3.count()}")

# Total enrollments per course
print("Total enrollments per course")
rdd4 = rdd.map(lambda x: (x.split(',')[3],1))
rdd4 = rdd4.reduceByKey(lambda x,y: x+y)
# print(f"Total enrollments per course {rdd4.collect()}")

# Total Marks per course
print("Total Marks per course")

rdd5 = rdd_n.map(lambda x: (x[3], int(x[5])))
rdd5 = rdd5.reduceByKey(lambda x,y: x+y)
print(f"Total Marks per course {rdd5.collect()}")