from pyspark import SparkContext

# Initital spark context
sc = SparkContext("local", "SparkApp")

# Create an RDDs of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])

# Use map and reduceByKey transformations with their 
# lambda expressions to aggregate and then compute average
agesRDD = dataRDD.map(lambda x, y: (x, (y, 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x, y, z: (x, y / z))

result = agesRDD.take(1)
print("result = " + result)