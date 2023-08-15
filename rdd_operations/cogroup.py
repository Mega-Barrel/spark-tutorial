from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('Spark - cogroup').
    getOrCreate()
)

data_1 = [
    ("A", (1, 1)),
    ("A", 2), 
    ("B", (3, 3)), 
    ("C",4) 
]

data_2 = [
    ("D", (5, 5)),
    ("A", 6), 
    ("B", 7), 
    ("A", 8)
]

rdd_1 = spark.sparkContext.parallelize(data_1)
rdd_2 = spark.sparkContext.parallelize(data_2)
rdd_3 = rdd_1.cogroup(rdd_2)

for data in rdd_3.collect():
    print(data)