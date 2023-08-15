from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('spark-take').
    getOrCreate()
)

data = [x for x in range(1, 11) if x % 2 == 0]

drdd = spark.sparkContext.parallelize(data)
drdd.collect()
print(drdd.take(3))